import java.time.{Duration, Instant}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.collection.{GenTraversableOnce, mutable}
import scala.math.Ordering.Implicits._

object Main {

  val ApplicationName = "NYC Taxi App"
  val EarthRadius = 6371000.0  // meters

  // A half-open time interval
  case class Interval(begin: Instant, end: Instant) {
    require(!end.isBefore(begin), s"$end is before $begin")
    def contains(instant: Instant) = begin <= instant && instant < end
  }
  object Interval {
    def apply(begin: Instant, duration: Duration): Interval = Interval(begin, begin.plus(duration))
  }

  case class Location(lat: Double, lng: Double) {
    require(lat >= -90.0 && lat <= 90.0, s"invalid latitude: $lat")
    require(lng >= -180.0 && lng <= 180.0, s"invalid longitude: $lng")
  }

  case class Area(lowerCorner: Location, upperCorner: Location) {
    require(lowerCorner.lat <= upperCorner.lat && lowerCorner.lng <= upperCorner.lng, s"invalid corners: lower: $lowerCorner, upper: $upperCorner")
    def contains(location: Location) =
      (location.lat >= lowerCorner.lat && location.lng >= lowerCorner.lng) &&
      (location.lat <= upperCorner.lat && location.lng <= upperCorner.lng)
  }
  object Area {
    def apply(center: Location, delta: Double): Area = Area(
      Location(center.lat - delta, center.lng - delta),
      Location(center.lat + delta, center.lng + delta)
    )
  }

  // Thanks: http://www.movable-type.co.uk/scripts/latlong.html
  def distanceBetween(l1: Location, l2: Location): Double = {
    val φ1 = l1.lat.toRadians
    val φ2 = l2.lat.toRadians
    val Δλ = Math.abs(l2.lng - l1.lng).toRadians
    val R = 6371000.0

    Math.acos( Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(Δλ) ) * EarthRadius
  }

  object IteratorOps {
    implicit class NextOption[A](val iterator: Iterator[A]) extends AnyVal {
      def nextOption(): Option[A] = iterator.hasNext match {
        case true => Some(iterator.next())
        case false => None
      }
    }
  }


  sealed abstract class Transfer {
    val time: Instant
    val location: Location
  }
  case class Pickup(time: Instant, location: Location) extends Transfer
  case class Dropoff(time: Instant, location: Location) extends Transfer

  case class Trip(pickup: Pickup, dropoff: Dropoff, passengers: Int, distance: Double)

  case class TransferTolerance(duration: Duration, distance: Double) {
    require(!duration.isNegative, s"invalid duration: $duration")
    require(distance > 0.0, s"invalid distance: $distance")

    val locationDelta = (distance / EarthRadius).toDegrees
  }

  case class LinkingWindow(interval: Interval, area: Area) {
    def matches(instant: Instant) = interval.contains(instant)
    def matches(location: Location) = area.contains(location)
  }
  object LinkingWindow {
    def apply(transfer: Transfer, tolerance: TransferTolerance): LinkingWindow =
      LinkingWindow(
        Interval(transfer.time, tolerance.duration),
        Area(transfer.location, tolerance.locationDelta))
  }

  case class TripLinkingWindow(trip: Trip, window: LinkingWindow)

  case class LinkedTrips(from: Trip, to: Trip)


  def linkTrips(trips: RDD[Trip], tolerance: TransferTolerance): RDD[LinkedTrips] = {

    // Sort trips by dropoff time
    val sortedByDropoff = trips.sortBy(_.dropoff.time)

    // Sort trips by pickup time
    val sortedByPickup = trips.map(trip => (trip.pickup.time, trip)).sortByKey().persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Calculate linking windows
    val linkingWindows = sortedByDropoff.map(trip => TripLinkingWindow(trip, LinkingWindow(trip.dropoff, tolerance))).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Release trips from cache to free memory
    // TOOD: Move this elsewhere
    println("Unpersisting trips...")
    trips.unpersist()

    // Calculate the time bounds (max interval) for each linking window partition
    val partitionIntervals = linkingWindows.mapPartitionsWithIndex { (index, windows) =>
        windows.foldLeft(Option.empty[Interval]) { case (interval, linking) =>
          interval.map(interval => Interval(interval.begin, linking.window.interval.end))
            .orElse(Some(linking.window.interval))
        }
        .map(interval => Iterator.single((index, interval))).getOrElse(Iterator.empty)
    }.collect()

    println(s"partition intervals: ${partitionIntervals.mkString(", ")}")

    val partitionCount = partitionIntervals.size

    // Select the range of candidate pickups for each linking window partition
    val candidatePickups = partitionIntervals
      .map { case (partition, interval) =>
        sortedByPickup.filterByRange(interval.begin, interval.end).map { case (time, trip) =>
          ((partition, time), trip)
        }
      }
      .reduceLeft(_ ++ _)
      .repartitionAndSortWithinPartitions(new Partitioner {
        override def numPartitions: Int = partitionCount
        override def getPartition(key: Any): Int = key match {
          case (partition: Int, _) => partition
        }
      })
      .map { case ((partition, time), trip) => trip }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("Unpersisting trips sorted by pickup time...")
    sortedByPickup.unpersist()

    val candidateCount = candidatePickups.count()
    println(s"candidate count: $candidateCount")

    // Link trips by selectively zipping dropoff trips with pickup trips via linking window
    val linkedTrips = linkingWindows.zipPartitions(candidatePickups, false) { (linkingWindows, candidatePickups) =>
      new TripLinkingZipIterator(linkingWindows, candidatePickups)
    }

    linkedTrips
  }


  class TripLinkingZipIterator(sortedLinkingWindows: Iterator[TripLinkingWindow], sortedCandidatePickups: Iterator[Trip]) extends Iterator[LinkedTrips] {
    import IteratorOps._

    val active = mutable.Queue.empty[TripLinkingWindow]
    val matches = mutable.Queue.empty[LinkedTrips]
    var nextWindow = sortedLinkingWindows.nextOption

    // Initialize matches
    nextMatches()

    override def hasNext: Boolean = !matches.isEmpty

    override def next(): LinkedTrips = {
      val linked = matches.dequeue()
      if (matches.isEmpty) nextMatches()
      linked
    }

    private def nextMatches() = {

      assert(matches.isEmpty)

      while (matches.isEmpty && sortedCandidatePickups.hasNext && nextWindow.isDefined) {
        val pickupTrip = sortedCandidatePickups.next()
        val pickupTime = pickupTrip.pickup.time

        // Purge expired windows
        while (active.headOption.map(_.window.interval.end <= pickupTime).getOrElse(false)) {
          active.dequeue()
        }

        // Skip non matching next window(s)
        while (nextWindow.map(_.window.interval.end <= pickupTime).getOrElse(false)) {
          nextWindow = sortedLinkingWindows.nextOption
        }

        // Queue newly valid window(s)
        while (nextWindow.map(_.window.matches(pickupTime)).getOrElse(false)) {
          nextWindow = nextWindow.flatMap { window =>
            active.enqueue(window)
            sortedLinkingWindows.nextOption
          }
        }

        matches ++= active.filter(_.window.matches(pickupTrip.pickup.location)).map { window =>
          LinkedTrips(window.trip, pickupTrip)
        }
      }
    }
  }


  def loadData(file: String)(implicit sqlContext: SQLContext): DataFrame =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)
  
  def isValidLatitude(column: Column): Column = column >= -90.0 && column <= 90.0
  def isValidLongitude(column: Column): Column = column >= -180.0 && column <= 180.0

  def filterData(dataFrame: DataFrame) = dataFrame.filter(
    (dataFrame("trip_distance") > 0.0) and
      (dataFrame("tpep_pickup_datetime") !== dataFrame("tpep_dropoff_datetime")) and
      isValidLatitude(dataFrame("pickup_latitude")) and
      isValidLatitude(dataFrame("dropoff_latitude")) and
      isValidLongitude(dataFrame("pickup_longitude")) and
      isValidLongitude(dataFrame("dropoff_longitude"))
  )

  // Adjust infered schema (cast timestamp columns)
  def adjustSchema(dataFrame: DataFrame) = dataFrame.columns.foldLeft(dataFrame) { case (dataFrame, name) =>
    name match {
      case "tpep_pickup_datetime" => dataFrame.withColumn(name, dataFrame(name).cast(TimestampType))
      case "tpep_dropoff_datetime" => dataFrame.withColumn(name, dataFrame(name).cast(TimestampType))
      case _ => dataFrame
    }
  }

  def mapData(dataFrame: DataFrame): RDD[Trip] = {
    val schema = dataFrame.schema
    dataFrame.map { row =>
      val pickupTime = row.getTimestamp(schema.fieldIndex("tpep_pickup_datetime")).toInstant
      val pickupLocation = Location(row.getDouble(schema.fieldIndex("pickup_latitude")), row.getDouble(schema.fieldIndex("pickup_longitude")))
      val dropoffTime = row.getTimestamp(schema.fieldIndex("tpep_dropoff_datetime")).toInstant
      val dropoffLocation = Location(row.getDouble(schema.fieldIndex("dropoff_latitude")), row.getDouble(schema.fieldIndex("dropoff_longitude")))
      val passengers = row.getInt(schema.fieldIndex("passenger_count"))
      val distance = row.getDouble(schema.fieldIndex("trip_distance"))

      Trip(Pickup(pickupTime, pickupLocation), Dropoff(dropoffTime, dropoffLocation), passengers, distance)
    }
  }


  def main (args: Array[String]){

    val FullDataFile = "data/yellow_tripdata_2015-01-06.csv"
    val First200K = "data/yellow_tripdata_2015-01-06-first200K.csv"
    val First1MM = "data/yellow_tripdata_2015-01-06-first1MM.csv"

    val dataFile = First1MM

    val conf = new SparkConf().setAppName(ApplicationName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    println("Reading and mapping data file...")
    val trips = mapData(filterData(adjustSchema(loadData(dataFile)))).persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("Calculating trip count...")
    val tripCount = trips.count
    println(s"filtered trip count: $tripCount")

//    val tipPercentCol = (tipAmountCol / (totalAmmountCol - tipAmountCol)).as("tip_percent")
//    val gd = filtered.groupBy(hour(pickupTimeCol))
//      .agg(count(pickupTimeCol), avg(tripDistanceCol), sum(totalAmmountCol), avg(totalAmmountCol), avg(totalAmmountCol / tripDistanceCol), avg(tipAmountCol), avg(tipPercentCol))
//
//    gd.show(100)
//
//    println("Sorting by pickup time...")
//    val pickupSorted = df.sort(pickupTimeCol).cache()
//    pickupSorted.show(10, false)
//

//    println("Sorting by dropoff time...")
//    val dropoffSorted = trips.sortBy(_.dropoff.time).cache()
//
//    dropoffSorted.take(100).foreach(println)
//
//    println("Sorting by pickup time...")
//    val pickupSorted = dropoffSorted.sortBy(_.pickup.time).cache()
//
//    pickupSorted.take(100).foreach(println)
//
//    val dropoffFile = dataFile.replace(".csv", "-dropoff")
//
//    println(s"Dropoff sorted partition count: ${pickupSorted.partitions.length}")
//    println(s"Pickup sorted partition count: ${dropoffSorted.partitions.length}")
//
//    val searchTolerance = PickupSearchTolerance(Duration.ofMinutes(1), 50.0)
//
//    val dropoffSortedSingle = dropoffSorted.coalesce(1, true)
//    val pickupSortedSingle = pickupSorted.coalesce(1, true)
//
//    println("Calculating zipped trips...")
//    val zipped = dropoffSortedSingle.zipPartitions(pickupSortedSingle) { (dropoffTrips, pickupTrips) =>
//      new PickupSearchZipIterator(dropoffTrips, pickupTrips, searchTolerance)
//    }
//
//    val zippedCount = zipped.count()
//    println(s"Zipped count: $zippedCount")


    println("Linking trips...")
    val tolerance = TransferTolerance(Duration.ofMinutes(1), 50.0)
    val links = linkTrips(trips, tolerance)

    case class Diff(duration: Duration, distance: Double) {
      def this(dropoff: Trip, pickup: Trip) = this(
        Duration.between(dropoff.dropoff.time, pickup.pickup.time),
        distanceBetween(dropoff.dropoff.location, pickup.pickup.location))
    }

    links.take(100).foreach(link => println(new Diff(link.from, link.to)))

    val linkCount = links.count()
    println(s"Link count: $linkCount")

    println("Processing complete, hit any key to exit...")
    System.in.read()

//    println("Writing dropoff sorted file...")
//    dropoffSorted.coalesce(1).write
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .mode(SaveMode.Overwrite)
//      .save(dropoffFile)
//
//    val timeTolerence = 30 // seconds
//    val idField = StructField("id", LongType, false)
//    val idSchema = StructType(idField +: df.schema.fields)
//
//    val rowRdd = pickupSorted.rdd.zipWithIndex()
//      .map { case (row, id) => id +: row.toSeq }
//      .map { values => Row.fromSeq(values) }
//    val idDF = sqlContext.createDataFrame(rowRdd, idSchema)

    //idDF.show(200, false)

    val joinQuery =
      """
        | SELECT
        |   dropoff.id, pickup.id,
        |   dropoff.tpep_dropoff_datetime, pickup.tpep_pickup_datetime,
        |   dropoff.dropoff_latitude, dropoff.dropoff_longitude,
        |   pickup.pickup_latitude, pickup.pickup_longitude
        | FROM
        |   trips dropoff
        | LEFT OUTER JOIN trips pickup ON
        |   pickup.tpep_pickup_unix > dropoff.tpep_dropoff_unix AND
        |   pickup.tpep_pickup_unix <= (dropoff.tpep_dropoff_unix + 30)
      """.stripMargin
//    AND

//    idDF.registerTempTable("trips")
//
//    println("Joining tables...")
//    val joined = idDF.sqlContext.sql(joinQuery)
//    val joinedCount = joined.count
//
//    println(s"Joined count: $joinedCount")
//
//    joined.show(1000, false)

  }

}
