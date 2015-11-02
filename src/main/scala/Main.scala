import java.time.{Duration, Instant}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.math.Ordering.Implicits._

object Main {

  val ApplicationName = "NYC Taxi App"
  val EarthRadius = 6371000.0  // meters

  case class Location(lat: Double, lng: Double) {
    require(lat >= -90.0 && lat <= 90.0)
    require(lng >= -180.0 && lng <= 180.0)
  }

  sealed abstract class Transfer {
    val time: Instant
    val location: Location
  }
  case class Pickup(time: Instant, location: Location) extends Transfer
  case class Dropoff(time: Instant, location: Location) extends Transfer

  case class Trip(
       pickup: Pickup,
       dropoff: Dropoff,
       passengers: Int,
       distance: Double
   )

  // A half-open time interval
  case class Interval(begin: Instant, end: Instant) {
    require(end.isAfter(begin))
    def contains(instant: Instant) = begin <= instant && instant < end
  }
  object Interval {
    def apply(begin: Instant, duration: Duration): Interval = Interval(begin, begin.plus(duration))
  }

  case class Area(lowerCorner: Location, upperCorner: Location) {
    require(lowerCorner.lat <= upperCorner.lat && lowerCorner.lng <= upperCorner.lng)
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

  object IteratorOps {
    implicit class NextOption[A](val iterator: Iterator[A]) extends AnyVal {
      def nextOption(): Option[A] = iterator.hasNext match {
        case true => Some(iterator.next())
        case false => None
      }
    }
  }


  case class PickupSearchTolerance(duration: Duration, distance: Double) {
    require(!duration.isNegative)
    require(distance > 0.0)

    val deltaLatLng = (distance / EarthRadius).toDegrees
  }

  case class TripLink(from: Trip, to: Trip)

  class PickupSearchZipIterator(dropoffTrips: Iterator[Trip], pickupTrips: Iterator[Trip], tolerance: PickupSearchTolerance) extends Iterator[TripLink] {
    import IteratorOps._

    case class PickupMatcher(trip: Trip) {
      val interval = Interval(trip.dropoff.time, tolerance.duration)
      val area = Area(trip.dropoff.location, tolerance.deltaLatLng)

      def matches(instant: Instant) = interval.contains(instant)
      def matches(location: Location) = area.contains(location)
    }

    val matchers = mutable.Queue.empty[PickupMatcher]
    val matches = mutable.Queue.empty[TripLink]
    var nextMatcher = dropoffTrips.nextOption.map(trip => PickupMatcher(trip))

    // Initialize matches
    nextMatches()

    override def hasNext: Boolean = !matches.isEmpty

    override def next(): TripLink = {
      val tripLink = matches.dequeue()
      if (matches.isEmpty) nextMatches()
      tripLink
    }

    private def nextMatches() = {

      assert(matches.isEmpty)

      while (matches.isEmpty && pickupTrips.hasNext && nextMatcher.isDefined) {
        val pickupTrip = pickupTrips.next()

        // Purge expired matchers
        while (matchers.headOption.map(_.interval.end <= pickupTrip.pickup.time).getOrElse(false)) {
          matchers.dequeue()
        }

        // Skip invalid next matcher(s)
        while (nextMatcher.map(_.interval.end <= pickupTrip.pickup.time).getOrElse(false)) {
          nextMatcher = dropoffTrips.nextOption.map(trip => PickupMatcher(trip))
        }

        // Queue newly valid matcher(s)
        while (nextMatcher.map(_.matches(pickupTrip.pickup.time)).getOrElse(false)) {
          nextMatcher = nextMatcher.flatMap { m =>
            matchers.enqueue(m)
            dropoffTrips.nextOption.map(trip => PickupMatcher(trip))
          }
        }

        matches ++= matchers.filter(_.matches(pickupTrip.pickup.location)).map(m => TripLink(m.trip, pickupTrip))
      }
    }
  }

  def distanceBetween(l1: Location, l2: Location): Double = {
    val φ1 = l1.lat.toRadians
    val φ2 = l2.lat.toRadians
    val Δλ = Math.abs(l2.lng - l1.lng).toRadians
    val R = 6371000.0

    Math.acos( Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(Δλ) ) * EarthRadius
  }

  def loadData(file: String)(implicit sqlContext: SQLContext): DataFrame =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(file)

  def filterData(dataFrame: DataFrame) = dataFrame.filter(
    (dataFrame("trip_distance") > 0.0) and
      (dataFrame("pickup_latitude") !== 0.0) and (dataFrame("pickup_longitude") !== 0.0) and
      (dataFrame("dropoff_latitude") !== 0.0) and (dataFrame("dropoff_longitude") !== 0.0) and
      (dataFrame("tpep_pickup_datetime") !== dataFrame("tpep_dropoff_datetime")))

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
      val pickupLoc = Location(row.getDouble(schema.fieldIndex("pickup_latitude")), row.getDouble(schema.fieldIndex("pickup_longitude")))
      val dropoffTime = row.getTimestamp(schema.fieldIndex("tpep_dropoff_datetime")).toInstant
      val dropoffLoc = Location(row.getDouble(schema.fieldIndex("dropoff_latitude")), row.getDouble(schema.fieldIndex("dropoff_longitude")))
      val passengers = row.getInt(schema.fieldIndex("passenger_count"))
      val distance = row.getDouble(schema.fieldIndex("trip_distance"))

      Trip(Pickup(pickupTime, pickupLoc), Dropoff(dropoffTime, dropoffLoc), passengers, distance)
    }
  }

  def addColumns(dataFrame: DataFrame) = dataFrame
    .withColumn("tpep_pickup_date", to_date(dataFrame("tpep_pickup_datetime")))
    .withColumn("tpep_dropoff_date", to_date(dataFrame("tpep_dropoff_datetime")))
    .withColumn("tpep_pickup_unix", unix_timestamp(dataFrame("tpep_pickup_datetime")))
    .withColumn("tpep_dropoff_unix", unix_timestamp(dataFrame("tpep_dropoff_datetime")))



  def main (args: Array[String]){

    val dataFile = "data/yellow_tripdata_2015-01-06-first1MM.csv"
//    val dataFile = "data/yellow_tripdata_2015-01-06-first200K.csv"

    val conf = new SparkConf().setAppName(ApplicationName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    println("Reading data file...")
    val trips = mapData(filterData(adjustSchema(loadData(dataFile))))

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

    println("Sorting by dropoff time...")
    val dropoffSorted = trips.sortBy(_.dropoff.time).cache()

    dropoffSorted.take(100).foreach(println)

    println("Sorting by pickup time...")
    val pickupSorted = dropoffSorted.sortBy(_.pickup.time).cache()

    pickupSorted.take(100).foreach(println)

    val dropoffFile = dataFile.replace(".csv", "-dropoff")

    println(s"Dropoff sorted partition count: ${pickupSorted.partitions.length}")
    println(s"Pickup sorted partition count: ${dropoffSorted.partitions.length}")

    val searchTolerance = PickupSearchTolerance(Duration.ofMinutes(1), 50.0)

    val dropoffSortedSingle = dropoffSorted.coalesce(1, true)
    val pickupSortedSingle = pickupSorted.coalesce(1, true)

    println("Calculating zipped trips...")
    val zipped = dropoffSortedSingle.zipPartitions(pickupSortedSingle) { (dropoffTrips, pickupTrips) =>
      new PickupSearchZipIterator(dropoffTrips, pickupTrips, searchTolerance)
    }

    val zippedCount = zipped.count()
    println(s"Zipped count: $zippedCount")

    case class Diff(duration: Duration, distance: Double) {
      def this(dropoff: Trip, pickup: Trip) = this(
        Duration.between(dropoff.dropoff.time, pickup.pickup.time),
        distanceBetween(dropoff.dropoff.location, pickup.pickup.location))
    }

    zipped.take(100).foreach(tl => println(new Diff(tl.from, tl.to)))

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
