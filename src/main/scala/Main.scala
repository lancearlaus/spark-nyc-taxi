import java.time.Duration
import java.util.NoSuchElementException

import Location._
import TripCsv._
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  val ApplicationName = "NYC Taxi App"

  object CsvFile {
    val All = "data/yellow_tripdata_2015-01-06.csv"
    val First200K = "data/yellow_tripdata_2015-01-06-first200K.csv"
    val First1MM = "data/yellow_tripdata_2015-01-06-first1MM.csv"
    val First10MM = "data/yellow_tripdata_2015-01-06-first10MM.csv"
  }

  def main (args: Array[String]){

    val csvFile = CsvFile.First1MM

    val conf = new SparkConf().setAppName(ApplicationName).setMaster("local[5]")
    val sc = new SparkContext(conf)
    implicit val sqlContext = new SQLContext(sc)

    println("Reading and mapping data file...")
    val trips = loadTripCsv(csvFile).filterInvalidRows.mapToTrips

    println("Calculating trip count...")
    val tripCount = trips.count
    println(s"Trip count: $tripCount")

    println("Linking trips...")
    val links = TripLinker().link(trips).persist(StorageLevel.MEMORY_AND_DISK_SER)

    println("Counting same location links...")
    val sameLocationLinks = links.filter(link => link.from.dropoff.location == link.to.pickup.location)

    sameLocationLinks.take(200).foreach(println)

    val sameLocationDurations = sameLocationLinks.map(link => Duration.between(link.from.dropoff.time, link.to.pickup.time).getSeconds)
    val sameLocationDurationStats = sameLocationDurations.stats()
    val sameLocationCount = sameLocationLinks.count()

    case class Diff(duration: Duration, distance: Double) {
      def this(dropoff: Trip, pickup: Trip) = this(
        Duration.between(dropoff.dropoff.time, pickup.pickup.time),
        dropoff.dropoff.location.distanceTo(pickup.pickup.location))
    }

    links.take(100).foreach(link => println(new Diff(link.from, link.to)))

    val linkCount = links.count()
    println(s"SUMMARY: Trips: $tripCount, Links: $linkCount, " +
      s"Links with same dropoff/pickup: $sameLocationCount (duration stats: $sameLocationDurationStats)")

    println("Processing complete, hit any key to exit...")
    System.in.read()

  }

}
