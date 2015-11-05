import java.time.{Duration, Instant}

import TripLinker._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.math.Ordering.Implicits._


case class TripLinker(tolerance: Tolerance = Tolerance.Default, storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER) {

  def link(trips: RDD[Trip]): RDD[Link] = {

    val persistedTrips = trips.persist(storageLevel)

    println("Sorting trips...")
    // Sort trips by dropoff and pickup time (both needed for linking)
    val sortedByDropoff = trips.map(trip => (trip.dropoff.time, trip)).sortByKey()
    val sortedByPickup = trips.map(trip => (trip.pickup.time, trip)).sortByKey().persist(storageLevel)

    // Calculate linking windows
    val tripWindows = sortedByDropoff
      .map { case (_, trip) => TripWindow(trip, Window(trip.dropoff, tolerance)) }
      .persist(storageLevel)

    // Unpersist trips to free storage
    persistedTrips.unpersist()

    // Calculate the time bounds (max interval) for each linking window partition
    val partitionIntervals = tripWindows.mapPartitionsWithIndex { (index, windows) =>
      windows.foldLeft(Option.empty[Interval]) { case (interval, linking) =>
        interval.map(interval => Interval(interval.begin, linking.window.interval.end))
          .orElse(Some(linking.window.interval))
      }
        .map(interval => Iterator.single((index, interval))).getOrElse(Iterator.empty)
    }.collect()

    val partitionCount = partitionIntervals.size

    println(s"partition intervals (size=$partitionCount): ${partitionIntervals.mkString(", ")}")

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
      //.persist(storageLevel)

    // Unpersist pickup sorted trips to free storage
    sortedByPickup.unpersist()

//    val candidateCount = candidatePickups.count()
//    println(s"candidate count: $candidateCount")

    // Link trips by selectively zipping dropoff trips with pickup trips via linking window
    val links = tripWindows.zipPartitions(candidatePickups, false) { (tripWindows, candidatePickups) =>
      new ZipIterator(tripWindows, candidatePickups)
    }

    // TODO: Unpersist trip windows (we're leaking storage here)

    links
  }

}

object TripLinker {

  case class Link(from: Trip, to: Trip)

  // Search tolerance for linking trips
  case class Tolerance(duration: Duration, distance: Double) {
    require(!duration.isNegative, s"invalid duration: $duration")
    require(distance > 0.0, s"invalid distance: $distance")

    val EarthRadius = 6371000.0  // meters
    val locationDelta = (distance / EarthRadius).toDegrees
  }
  object Tolerance {
    val Default = Tolerance(Duration.ofSeconds(60), 20)
  }

  private case class Window(interval: Interval, area: Area) {
    def matches(instant: Instant) = interval.contains(instant)
    def matches(location: Location) = area.contains(location)
  }
  private object Window {
    def apply(transfer: Transfer, tolerance: Tolerance): Window =
      Window(
        Interval(transfer.time, tolerance.duration),
        Area(transfer.location, tolerance.locationDelta))
  }

  private case class TripWindow(trip: Trip, window: Window)


  implicit class NextOption[A](val iterator: Iterator[A]) extends AnyVal {
    def nextOption(): Option[A] = iterator.hasNext match {
      case true => Some(iterator.next())
      case false => None
    }
  }


  private class ZipIterator(sortedLinkingWindows: Iterator[TripWindow], sortedCandidatePickups: Iterator[Trip]) extends Iterator[Link] {

    val active = mutable.Queue.empty[TripWindow]
    val matches = mutable.Queue.empty[Link]
    var nextWindow = sortedLinkingWindows.nextOption

    // Initialize matches
    nextMatches()

    override def hasNext: Boolean = !matches.isEmpty

    override def next(): Link = {
      val linked = matches.dequeue()
      if (matches.isEmpty) nextMatches()
      linked
    }

    // The heart of the trip matching algorithm
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
          Link(window.trip, pickupTrip)
        }
      }
    }
  }

}

