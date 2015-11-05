import java.time.Instant


sealed abstract class Transfer {
  val time: Instant
  val location: Location
}
case class Pickup(time: Instant, location: Location) extends Transfer
case class Dropoff(time: Instant, location: Location) extends Transfer

case class Trip(pickup: Pickup, dropoff: Dropoff, passengers: Int, distance: Double)
