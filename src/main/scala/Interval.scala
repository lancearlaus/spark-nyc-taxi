import java.time.{Duration, Instant}

import scala.math.Ordering.Implicits._


// A half-open time interval
case class Interval(begin: Instant, end: Instant) {
  require(!end.isBefore(begin), s"$end is before $begin")
  def contains(instant: Instant) = begin <= instant && instant < end
}
object Interval {
  def apply(begin: Instant, duration: Duration): Interval = Interval(begin, begin.plus(duration))
}

