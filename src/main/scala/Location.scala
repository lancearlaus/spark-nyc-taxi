


case class Location(lat: Double, lng: Double) {
  require(lat >= -90.0 && lat <= 90.0, s"invalid latitude: $lat")
  require(lng >= -180.0 && lng <= 180.0, s"invalid longitude: $lng")
}

object Location {

  implicit class LocationFunctions(location: Location) {

    // Returns distance (in meters) between two locations
    // Thanks: http://www.movable-type.co.uk/scripts/latlong.html
    def distanceTo(that: Location): Double = {
      val EarthRadius = 6371000.0  // meters
      val φ1 = location.lat.toRadians
      val φ2 = that.lat.toRadians
      val Δλ = Math.abs(that.lng - location.lng).toRadians

      Math.acos( Math.sin(φ1) * Math.sin(φ2) + Math.cos(φ1) * Math.cos(φ2) * Math.cos(Δλ) ) * EarthRadius
    }

  }
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

