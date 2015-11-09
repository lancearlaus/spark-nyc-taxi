import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

object TripCsv {

  val Schema = StructType(Seq(
    StructField("VendorID", IntegerType, true),
    StructField("tpep_pickup_datetime", TimestampType, true),
    StructField("tpep_dropoff_datetime", TimestampType, true),
    StructField("passenger_count", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("RateCodeID", IntegerType, true),
    StructField("store_and_fwd_flag", StringType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("payment_type", IntegerType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("extra", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true)))


  def loadTripCsv(file: String)(implicit sqlContext: SQLContext): DataFrame =
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(Schema)
      .load(file)



  implicit class ColumnFunctions(val column: Column) {
    def isValidLatitude = (column !== 0.0) && column >= -90.0 && column <= 90.0
    def isValidLongitude = (column !== 0.0) && column >= -180.0 && column <= 180.0
  }

  implicit class DataFrameFunctions(val dataFrame: DataFrame) {

    def filterInvalidRows = dataFrame.filter(
      (dataFrame("trip_distance") > 0.0) and
        (dataFrame("tpep_pickup_datetime") !== dataFrame("tpep_dropoff_datetime")) and
        dataFrame("pickup_latitude").isValidLatitude and
        dataFrame("dropoff_latitude").isValidLatitude and
        dataFrame("pickup_longitude").isValidLongitude and
        dataFrame("dropoff_longitude").isValidLongitude
    )

    def mapToTrips: RDD[Trip] = {
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
  }

}
