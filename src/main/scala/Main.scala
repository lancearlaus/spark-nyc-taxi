import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Main {

  val schemaAdjustments = Seq(
    StructField("tpep_pickup_datetime", TimestampType),
    StructField("tpep_dropoff_datetime", TimestampType)
  ).map(f => (f.name -> f)).toMap

  def main (args: Array[String]){
    val dataFile = "data/yellow_tripdata_2015-01-06-first1MM.csv"
//    val dataFile = "data/yellow_tripdata_2015-01-06-first200K.csv"
    val conf = new SparkConf().setAppName("NYC Taxi App").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val temp = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(dataFile)
      .limit(1)
    val adjustedFields = temp.schema.map(f => schemaAdjustments.getOrElse(f.name, f))
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(StructType(adjustedFields))
      .load(dataFile)
      .cache()

    df.printSchema()

    val c = df.count
    println(s"count: $c")

    val pickupTimeCol = df("tpep_pickup_datetime")
    val totalAmmountCol = df("total_amount")
    val tripDistanceCol = df("trip_distance")
    val tipAmountCol = df("tip_amount")
    val tipPercentCol = (tipAmountCol / (totalAmmountCol - tipAmountCol)).as("tip_percent")
    df.groupBy(hour(pickupTimeCol))
      .agg(count(pickupTimeCol), avg(tripDistanceCol), sum(totalAmmountCol), avg(totalAmmountCol), avg(totalAmmountCol / tripDistanceCol), avg(tipAmountCol), avg(tipPercentCol))
      .show(100)


  }

}
