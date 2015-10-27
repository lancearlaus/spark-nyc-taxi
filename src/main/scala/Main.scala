import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, Column, SQLContext}
import org.apache.spark.sql.functions._

object Main {

  // Adjustments to the infered schema
  val schemaAdjustments = Seq(
    StructField("tpep_pickup_datetime", TimestampType),
    StructField("tpep_dropoff_datetime", TimestampType)
  ).map(f => (f.name -> f)).toMap

  val adjustments = Map[String, Column => Column](
    "tpep_pickup_datetime" -> { col => col.cast(TimestampType) }
  )

  def main (args: Array[String]){
    val dataFile = "data/yellow_tripdata_2015-01-06-first1MM.csv"
//    val dataFile = "data/yellow_tripdata_2015-01-06-first200K.csv"
    val conf = new SparkConf().setAppName("NYC Taxi App").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    println("Reading data file...")
    val raw = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(dataFile)

    println("Adjusting schema...")
    // Adjust infered schema (cast timestamp columns)
    val adjusted = raw.columns.foldLeft(raw) { case (df, name) =>
      name match {
        case "tpep_pickup_datetime" => df.withColumn(name, df(name).cast(TimestampType))
        case "tpep_dropoff_datetime" => df.withColumn(name, df(name).cast(TimestampType))
        case _ => df
      }
    }

    // Add extra columns
    val additional = adjusted
      .withColumn("tpep_pickup_date", to_date(adjusted("tpep_pickup_datetime")))
      .withColumn("tpep_dropoff_date", to_date(adjusted("tpep_dropoff_datetime")))

    println("Filtering data...")
    val filtered = additional.filter(
      (additional("trip_distance") > 0.0) and
        (additional("pickup_latitude") !== 0.0) and (additional("pickup_longitude") !== 0.0) and
        (additional("dropoff_latitude") !== 0.0) and (additional("dropoff_longitude") !== 0.0))

    println("Caching data...")
    val df = filtered.cache()
    df.printSchema()


    val pickupTimeCol = df("tpep_pickup_datetime")
    val dropoffTimeCol = df("tpep_dropoff_datetime")
    val pickupLatCol = df("pickup_latitude")
    val pickupLongCol = df("pickup_longitude")
    val dropoffLatCol = df("dropoff_latitude")
    val dropoffLongCol = df("dropoff_longitude")
    val totalAmmountCol = df("total_amount")
    val tripDistanceCol = df("trip_distance")
    val tipAmountCol = df("tip_amount")


    println(s"raw count: ${raw.count}")
    println(s"filtered count: ${df.count}")

//    val tipPercentCol = (tipAmountCol / (totalAmmountCol - tipAmountCol)).as("tip_percent")
//    val gd = filtered.groupBy(hour(pickupTimeCol))
//      .agg(count(pickupTimeCol), avg(tripDistanceCol), sum(totalAmmountCol), avg(totalAmmountCol), avg(totalAmmountCol / tripDistanceCol), avg(tipAmountCol), avg(tipPercentCol))
//
//    gd.show(100)
//
    println("Sorting by pickup time...")
    val pickupSorted = df.sort(pickupTimeCol).cache()
    pickupSorted.show(10, false)
//
//    println("Sorting by dropoff time...")
//    val dropoffSorted = filtered.sort(dropoffTimeCol)
//    dropoffSorted.show(10, false)

    val idField = StructField("id", LongType, false)
    val idSchema = StructType(idField +: df.schema.fields)

    val rowRdd = pickupSorted.rdd.zipWithIndex()
      .map { case (row, id) => id +: row.toSeq }
      .map { values => Row.fromSeq(values) }
    val idDF = sqlContext.createDataFrame(rowRdd, idSchema)

    idDF.show(200, false)

  }

}
