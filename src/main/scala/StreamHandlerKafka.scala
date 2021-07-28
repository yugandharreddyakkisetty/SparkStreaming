import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import java.sql.Timestamp
case class DeviceData(device: String, temp: Double, humd: Double, pres: Double,timestamp:Timestamp)

object StreamHandlerKafka {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().master("local[*]").appName("Stream Handler Kafka").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val inputDF= spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","bcone.kafka.dev.com:9092")
      .option("subscribe","weather")
      .load()


    /*
          inputDF example
        +----+--------------------+-------+---------+------+--------------------+-------------+
        | key|               value|  topic|partition|offset|           timestamp|timestampType|
        +----+--------------------+-------+---------+------+--------------------+-------------+
        |null|[32 30 32 31 2D 3...|weather|        0|  6218|2021-07-28 15:34:...|            0|
        |null|[32 30 32 31 2D 3...|weather|        0|  6219|2021-07-28 15:34:...|            0|
        +----+--------------------+-------+---------+------+--------------------+-------------+
    */
    val rawDF = inputDF.selectExpr("CAST(value as STRING)").as[String]
    val expandedDF = rawDF.map(row => row.split(","))
      .map(row => DeviceData(
        row(1),
        row(2).toDouble,
        row(3).toDouble,
        row(4).toDouble,
        Timestamp.valueOf(row(0))
      )).withWatermark("timestamp","10 minutes")
  /*
    Complete Mode - The entire updated Result Table will be written to the external storage.
                    It is up to the storage connector to decide how to handle writing of the entire table.

    Append Mode - Only the new rows appended in the Result Table since the last trigger will be written to the external storage.
                  This is applicable only on the queries where existing rows in the Result Table are not expected to change.

    Update Mode - Only the rows that were updated in the Result Table since the last trigger will be written to the external
                  storage (available since Spark 2.1.1). Note that this is different from the Complete Mode in that this mode only
                  outputs the rows that have changed since the last trigger. If the query doesn’t contain aggregations,
                  it will be equivalent to Append mode.
  */
    val query = expandedDF
      .writeStream
      .format("console")
      .outputMode("update")
      .start()
    query.awaitTermination()
  }

}
