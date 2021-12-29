import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._
import java.sql.Timestamp
object WaterMark {

  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("WaterMarking").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","weather")
      .option("startingOffset","latest")
      .load()
    val sensorDF = rawStream.select(col("value")).as[String].map(i=>i.split(",")).map(row=>(Timestamp.valueOf(row(0)),row(1),row(2),row(3),row(4))).toDF("timestamp","device","temp","humd","pres")
    val groupedDf = sensorDF.withWatermark("timestamp","1 minutes")
      .groupBy($"device",window($"timestamp","1 minutes","1 minutes"))
      .count()

    val streamingProcess = groupedDf.writeStream.format("console").outputMode("append").start()
    streamingProcess.awaitTermination()
  }

}
