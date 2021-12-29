import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.StreamingQueryListener._

case class DeviceData(device: String, temp: Double, humd: Double, pres: Double,timestamp:Timestamp)
class JDBCSink(url:String,user:String,pwd:String) extends ForeachWriter[DeviceData]{
  val driver = "org.postgresql.Driver"
  var connection:Connection = _
  var statement:Statement = _
  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = DriverManager.getConnection(url,user,pwd)
    statement = connection.createStatement
    true
  }

  def process(record: DeviceData) = {
    val sqlQuery = "INSERT INTO trace.table_name(device,temp,humd,pres,timestamp) values('"+record.device+"','"+record.temp+"','"+record.humd+"','"+record.pres+"','"+record.timestamp+"')"
    statement.executeUpdate(sqlQuery)
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close()
  }
}




object Test {

  val prop = new Properties()
  prop.setProperty("url","jdbc:postgresql://validhost:5432/sysdb")
  prop.setProperty("user","username")
  prop.setProperty("password","password")
  prop.setProperty("hostname","validhost")
  prop.setProperty("port","5432")
  prop.setProperty("database","sysdb")
  prop.setProperty("driver","org.postgresql.Driver")

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Streaming Test").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
//    spark.streams.addListener(myListener) // use this to monitor the stream query
    val rawDf = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","weather")
      .option("startingOffsets","latest")
      .load()

    val sensorDF = rawDf.select(col("value")).as[String]
      .map(row=>row.split(",")).map{
        row=>{
          DeviceData(
            row(1),
            row(2).toDouble,
            row(3).toDouble,
            row(4).toDouble,
            Timestamp.valueOf(row(0))
          )
        }
      }
    val checkpointLocation = "./Tracker/"

    // write to console - start

    val streamingProcess = sensorDF
      .writeStream
      .format("console")
      .option("numRows","20")
      .option("checkpointLocation",checkpointLocation)
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("1 second"))
      .start()

    // write to console - end

    // Custom sink foreachBatch  - start
    val customSinkForeachBatch = sensorDF
      .writeStream
      .foreachBatch(forEachBatchSink)
      .outputMode("append")
      .start()
    customSinkForeachBatch.awaitTermination()


    // Custom sink foreachBatch - end

    // Custom sink foreach - start
    val customWriter = new JDBCSink(prop.getProperty("url"),prop.getProperty("user"),prop.getProperty("password"))
    val customSinkForeach = sensorDF
      .writeStream
      .foreach(customWriter)
      .outputMode("update")
      .start()
    customSinkForeach.awaitTermination()
    // Custom sink foreach - end


  }

  val myListener = new StreamingQueryListener() {
    override def onQueryStarted(event: QueryStartedEvent): Unit = {
      println("Query started: " + event.id)
    }
    override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      println("Query terminated: " + event.id)
    }
    override def onQueryProgress(event: QueryProgressEvent): Unit = {
      println("Query made progress: " + event.progress)
    }
  }


  def forEachBatchSink = (df:Dataset[DeviceData],batchId:Long)=> {
    df.persist()
    df.write.format("csv").option("header","true").mode("append").save("D:\\temp\\")
//    df.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"),"trace.table_name",prop)
    df.unpersist()
    println("")

  }


}
