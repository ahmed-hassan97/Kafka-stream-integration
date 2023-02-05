package com.github.mrpowers.spark.daria.hadoop

// import library 
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class DeviceData(device: String , purpose: String )
//case class DeviceData(device: String , purpose: String , value: Float  , longitude: Float , latitude: Float)
object KafkaIntegration {
   def main(args : Array[String]) : Unit = {
     val spark:SparkSession = SparkSession.builder()
                                           .master("local[1]")
                                           .config("spark.driver.maxResultSize" , "8G")
                                           .config("spark.sql.broadcastTimeout", "36000")
                                           .appName("SparkByExamples.com")
                                           .getOrCreate()
       
      import spark.implicits._
      val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers" , "localhost:9092")
            .option("subscribe" , "info")
            .load()
      
      val rawDF =  inputDF.selectExpr("CAST(value AS STRING)").as[String]
      val expendDF = rawDF.map(row => row.split(","))
                        .map(row => DeviceData(
                           row(1),
                           row(2),
                          // row(3).toFloat,
                           //row(4).toString,
                          // row(4).toFloat,
                          // row(5).toFloat

                        ))
      val url = "jdbc:postgres://localhost:5432/postgres"
      // expendDF.writeStream.foreachBatch { (askDF: DataFrame, batchId: Long) =>
      // askDF.persist()
      // askDF.write.parquet("StreamHandler/output/")
      // askDF.unpersist()
      //   }.start().awaitTermination()
     

      def myFunc(batchDF: DataFrame, batchID: Long) : Unit = {
                  batchDF.write
                        .format("jdbc")
                        .option("url", url)
                        .option("dbtable", "test")
                        .option("user", "postgres")
                        .option("password", "postgres")
                        .mode("append")
                        .save()                      
                 
                    }
                  }
      
      expendDF
            .writeStream
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .outputMode("append")
            .foreachBatch(myFunc _)
            .start()
            .awaitTermination()
 }
   
  


