package com.example.deltalake

import org.apache.spark.sql.delta.DeltaLog
import io.delta.tables._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession, DataFrame}

object ExampleOne extends App {

  val spark = SparkSession.builder().appName("ExampleOne")
    .config("spark.network.timeout", "1500s")
    .config("spark.broadcast.compress", "true")
    .config("spark.sql.broadcastTimeout", "36000")
    .getOrCreate()

  import spark.implicits._
  val data = spark.range(0, 5)
  data.write.format("delta").save("/tmp/delta-table")

  val df = spark.read.format("delta").load("/tmp/delta-table")
  df.show()

  // overwrite
  val dataOne = spark.range(5, 10)
  dataOne.write.format("delta").mode("overwrite").save("/tmp/delta-table")
  df.show()

  //TODO: Conditional update without Overwrite

  val deltaTable = DeltaTable.forPath("/tmp/delta-table")

  // Update every even value by adding 100 to it
  deltaTable.update(
    condition = expr("id % 2 == 0"),
    set = Map("id" -> expr("id + 100")))

  // Delete every even value
  deltaTable.delete(condition = expr("id % 2 == 0"))

  // Upsert (merge) new data
  val newData = spark.range(0, 20).toDF

  deltaTable.as("oldData")
    .merge(
      newData.as("newData"),
      "oldData.id = newData.id")
    .whenMatched
    .update(Map("id" -> col("newData.id")))
    .whenNotMatched
    .insert(Map("id" -> col("newData.id")))
    .execute()

  deltaTable.toDF.show()

  // Read older version of data using Time Travel

  val dfOne = spark.read.format("delta").option("versionAsOf", 0).load("/tmp/delta-table")
  dfOne.show()

  // The Delta Lake transaction log guarantees exactly-once processing
  // By default, streams run in append mode, which adds new records to the table
  val streamingDf = spark.readStream.format("rate").load()
  val stream = streamingDf.select($"value" as "id").writeStream.format("delta")
    .option("checkpointLocation", "/tmp/checkpoint").start("/tmp/delta-table")


  val stream2 = spark.readStream.format("delta").load("/tmp/delta-table")
    .writeStream.format("console").start()

  df.write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", "date >= '2017-01-01' AND date <= '2017-01-31'")
    .save("/delta/events")

  // pyspark --packages io.delta:delta-core_2.12:0.7.0

  deltaTable.vacuum()
  deltaTable.vacuum(100)

  val fullHistoryDF = deltaTable.history()
  val lastOperationDF = deltaTable.history(1)

  // full-load 1000
  // insert 10, delete 2, update 3
  // transaction_log_24_june_2020/ 15, 2(id), 3 update (merge)
  // schema structure
}
