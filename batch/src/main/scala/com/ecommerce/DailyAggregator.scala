package com.ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DailyAggregator {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DailyAggregator")
      .getOrCreate()

    // Read raw events from BigQuery
    val events = spark.read.format("bigquery")
      .option("table", "epicfuely777.raw.events")
      .option("temporaryGcsBucket", "epicfuely777-dataflow-staging")
      .load()

    // Filter orders and aggregate by date
    val dailySales = events
      .filter(col("event_type") === "order")
      .groupBy(to_date(col("timestamp")).alias("date"))
      .agg(
        count("*").alias("total_orders"),
        sum("amount").alias("total_revenue")
      )

    // Write to analytics dataset
    dailySales.write
      .format("bigquery")
      .option("table", "epicfuely777.analytics.daily_sales")
      .option("temporaryGcsBucket", "epicfuely777-dataflow-staging")
      .mode("overwrite")
      .save()

    spark.stop()
  }
}
