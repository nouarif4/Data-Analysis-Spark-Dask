import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Load data into a DataFrame 
val filePath = "gs://datasetspark/DaskCleanedData.parquet"
val df = spark.read.parquet(filePath)

// Extract top-level category from the 'category_code'
val dfWithTopLevelCategory = df.withColumn("top_level_category", split(col("category_code"), "\\.").getItem(0))

// Create a price_range column
val priceRangeDf = dfWithTopLevelCategory.withColumn("price_range", when(col("price") <= 20, "0-20")
  .when(col("price") > 20 && col("price") <= 50, "21-50")
  .when(col("price") > 50 && col("price") <= 100, "51-100")
  .when(col("price") > 100 && col("price") <= 200, "101-200")
  .when(col("price") > 200 && col("price") <= 500, "201-500")
  .otherwise("500+"))

// Filter for 'view' and 'add_to_cart' events
val filteredDf = priceRangeDf.filter(col("event_type").isin("view", "cart"))

// Group by price range and top-level category, then calculate counts for 'view' and 'add_to_cart'
val groupedDf = filteredDf.groupBy("price_range", "top_level_category", "event_type")
  .agg(count("*").alias("event_count"))

// Pivot the event type column to get separate columns for 'view' and 'add_to_cart' counts
val pivotedDf = groupedDf.groupBy("price_range", "top_level_category")
  .pivot("event_type", Seq("view", "cart"))
  .agg(first("event_count"))

// Add conversion rate column: conversion rate = add_to_cart / view
val dfWithConversionRate = pivotedDf.withColumn("conversion_rate", 
  when(col("view") > 0, col("cart") / col("view")).otherwise(0))

// Select relevant columns for output
val outputDf = dfWithConversionRate.select("price_range", "top_level_category", "view", "conversion_rate")

// Save the result as CSV in Google Cloud Storage
outputDf.write.mode("overwrite").option("header", "true").csv("gs://datasetspark/results/conversion_rates.csv")

println("Data saved as 'conversion_rates.csv' in Google Cloud Storage.")

