import org.apache.spark.sql.functions._

// Read data from Parquet file
val df = spark.read.parquet("gs://datasetspark/DaskCleanedData.parquet")

// Clean and process the data to calculate conversion rates, sorted by descending conversion_rate
val conversionRates = df
  .groupBy("category_code")
  .agg(
    count(when(col("event_type") === "purchase", true)).alias("purchases"),
    count(when(col("event_type") === "view", true)).alias("views"),
    (count(when(col("event_type") === "purchase", true)).cast("double") / count(when(col("event_type") === "view", true)).cast("double")).alias("conversion_rate")
  )
  .orderBy(desc("conversion_rate"))

// Show conversion rates (Optional for debugging)
conversionRates.show()

