import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartUtils
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.renderer.category.LineAndShapeRenderer
import org.jfree.data.category.DefaultCategoryDataset
import java.awt.Color
import java.io.File

// Load data into a DataFrame (replace with the correct path to your Parquet file)
val filePath = "/home/nouarif4/Downloads/SparkCleanedData.parquet"
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
  .na.fill(0, Seq("view", "cart")) // Fill any null values with 0

// Add conversion rate column: conversion rate = add_to_cart / view
val dfWithConversionRate = pivotedDf.withColumn("conversion_rate", 
  when(col("view") > 0, col("cart") / col("view")).otherwise(0))

// Add margin of error column (normal approximation confidence interval for binomial proportion)
val dfWithMarginOfError = dfWithConversionRate.withColumn("margin_of_error", 
  when(col("view") > 0, sqrt((col("conversion_rate") * (lit(1) - col("conversion_rate"))) / col("view"))).otherwise(0))

// Collect data for the chart
val result = dfWithMarginOfError.collect()

// Create the dataset for JFreeChart (bar chart)
val dataset = new DefaultCategoryDataset
result.foreach(row => {
  val priceRange = row.getString(0)
  val category = row.getString(1) // Top-level category
  val viewCount = row.getLong(2) // 'view_count'

  // Add data to the dataset for views (colored by top-level category)
  dataset.addValue(viewCount, category, priceRange) // Use category as series
})

// Create the dataset for the conversion rate (line chart)
val conversionRateDataset = new DefaultCategoryDataset
result.foreach(row => {
  val priceRange = row.getString(0)
  val conversionRate = row.getDouble(4) * 100 // Convert to percentage

  // Add conversion rate to the dataset
  conversionRateDataset.addValue(conversionRate, "Conversion Rate", priceRange)
})

// Create the dataset for the margin of error (line chart for upper and lower bounds)
val marginOfErrorDataset = new DefaultCategoryDataset
result.foreach(row => {
  val priceRange = row.getString(0)
  val conversionRate = row.getDouble(4) * 100 // Convert to percentage
  val marginOfError = row.getDouble(5) * 100 // Convert margin of error to percentage

  // Add upper bound
  marginOfErrorDataset.addValue(conversionRate + marginOfError, "Upper Bound", priceRange)
  // Add lower bound
  marginOfErrorDataset.addValue(conversionRate - marginOfError, "Lower Bound", priceRange)
})

// Sort price ranges
val sortedDataset = new DefaultCategoryDataset
val priceRanges = Seq("0-20", "21-50", "51-100", "101-200", "201-500", "500+")
priceRanges.foreach(priceRange => {
  result.filter(row => row.getString(0) == priceRange).foreach(row => {
    val category = row.getString(1)
    val viewCount = row.getLong(2)
    sortedDataset.addValue(viewCount, category, priceRange)
  })
})

// Create a bar chart for views and conversion rates
val chart = ChartFactory.createBarChart(
  "Views and Conversion Rate by Price Range and Category",
  "Price Range",
  "View Count",
  sortedDataset,
  PlotOrientation.VERTICAL,
  true, // Include legend
  true,
  false
)

// Get the plot object to overlay conversion rate
val plot: CategoryPlot = chart.getCategoryPlot()

// Create a line renderer for conversion rates
val lineRenderer = new LineAndShapeRenderer()
lineRenderer.setSeriesPaint(0, Color.RED) // Set conversion rate line color to red

// Create a line renderer for margin of error (upper and lower bounds)
val marginRenderer = new LineAndShapeRenderer()
marginRenderer.setSeriesPaint(0, Color.GRAY) // Set upper bound color
marginRenderer.setSeriesPaint(1, Color.GRAY) // Set lower bound color

// Add the conversion rate dataset to the plot
plot.setDataset(1, conversionRateDataset)
plot.setRenderer(1, lineRenderer)

// Add the margin of error dataset to the plot
plot.setDataset(2, marginOfErrorDataset)
plot.setRenderer(2, marginRenderer)

// Adjust axis for the conversion rate to display percentages
val axis2 = new org.jfree.chart.axis.NumberAxis("Conversion Rate (%)")
plot.setRangeAxis(1, axis2)
plot.mapDatasetToRangeAxis(1, 1) // Map the conversion rate dataset to the second axis
plot.mapDatasetToRangeAxis(2, 1) // Map the margin of error dataset to the second axis

// Save the chart as a PNG file
val chartFile = new File("views_conversion_rate_with_margin_by_price_range_category.png")
ChartUtils.saveChartAsPNG(chartFile, chart, 800, 600)

println("Chart saved as 'views_conversion_rate_with_margin_by_price_range_category.png'")

