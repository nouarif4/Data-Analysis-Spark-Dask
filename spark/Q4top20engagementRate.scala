
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartUtils
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.renderer.category.BarRenderer
import org.jfree.data.category.DefaultCategoryDataset
import java.awt.{Color, Font}
import java.io.File
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.awt.Graphics2D
import scala.util.Random

// Load data into a DataFrame (replace with the correct path to your Parquet file)
val filePath = "/home/nouarif4/Downloads/SparkCleanedData.parquet"
val df = spark.read.parquet(filePath)

// Extract top-level category from the 'category_code'
val dfWithTopLevelCategory = df.withColumn("top_level_category", split(col("category_code"), "\\.").getItem(0))

// Check distinct event types to verify the presence of 'purchase'
dfWithTopLevelCategory.select("event_type").distinct().show()

// Filter for relevant event types ('view', 'cart', 'purchase')
val filteredDf = dfWithTopLevelCategory.filter(col("event_type").isin("view", "cart", "purchase"))

// Get distinct categories and event types
val eventTypes = Seq("view", "cart", "purchase")
val categoriesDf = dfWithTopLevelCategory.select("top_level_category").distinct()

// Use cross join to ensure all combinations of top_level_category and event_type are present
val eventTypesDf = eventTypes.toDF("event_type")
val completeDf = categoriesDf.crossJoin(eventTypesDf)

// Debug: Print the completeDf to verify combinations
println("Combinations after cross join:")
completeDf.show()

// Count after cross join
println(s"Count after cross join: ${completeDf.count()}")

// Group by category and event type to calculate counts
val groupedDf = filteredDf.groupBy("top_level_category", "event_type")
  .agg(count("*").alias("event_count"))

// Perform a left join to ensure no missing combinations, keeping all categories and event types
val completedGroupedDf = completeDf.join(groupedDf, Seq("top_level_category", "event_type"), "left_outer")
  .na.fill(0, Seq("event_count"))

// Debug: Print completedGroupedDf to verify after join
println("Data after left join:")
completedGroupedDf.show()

// Pivot the event type to get separate columns for 'view', 'cart', and 'purchase' counts
val pivotedDf = completedGroupedDf.groupBy("top_level_category")
  .pivot("event_type", eventTypes)
  .agg(first("event_count"))

// Count the rows after filling missing combinations
println(s"Count after ensuring all combinations: ${pivotedDf.count()}")

// Debug: Show the pivoted DataFrame
println("Pivoted DataFrame:")
pivotedDf.show()

// Drop rows where all event counts are zero
val filteredPivotedDf = pivotedDf.filter(!(col("view") === 0 && col("cart") === 0 && col("purchase") === 0))

// Debug: Show the filtered DataFrame
println("Filtered Pivoted DataFrame (after dropping rows with all zeros):")
filteredPivotedDf.show()

// Add a column for cart abandonment rate: cart abandonment = max((cart - purchase) / cart, 0)
val dfWithAbandonmentRate = filteredPivotedDf.withColumn("cart_abandonment_rate", 
  when(col("cart") > 0, ((col("cart") - col("purchase")) / col("cart")).cast("double"))
    .otherwise(0.0))

// Debug: Show DataFrame with abandonment rate
println("DataFrame with Cart Abandonment Rate:")
dfWithAbandonmentRate.show()

// Collect data for the chart
val result = dfWithAbandonmentRate.collect()

// Create the dataset for JFreeChart (bar chart)
val dataset = new DefaultCategoryDataset
result.foreach(row => {
  val category = row.getString(0)
  val abandonmentRate = row.getAs[Double]("cart_abandonment_rate") * 100 // 'cart_abandonment_rate' as a percentage

  // Add data to the dataset for cart abandonment rate
  dataset.addValue(math.max(0, abandonmentRate), "Cart Abandonment Rate", category) // Ensure non-negative values
})

// Create a bar chart for cart abandonment rate by category
val chart = ChartFactory.createBarChart(
  "Cart Abandonment Rate by Category",
  "Product Category",
  "Cart Abandonment Rate (%)",
  dataset,
  PlotOrientation.VERTICAL,
  true, // Include legend
  true,
  false
)

// Adjust the width of the bars
val plot: CategoryPlot = chart.getCategoryPlot()
val renderer = new BarRenderer()
renderer.setMaximumBarWidth(0.3) // Increase the bar width to make the bars clearer
plot.setRenderer(renderer)

// Set unique colors for each category using random colors
result.zipWithIndex.foreach { case (row, index) =>
  renderer.setSeriesPaint(index, new Color(Random.nextInt(256), Random.nextInt(256), Random.nextInt(256)))
}

// Save the chart as a PNG file
val chartFile = new File("cart_abandonment_rate_by_category.png")
ChartUtils.saveChartAsPNG(chartFile, chart, 1000, 800)

println("Chart saved as 'cart_abandonment_rate_by_category.png'")

