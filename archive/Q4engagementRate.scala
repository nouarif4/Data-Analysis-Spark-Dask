import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.jfree.chart.ChartFactory
import org.jfree.chart.ChartUtils
import org.jfree.chart.plot.{CategoryPlot, PlotOrientation}
import org.jfree.chart.renderer.category.BarRenderer
import org.jfree.chart.renderer.category.LineAndShapeRenderer
import org.jfree.data.category.DefaultCategoryDataset
import java.awt.Color
import java.io.File
import scala.collection.JavaConverters._
import org.jfree.chart.title.LegendTitle
import java.awt.Font
import java.awt.image.BufferedImage
import javax.imageio.ImageIO
import java.awt.GradientPaint
import scala.util.Random

// Load data into a DataFrame (replace with the correct path to your Parquet file)
val filePath = "/home/nouarif4/Downloads/SparkCleanedData.parquet"
val df = spark.read.parquet(filePath)

// Extract top-level category from the 'category_code'
val dfWithTopLevelCategory = df.withColumn("top_level_category", split(col("category_code"), "\\.").getItem(0))

// Group by brand, category, and event type ('view' and 'purchase') to calculate counts
val groupedDf = dfWithTopLevelCategory.filter(col("event_type").isin("view", "purchase"))
  .groupBy("brand", "top_level_category", "event_type")
  .agg(count("*").alias("event_count"))

// Pivot the event type to get separate columns for 'view' and 'purchase' counts 
val pivotedDf = groupedDf.groupBy("brand", "top_level_category")
  .pivot("event_type", Seq("view", "purchase"))
  .agg(first("event_count"))
  .na.fill(0, Seq("view", "purchase")) // Fill any null values with 0

// Add a column for total engagement (views + purchases)
val dfWithTotalEngagement = pivotedDf.withColumn("total_engagement", col("view") + col("purchase"))

// Collect data for the chart
val result = dfWithTotalEngagement.collect()

// Create the dataset for JFreeChart (bar chart)
val dataset = new DefaultCategoryDataset
result.foreach(row => {
  val brand = row.getString(0)
  val category = row.getString(1)
  val totalEngagement = row.getLong(4) // 'total_engagement'

  // Add data to the dataset for total engagement
  dataset.addValue(totalEngagement, brand, category) // Use brand as series and category as domain
})

// Create a bar chart for brand engagement by category
val chart = ChartFactory.createBarChart(
  "Brand Engagement by Category",
  "Product Category",
  "Total Engagement (Views + Purchases)",
  dataset,
  PlotOrientation.VERTICAL,
  false, // Exclude legend from the chart
  true,
  false
)

// Adjust the width of the bars
val plot: CategoryPlot = chart.getCategoryPlot()
val renderer = new BarRenderer()
renderer.setMaximumBarWidth(0.3) // Increase the bar width to make the bars clearer
plot.setRenderer(renderer)

// Set unique colors for each brand using a random color generator
val uniqueColors = (0 until dataset.getRowCount).map(_ => new Color(Random.nextInt(256), Random.nextInt(256), Random.nextInt(256))).toArray
for (i <- 0 until dataset.getRowCount) {
  renderer.setSeriesPaint(i, uniqueColors(i))
}

// Create a separate legend image
val legendDataset = new DefaultCategoryDataset
result.foreach(row => {
  val brand = row.getString(0)
  val category = row.getString(1)
  legendDataset.addValue(0, brand, category) // Use brand as series and category as domain to generate legend
})

val legendChart = ChartFactory.createBarChart(
  null, // No title for legend chart
  null, // No category axis label
  null, // No value axis label
  legendDataset,
  PlotOrientation.VERTICAL,
  true, // Include legend
  false,
  false
)

val legendTitle: LegendTitle = legendChart.getLegend
legendTitle.setItemFont(new Font("SansSerif", Font.PLAIN, 10)) // Set font size for the legend
val legendImage: BufferedImage = new BufferedImage(1000, 200, BufferedImage.TYPE_INT_ARGB)
val graphics = legendImage.createGraphics()
legendChart.draw(graphics, new java.awt.geom.Rectangle2D.Double(0, 0, 1000, 200))

// Save the chart and legend as separate PNG files
val chartFile = new File("brand_engagement_by_category.png")
ChartUtils.saveChartAsPNG(chartFile, chart, 1000, 800)

val legendFile = new File("brand_engagement_legend.png")
ImageIO.write(legendImage, "png", legendFile)

println("Chart saved as 'brand_engagement_by_category.png'")
println("Legend saved as 'brand_engagement_legend.png'")

