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

// Select the top 20 brands by total engagement
val topBrandsDf = dfWithTotalEngagement.orderBy(col("total_engagement").desc).limit(20)

// Collect data for the chart
val result = topBrandsDf.collect()

// Create a color mapping for each brand
val uniqueColors = result.map(row => row.getString(0) -> new Color(Random.nextInt(256), Random.nextInt(256), Random.nextInt(256))).toMap

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
  "Top 20 Brand Engagement by Category",
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

// Set unique colors for each brand using the shared color mapping
result.zipWithIndex.foreach { case (row, index) =>
  val brand = row.getString(0)
  renderer.setSeriesPaint(index, uniqueColors(brand))
}

// Create a separate legend image manually with updated top 20 brands
val legendWidth = 1000
val legendHeight = 400
val legendImage = new BufferedImage(legendWidth, legendHeight, BufferedImage.TYPE_INT_ARGB)
val graphics: Graphics2D = legendImage.createGraphics()

// Set font for the legend
graphics.setFont(new Font("SansSerif", Font.PLAIN, 14))

graphics.drawString("Legend:", 10, 20)

// Draw legend entries
var legendXStart = 10
var legendYPosition = 40
val boxSize = 15
val boxSpacing = 10

uniqueColors.foreach { case (brand, color) =>
  // Set color and draw the color box
  graphics.setPaint(color)
  graphics.fillRect(legendXStart, legendYPosition - boxSize, boxSize, boxSize)

  // Draw the brand name next to the color box
  graphics.setPaint(Color.BLACK)
  graphics.drawString(brand, legendXStart + boxSize + boxSpacing, legendYPosition)

  legendYPosition += 25
  if (legendYPosition > legendHeight - 20) {
    legendYPosition = 40
    legendXStart += 250
  }
}

// Save the chart and legend as separate PNG files
val chartFile = new File("top_20_brand_engagement_by_category.png")
ChartUtils.saveChartAsPNG(chartFile, chart, 1000, 800)

val legendFile = new File("top_20_brand_engagement_legend.png")
ImageIO.write(legendImage, "png", legendFile)

println("Chart saved as 'top_20_brand_engagement_by_category.png'")
println("Legend saved as 'top_20_brand_engagement_legend.png'")

