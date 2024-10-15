import org.jfree.chart.{ChartFactory, ChartPanel, ChartUtils}
import org.jfree.chart.axis.{CategoryAxis, CategoryLabelPositions}
import org.jfree.chart.plot.CategoryPlot
import org.jfree.chart.renderer.category.BarRenderer
import javax.swing.{JFrame, SwingUtilities}
import org.jfree.data.category.DefaultCategoryDataset
import javax.swing.WindowConstants
import org.apache.spark.sql.functions._
import java.io.File

//don't forget to start the spark-shell with dependencies(in readme file)


// Read data from Parquet file
val df = spark.read.parquet("/home/nouarif4/Downloads/SparkCleanedData.parquet") 

// Clean and process the data to calculate conversion rates, sorted by descending conversion_rate
val conversionRates = df
  .groupBy("category_code")
  .agg(
    count(when(col("event_type") === "purchase", true)).alias("purchases"),
    count(when(col("event_type") === "view", true)).alias("views"),
    (count(when(col("event_type") === "purchase", true)).cast("double") / count(when(col("event_type") === "view", true)).cast("double")).alias("conversion_rate")  // Ensure conversion rate is Double
  )
  .orderBy(desc("conversion_rate"))  // Sort by descending conversion rate

// Show conversion rates
conversionRates.show()

// Create dataset for visualization
val dataset = new DefaultCategoryDataset()

// Populate dataset with conversion rates
conversionRates.collect().foreach { row =>
  val category = row.getString(0)
  val conversionRate = row.getAs[Double]("conversion_rate")  // Use getAs to fetch the conversion rate as Double
  dataset.addValue(conversionRate, "Conversion Rate", category)
}

// Function to create, display, and save the chart
def createAndSaveChart(): Unit = {
  val chart = ChartFactory.createBarChart(
    "Product Conversion Rates (Sorted by Conversion Rate)",
    "Category",
    "Conversion Rate",
    dataset
  )

  // Get the plot and configure it
  val plot = chart.getCategoryPlot()

  // Rotate category labels by 45 degrees
  val domainAxis = plot.getDomainAxis().asInstanceOf[CategoryAxis]
  domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45)

  // Adjust the spacing between bars
  val renderer = plot.getRenderer().asInstanceOf[BarRenderer]
  renderer.setItemMargin(0.2)  // Adjust the margin between bars

  // Create and display the frame
  val frame = new JFrame("JFreeChart Example")
  frame.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)
  frame.setContentPane(new ChartPanel(chart))
  frame.pack()
  frame.setSize(2000, 800)  // Widen the chart to 2000x800 for better readability
  frame.setVisible(true)

  // Save the chart as a PNG image file
  val file = new File("/home/nouarif4/Downloads/conversion_rates_chart_wider.png")
  ChartUtils.saveChartAsPNG(file, chart, 2000, 800)  // Save the chart with widened dimensions
}

// Call the function to create, display, and save the chart
SwingUtilities.invokeLater(() => createAndSaveChart())
