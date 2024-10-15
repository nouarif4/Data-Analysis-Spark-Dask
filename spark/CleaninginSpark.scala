val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/home/nouarif4/Downloads/2019-Oct.csv")

  // Drop rows with null values
  val cleanDF = df.na.drop()

  // remove unnecessary columns 
  val filteredDF = cleanDF.drop("product_id", "category_id", "user_id", "user_session")

  // Remove duplicate rows
  val noDuplicatedDF = filteredDF.dropDuplicates()

  // Count rows after cleaning
  val rowCount = noDuplicatedDF.count()

  // Write cleaned data to a new parquet file
  noDuplicatedDF.write.mode("overwrite").parquet("/home/nouarif4/Downloads/SparkCleanedData.parquet")

  println(s"Number of rows after cleaning: $rowCount")
 
