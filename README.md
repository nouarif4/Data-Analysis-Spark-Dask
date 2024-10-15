# Data Analysis Spark Dask
This repository contains code for distributed data processing and analysis using Apache Spark and Dask. The analysis covers eCommerce online store reviews, including visualization of conversion rates, engagement metrics, and price range impacts.


## Project Overview

- **Data Source**: [eCommerce store reviews dataset]([https://example.com/path-to-dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)) 
- **Processing Systems**: 
  - **Apache Spark** (using `spark-shell` and Scala)
  - **Dask** (using Python scripts in the terminal)
- **Visualization Tools**:
  - **Spark**: JFreeChart (a Java library, but you can use JFreeChart in Scala within the Spark Shell.)
  - **Dask**: Matplotlib and Seaborn (Python libraries for visualizations)
- **Platform**: Ubuntu terminal


## How to Run

### Spark
1. Open the `spark-shell --packages org.jfree:jfreechart:1.5.3 ` this will include the JFreeChart library as a dependency when starting Spark Shell
2. Load the Scala scripts in the repository.
3. Execute the analysis scripts.
4. The JFreeChart will visualize the results.

### Dask
1. Open a terminal.
2. Run the Python scripts in the `dask/` directory. (you use `nano` before the file name for opening the file, and `python` before the file name to run the file)
3. View the results using Matplotlib and Seaborn visualizations.



