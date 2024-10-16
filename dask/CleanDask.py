import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns

# Read the parquet file
df = dd.read_csv("/home/nouarif4/Downloads/2019-Oct.csv")

# Drop rows with null values
clean_df = df.dropna()

# Remove unnecessary columns
columns_to_drop = ["product_id", "category_id", "user_id", "user_session"]
filtered_df = clean_df.drop(columns=columns_to_drop)

# Remove duplicate rows
no_duplicated_df = filtered_df.drop_duplicates()

# Count rows after cleaning
row_count = no_duplicated_df.compute().shape[0]

# Write cleaned data to a new parquet file
no_duplicated_df.to_parquet("/home/nouarif4/Downloads/DaskCleanedData.parquet")

print(f"Number of rows after cleaning: {row_count}")


