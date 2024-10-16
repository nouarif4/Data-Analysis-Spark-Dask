import dask.dataframe as dd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the dataset from the specified Parquet file
df = dd.read_parquet('/home/nouarif4/Downloads/DaskCleanedData.parquet')

# Compute the DataFrame to get a Pandas DataFrame
pandas_df = df.compute()

# Group by 'category_code' and calculate conversion rate
conversion_rate_df = pandas_df.groupby('category_code').agg({
    'event_type': lambda x: (x == 'purchase').sum() / len(x) * 100
}).reset_index()

# Rename the columns for clarity
conversion_rate_df.columns = ['category_code', 'conversion_rate']

# Sort the data by conversion rate (descending order)
conversion_rate_df = conversion_rate_df.sort_values('conversion_rate', ascending=False)

# Plot using Seaborn
plt.figure(figsize=(15, 13))  # Adjust figure size for readability
sns.barplot(x="category_code", y="conversion_rate", data=conversion_rate_df)

# Rotate x-axis labels for better readability and adjust font size
plt.xticks(rotation=90, fontsize=8)

# Add labels and title
plt.xlabel("Category Code")
plt.ylabel("Conversion Rate (%)")
plt.title("Conversion Rate by Category (Highest to Lowest)")

# Save the plot as an image
plt.tight_layout()  # Adjust the layout so everything fits
plt.savefig("/home/nouarif4/conversion_rate_sorted.png")
# Inform the user
print("Plot saved as 'conversion_rate_sorted.png'.")
