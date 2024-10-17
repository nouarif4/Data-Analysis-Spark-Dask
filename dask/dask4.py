import dask.dataframe as dd
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the Parquet data file using Dask
file_path = r'DaskCleanedData.parquet (Copy)'
df = dd.read_parquet(file_path)

# Filter rows where 'brand' is missing (if necessary)
df = df.dropna(subset=['brand'])

# Ensure 'event_type' is in the data and includes relevant actions
print("Unique event types:", df['event_type'].unique().compute())

# Group by 'brand' and 'event_type', then compute the size
engagement_counts = df.groupby(['brand', 'event_type']).size().compute()

# Convert the Series to a DataFrame
engagement_counts = engagement_counts.reset_index(name='count')

# Pivot the DataFrame to have event types as columns
engagement_metrics = engagement_counts.pivot(index='brand', columns='event_type', values='count').fillna(0)

# Add a total engagement column
engagement_metrics['total_engagement'] = engagement_metrics.get('view', 0) + engagement_metrics.get('purchase', 0)

# Sort brands by total engagement
engagement_metrics = engagement_metrics.sort_values(by='total_engagement', ascending=False)

# Plot the top brands by total engagement
top_brands = engagement_metrics.nlargest(20, 'total_engagement')

plt.figure(figsize=(12, 8))
sns.barplot(x=top_brands.index, y='total_engagement', data=top_brands.reset_index(), palette='coolwarm')

# Add titles and labels
plt.title('Top 20 Brands by Customer Engagement', fontsize=16)
plt.xlabel('Brand', fontsize=12)
plt.ylabel('Total Engagement (Views + Purchases)', fontsize=12)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right')

# Save the plot to a file
output_file = r'top_brands_engagement_plot.png'
plt.tight_layout()
plt.savefig(output_file)

print(f"Plot saved as {output_file}")
