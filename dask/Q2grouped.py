import dask.dataframe as dd
import pandas as pd  # Required for merging the results
import matplotlib.pyplot as plt
import seaborn as sns
from dask.diagnostics import ProgressBar
ProgressBar().register()
# Load your dataset
df = dd.read_parquet('/home/nouarif4/Downloads/DaskCleanedData.parquet')

# Ensure that 'category_code' exists
if 'category_code' not in df.columns:
    raise ValueError("The 'category_code' column is missing from the DataFrame.")

# Extract the top-level category from 'category_code'
df['top_level_category'] = df['category_code'].map(lambda x: x.split('.')[0], meta=('category_code', 'object'))

# Define price ranges and labels
price_bins = [0, 20, 50, 100, 200, 500, float('inf')]
price_labels = ['0-20', '21-50', '51-100', '101-200', '201-500', '500+']

# Custom function to assign price range based on price
def assign_price_range(price):
    if price <= 20:
        return '0-20'
    elif price <= 50:
        return '21-50'
    elif price <= 100:
        return '51-100'
    elif price <= 200:
        return '101-200'
    elif price <= 500:
        return '201-500'
    else:
        return '500+'

# Apply the function to create a new column for price ranges
df['price_range'] = df['price'].map(assign_price_range, meta=('price', 'object'))

# Aggregate views and add to cart events separately, using top-level category
views_df = df[df['event_type'] == 'view'].groupby(['price_range', 'top_level_category']).size().reset_index()
views_df.columns = ['price_range', 'top_level_category', 'total_views']  # Rename columns manually

cart_df = df[df['event_type'] == 'add_to_cart'].groupby(['price_range', 'top_level_category']).size().reset_index()
cart_df.columns = ['price_range', 'top_level_category', 'total_add_to_cart']  # Rename columns manually

# Compute Dask DataFrames before merging
views_df = views_df.compute()
cart_df = cart_df.compute()

# Merge the two Pandas DataFrames
agg_df = views_df.merge(cart_df, on=['price_range', 'top_level_category'], how='left')

# Fill NaN values with 0 (in case there are no 'add_to_cart' events for some categories)
agg_df['total_add_to_cart'] = agg_df['total_add_to_cart'].fillna(0)

# Compute conversion rates
agg_df['conversion_rate'] = agg_df['total_add_to_cart'] / agg_df['total_views']

# Plotting (Pandas DataFrame)
plt.figure(figsize=(12, 6))
sns.set_theme(style="whitegrid")

# Create bar plot for total views
bar_plot = sns.barplot(x='price_range', y='total_views', hue='top_level_category', data=agg_df, alpha=0.6)

# Create line plot for conversion rates on the same axes
line_plot = sns.lineplot(x='price_range', y='conversion_rate', data=agg_df, marker='o', color='red', linewidth=2.5)

# Add labels and title
plt.title('Total Views and Conversion Rates by Price Range and Category Group')
plt.ylabel('Total Views / Conversion Rate')
plt.xlabel('Price Range')
plt.axhline(0, color='gray', lw=1)
plt.legend(title='Top-Level Category', loc='upper left')
plt.grid()

# Save the plot to a file
plt.savefig('viewsconversionrates_by_price_range_and_category.png')
plt.close()

print("Plot saved as 'views_conversion_rates_by_price_range_and_category.png'.")
