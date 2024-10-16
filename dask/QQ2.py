import dask.dataframe as dd
import pandas as pd  # Required for merging the results
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar
import seaborn as sns
ProgressBar().register()
# Load your dataset
df = dd.read_parquet('/home/nouarif4/Downloads/DaskCleanedData.parquet')

# Ensure that 'category_code' exists
if 'category_code' not in df.columns:
    raise ValueError("The 'category_code' column is missing from the DataFrame.")

# Extract the top-level category from 'category_code'
df['category_groups'] = df['category_code'].map(lambda x: x.split('.')[0], meta=('category_code', 'object'))

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

# ... (your previous code)

# Apply the function to create a new column for price ranges in the original Dask DataFrame
df['price_range'] = df['price'].map(assign_price_range, meta=('price', 'object'))

# Aggregate views and cart events separately, using top-level category
views_df = df[df['event_type'] == 'view'].groupby(['price_range', 'category_groups']).size().reset_index()
views_df.columns = ['price_range', 'category_groups', 'total_views']

cart_df = df[df['event_type'] == 'cart'].groupby(['price_range', 'category_groups']).size().reset_index()
cart_df.columns = ['price_range', 'category_groups', 'total_add_to_cart']

# Compute Dask DataFrames before merging
views_df = views_df.compute()
cart_df = cart_df.compute()

# Merge the two Pandas DataFrames
agg_df = views_df.merge(cart_df, on=['price_range', 'category_groups'], how='left')

# Fill NaN values with 0 (in case there are no 'cart' events for some categories)
agg_df['total_add_to_cart'] = agg_df['total_add_to_cart'].fillna(0)

# Compute conversion rates
agg_df['conversion_rate'] = agg_df['total_add_to_cart'] / agg_df['total_views'].replace(0, pd.NA)  # Avoid division by zero

# Convert 'price_range' to a categorical type with the specified order
price_order = ['0-20', '21-50', '51-100', '101-200', '201-500', '500+']
agg_df['price_range'] = pd.Categorical(agg_df['price_range'], categories=price_order, ordered=True)

print(agg_df[['price_range', 'category_groups', 'total_views', 'total_add_to_cart', 'conversion_rate']])

# Plotting (Pandas DataFrame)
plt.figure(figsize=(12, 6))
sns.set_theme(style="whitegrid")

# Create bar plot for total views
bar_plot = sns.barplot(x='price_range', y='total_views', hue='category_groups', data=agg_df, alpha=0.6, palette="Set2")
bar_plot.set_ylim(0, agg_df['total_views'].max() + 10)  # Adjust this based on the maximum total views
print(agg_df[['price_range', 'category_groups', 'total_views']])  # Debugging print

# Create a secondary y-axis for conversion rates
ax2 = bar_plot.twinx()
line_plot = sns.lineplot(
    x='price_range', 
    y='conversion_rate', 
    data=agg_df, 
    marker='o', 
    color='darkorchid', 
    linewidth=2.5,
    label='Conversion Rate'  # Add a label for the line plot
)

# Set y-limits for conversion rates if needed
max_conversion_rate = agg_df['conversion_rate'].max()
ax2.set_ylim(0, max_conversion_rate + 0.05)  # Adjust the buffer as needed

# Add labels and title
plt.title('Total Views and Conversion Rates by Price Range and Category Groups ')
bar_plot.set_ylabel('Total Views')
ax2.set_ylabel('Conversion Rate')
plt.xlabel('Price Range')
plt.axhline(0, color='gray', lw=1)
plt.legend(loc='upper right')
plt.grid()

# Save the plot to a file
plt.savefig('prgory.png')
plt.close()

print("Plot saved as 'prgory.png'.")


1. Loading the Dataset
2. Extracting Category Groups
3. Defining Price Ranges
4. Assigning Price Ranges
5. Aggregating Views and Cart Events
6. Merging Aggregated Data (left join of views and carts, so the views are reserved even if there are no corresponding rows in cart)
7. Filling Missing Values
8. Calculating Conversion Rates (ratio of total add to cart to total views)
9. Converting Price Range to Categorical (so to organize it in ascending order)
10. Plotting








