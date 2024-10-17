import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the Parquet data file
file_path = r'DaskCleanedData.parquet (Copy)'
df = pd.read_parquet(file_path)

# Drop rows where 'category_code' or 'event_type' is missing
df = df.dropna(subset=['category_code', 'event_type'])

# Ensure 'event_type' includes relevant actions
print("Unique event types:", df['event_type'].unique())

# Group by 'category_code' and calculate cart additions and purchases
category_engagement = df.groupby(['category_code', 'event_type']).size().unstack(fill_value=0).reset_index()

# Calculate the cart abandonment rate
category_engagement['cart_abandonment_rate'] = (
    (category_engagement['cart'] - category_engagement['purchase']) / category_engagement['cart']
) * 100

# Sort categories by highest abandonment rate
category_engagement = category_engagement.sort_values(by='cart_abandonment_rate', ascending=False)

# Plot the top 20 categories with the highest cart abandonment rate
top_abandonments = category_engagement.nlargest(20, 'cart_abandonment_rate')

plt.figure(figsize=(12, 8))
sns.barplot(x='category_code', y='cart_abandonment_rate', data=top_abandonments, palette='coolwarm')

# Add titles and labels
plt.title('Top 20 Categories by Cart Abandonment Rate', fontsize=16)
plt.xlabel('Category', fontsize=12)
plt.ylabel('Cart Abandonment Rate (%)', fontsize=12)

# Rotate x-axis labels for better readability
plt.xticks(rotation=45, ha='right')

# Save the plot to a file
output_file = r'cart_abandonment_rate_plot.png'
plt.tight_layout()
plt.savefig(output_file)

print(f"Plot saved as {output_file}")
