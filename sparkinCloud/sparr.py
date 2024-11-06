import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load data from Google Cloud Storage
df = pd.read_csv("/home/nouarif4/Downloads/results_conversion_rates.csv_part-00000-f02e829d-d12c-492a-b00d-223e0c7e6274-c000.csv")

# Create a figure and axes
fig, ax1 = plt.subplots(figsize=(12, 6))

# Bar plot for total views
bar_plot = sns.barplot(data=df, x='price_range', y='view', hue='top_level_category', palette='viridis', alpha=0.6, ax=ax1)

# Create a secondary y-axis for conversion rate
ax2 = ax1.twinx()
line_plot = sns.lineplot(data=df, x='price_range', y='conversion_rate', color='red', marker='o', ax=ax2)

# Set labels and titles
ax1.set_xlabel("Price Range")
ax1.set_ylabel("Total Views")
ax2.set_ylabel("Conversion Rate")
ax1.set_title("Total Views and Conversion Rate by Price Range and Category")
plt.savefig("views_conversion_rate_by_price_range.png", bbox_inches='tight', dpi=300)


