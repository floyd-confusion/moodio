# %% [markdown]
# # Data Exploration Notebook
# 
# This notebook provides a template for exploring CSV data files. It includes common data analysis and visualization techniques.

# %%
# Import necessary libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Set plot style
plt.style.use('seaborn')
sns.set_palette('husl')

# Display settings
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
pd.set_option('display.width', 1000)

# %% [markdown]
# ## Load and Explore Data
# 
# Replace 'your_file.csv' with your actual CSV file path.

# %%
# Load the CSV file
# df = pd.read_csv('path/to/your/file.csv')

# Display first few rows
# df.head()

# %% [markdown]
# ## Basic Data Information

# %%
# Get basic information about the dataset
# df.info()

# Get statistical summary
# df.describe()

# %% [markdown]
# ## Data Visualization

# %%
# Example: Create a histogram for a numeric column
# plt.figure(figsize=(10, 6))
# sns.histplot(data=df, x='column_name')
# plt.title('Distribution of Column Name')
# plt.show()

# %%
# Example: Create a correlation heatmap for numeric columns
# plt.figure(figsize=(12, 8))
# sns.heatmap(df.corr(), annot=True, cmap='coolwarm')
# plt.title('Correlation Heatmap')
# plt.show()

# %% [markdown]
# ## Data Cleaning and Preprocessing

# %%
# Check for missing values
# df.isnull().sum()

# Handle missing values (example)
# df = df.fillna(df.mean())  # For numeric columns
# df = df.fillna(df.mode().iloc[0])  # For categorical columns

# %% [markdown]
# ## Advanced Analysis

# %%
# Example: Group by analysis
# df.groupby('category_column').agg({
#     'numeric_column1': ['mean', 'std'],
#     'numeric_column2': ['min', 'max']
# }) 