
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from data_processing import clean_data
df = pd.read_csv("youtube_videos.csv")
cleaned_df = clean_data(df)
print("Missing values after filling:")
print(cleaned_df.isnull().sum())
x=df['category_id'].value_counts().head(10)
print(x)
numeric_columns = cleaned_df.select_dtypes(include=['float64', 'int64']).columns
sns.heatmap(cleaned_df[numeric_columns].corr(), annot=True, cmap='coolwarm', fmt='.2f', linewidths=0.5)
plt.show()