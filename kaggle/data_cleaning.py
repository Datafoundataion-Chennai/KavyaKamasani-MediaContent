import pandas as pd
df_news = pd.read_json("News_Category_Dataset_v3.json", lines=True)
df_news = df_news[["category", "headline", "short_description", "link", "date"]]
df_news["date"] = pd.to_datetime(df_news["date"])
df_news.dropna(inplace=True)
print(df_news.info())
print(df_news.head())
df_news.to_csv("newscategory_cleaned.csv", index=False)
