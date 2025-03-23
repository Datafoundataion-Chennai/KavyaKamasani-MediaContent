from textblob import TextBlob
import pandas as pd
df_news = pd.read_csv("newscategory_cleaned.csv")
def get_sentiment(text):
    if not isinstance(text, str):
        text = str(text)  
    blob = TextBlob(text) 
    polarity = blob.sentiment.polarity 
    if polarity > 0:
        return 'positive'
    elif polarity < 0:
        return 'negative'
    else:
        return 'neutral'
df_news['headline_sentiment'] = df_news['headline'].apply(get_sentiment)
df_news['short_description_sentiment'] = df_news['short_description'].apply(get_sentiment)
df_news.to_csv("newscategory_with_sentiment.csv", index=False)
print(df_news[['headline', 'headline_sentiment']].head())
print(df_news['headline_sentiment'].value_counts())

