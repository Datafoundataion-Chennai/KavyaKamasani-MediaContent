import pandas as pd
from textblob import TextBlob
df = pd.read_csv('youtube_videos.csv')
def get_sentiment(text):
    blob = TextBlob(str(text))
    return blob.sentiment.polarity  
df['title_sentiment'] = df['title'].apply(get_sentiment)  
df['description_sentiment'] = df['description'].apply(get_sentiment)  
def label_sentiment(polarity):
    if polarity > 0:
        return 'Positive'
    elif polarity < 0:
        return 'Negative'
    else:
        return 'Neutral'
df['title_sentiment_label'] = df['title_sentiment'].apply(label_sentiment)
df['description_sentiment_label'] = df['description_sentiment'].apply(label_sentiment)
df.to_csv('youtube_videos_with_sentiment.csv', index=False)
print(df[['title', 'title_sentiment', 'title_sentiment_label', 'description_sentiment', 'description_sentiment_label']].head(10))
