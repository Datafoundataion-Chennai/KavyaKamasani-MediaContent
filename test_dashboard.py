import unittest
import pandas as pd # type: ignore
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, RetryError # type: ignore
from Dashboards.y6 import fetch_data
from textblob import TextBlob # type: ignore
import pytest # type: ignore

@pytest.mark.unittest
class TestMediaAnalyticsDashboard(unittest.TestCase):
    def setUp(self):
        """Set up test environment."""
        self.dataset_options = ["YouTube", "News", "Custom Insights"]
        self.sample_youtube_data = pd.DataFrame({
            "video_id": ["1", "2"],
            "title": ["Video 1", "Video 2"],
            "channel_title": ["Channel A", "Channel B"],
            "category_id": [10, 20],
            "publish_time": ["2023-01-01", "2023-01-02"],
            "views": [100, 200],
            "likes": [10, 20],
            "comment_count": [5, 10],
            "comments_disabled": [False, False],
            "tags": ["tag1, tag2", "tag3, tag4"],
            "description": ["Desc 1", "Desc 2"],
            "thumbnail_link": ["http://link1.com", "http://link2.com"]
        })
        self.sample_news_data = pd.DataFrame({
            "category": ["Sports", "Politics"],
            "headline": ["Headline 1", "Headline 2"],
            "short_description": ["Desc 1", "Desc 2"],
            "link": ["http://link1.com", "http://link2.com"],
            "date": ["2023-01-01", "2023-01-02"]
        })

    def test_fetch_data_youtube(self):
        """Test fetch_data for YouTube dataset."""
        try:
            df = fetch_data("YouTube")
            self.assertIsInstance(df, pd.DataFrame)
            self.assertFalse(df.empty)
        except Exception as e:
            self.fail(f"fetch_data for YouTube failed with error: {e}")

    def test_fetch_data_news(self):
        """Test fetch_data for News dataset."""
        try:
            df = fetch_data("News")
            self.assertIsInstance(df, pd.DataFrame)
            self.assertFalse(df.empty)
        except Exception as e:
            self.fail(f"fetch_data for News failed with error: {e}")

    def test_fetch_data_custom_insights(self):
        """Test fetch_data for Custom Insights dataset."""
        try:
            df = fetch_data("Custom Insights")
            self.assertIsInstance(df, pd.DataFrame)
            self.assertFalse(df.empty)
        except Exception as e:
            self.fail(f"fetch_data for Custom Insights failed with error: {e}")

    def test_youtube_filters(self):
        """Test YouTube filtering logic."""
        filtered_df = self.sample_youtube_data
        # Filter by channel
        filtered_df = filtered_df[filtered_df['channel_title'] == "Channel A"]
        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['title'], "Video 1")

        # Filter by category
        filtered_df = self.sample_youtube_data
        filtered_df = filtered_df[filtered_df['category_id'] == 10]
        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['title'], "Video 1")

        # Filter by tags
        filtered_df = self.sample_youtube_data
        selected_tags = ["tag1"]
        filtered_df = filtered_df[
            filtered_df['tags'].apply(lambda x: any(tag in (x or "") for tag in selected_tags))
        ]
        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['title'], "Video 1")

    def test_news_filters(self):
        """Test News filtering logic."""
        filtered_df = self.sample_news_data
        # Filter by category
        filtered_df = filtered_df[filtered_df['category'] == "Sports"]
        self.assertEqual(len(filtered_df), 1)
        self.assertEqual(filtered_df.iloc[0]['headline'], "Headline 1")

        # Filter by date range
        filtered_df = self.sample_news_data
        min_date = pd.to_datetime("2023-01-01")
        max_date = pd.to_datetime("2023-01-02")
        filtered_df = filtered_df[
            (pd.to_datetime(filtered_df['date']) >= min_date) &
            (pd.to_datetime(filtered_df['date']) <= max_date)
        ]
        self.assertEqual(len(filtered_df), 2)

    def test_pagination(self):
        """Test pagination logic."""
        df = self.sample_youtube_data
        page_size = 1
        total_pages = (len(df) // page_size) + (1 if len(df) % page_size != 0 else 0)
        self.assertEqual(total_pages, 2)

        # Test first page
        paginated_df = df.iloc[0:1]
        self.assertEqual(len(paginated_df), 1)
        self.assertEqual(paginated_df.iloc[0]['title'], "Video 1")

        # Test second page
        paginated_df = df.iloc[1:2]
        self.assertEqual(len(paginated_df), 1)
        self.assertEqual(paginated_df.iloc[0]['title'], "Video 2")

    def test_sentiment_analysis(self):
        """Test sentiment analysis logic."""
        sample_data = pd.DataFrame({
            "headline": ["Great news!", "Bad news.", None],
            "description": ["Positive description.", "Negative description.", None]
        })
        # Test sentiment for headlines
        sample_data['sentiment'] = sample_data['headline'].apply(
            lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0
        )
        self.assertGreater(sample_data.iloc[0]['sentiment'], 0)
        self.assertLess(sample_data.iloc[1]['sentiment'], 0)
        self.assertEqual(sample_data.iloc[2]['sentiment'], 0)

        # Test sentiment for descriptions
        sample_data['sentiment'] = sample_data['description'].apply(
            lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0
        )
        self.assertGreater(sample_data.iloc[0]['sentiment'], 0)
        self.assertLess(sample_data.iloc[1]['sentiment'], 0)
        self.assertEqual(sample_data.iloc[2]['sentiment'], 0)


if __name__ == "__main__":
    unittest.main()