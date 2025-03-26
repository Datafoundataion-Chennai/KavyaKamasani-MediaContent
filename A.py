import streamlit as st  # type: ignore
from google.cloud import bigquery
import pandas as pd  # type: ignore
import os
import logging
from google.api_core.exceptions import GoogleAPICallError, RetryError  # type: ignore
from textblob import TextBlob  # type: ignore


log_file = "dashboard.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logging.info("Dashboard started successfully!")

def initialize_bigquery_client(credentials_path):
    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        client = bigquery.Client()
        logging.info(f"BigQuery client initialized successfully using {credentials_path}.")
        return client
    except Exception as e:
        logging.error(f"Failed to initialize BigQuery client: {e}")
        st.error(f"Failed to initialize BigQuery client: {e}")
        st.stop()


st.sidebar.header("Step 0: Select Your Role")
role_options = ["BigQuery Admin", "BigQuery User"]
selected_role = st.sidebar.selectbox("Select Role", role_options)


if selected_role == "BigQuery Admin":
    credentials_path = r"C:\Users\kavya\Downloads\credentials.json"  # Path to Admin Credentials
elif selected_role == "BigQuery User":
    credentials_path = r"C:\Users\kavya\Downloads\mediaanalyticsplatform-50fb330415fa.json"  # Path to User Credentials


client = initialize_bigquery_client(credentials_path)


st.title("Media Analytics Dashboard")

st.sidebar.header("Step 1: Choose a Dataset")
dataset_options = ["Select", "YouTube", "News", "Custom Insights"]
selected_dataset = st.sidebar.selectbox("Select Dataset", dataset_options)

if selected_dataset == "Select":
    logging.info("No dataset selected. Execution stopped.")
    st.info("Please select a dataset to proceed.")
    st.stop()

@st.cache_data
def fetch_data(dataset):
    try:
        if dataset == "YouTube":
            query = """
            SELECT video_id, title, channel_title, category_id, publish_time, views, likes, comment_count, 
                comments_disabled, tags, description, thumbnail_link
            FROM `mediaanalyticsplatform.media_analytics.youtube_videos`
            """
        elif dataset == "News":
            query = """
            SELECT category, headline, short_description, link, date
            FROM 'mediaanalyticsplatform.media_analytics.newscategory_cleaned'
            """
        elif dataset == "Custom Insights":
            query = """
            SELECT 
                'YouTube' AS dataset_type, 
                video_id, 
                title, 
                channel_title, 
                category_id, 
                publish_time, 
                views, 
                likes, 
                comment_count, 
                comments_disabled, 
                tags, 
                description,
                thumbnail_link,
                NULL AS category,  -- Add NULL for missing columns in YouTube data
                NULL AS headline,
                NULL AS short_description,
                NULL AS link,
                NULL AS date
            FROM `mediaanalyticsplatform.media_analytics.youtube_videos`
            UNION ALL
            SELECT 
                'News' AS dataset_type, 
                NULL AS video_id,  -- Add NULL for missing columns in News data
                headline AS title,
                NULL AS channel_title,
                NULL AS category_id,
                NULL AS publish_time,
                NULL AS views,
                NULL AS likes,
                NULL AS comment_count,
                NULL AS comments_disabled,
                NULL AS tags,
                NULL AS description,
                NULL AS thumbnail_link,
                category, 
                headline, 
                short_description, 
                link, 
                date
            FROM `mediaanalyticsplatform.media_analytics.newscategory_cleaned`
            """
        logging.info(f"Fetching data for dataset: {dataset}")
        return client.query(query).to_dataframe()
    except GoogleAPICallError as e:
        logging.error(f"Failed to fetch data from BigQuery: {e}")
        st.error(f"Failed to fetch data from BigQuery: {e}")
        return pd.DataFrame()
    except RetryError as e:
        logging.error(f"Request timed out. Please try again later: {e}")
        st.error(f"Request timed out. Please try again later: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"An unexpected error occurred while fetching data: {e}")
        st.error(f"An unexpected error occurred while fetching data: {e}")
        return pd.DataFrame()

try:
    df = fetch_data(selected_dataset)
    logging.info(f"Data fetched successfully for dataset: {selected_dataset}")
except Exception as e:
    logging.error(f"An error occurred while processing the dataset: {e}")
    st.error(f"An error occurred while processing the dataset: {e}")
    st.stop()


if selected_dataset in ["YouTube", "News"]:
    st.sidebar.header("Step 2: Apply Filters")
    filtered_df = df
    if selected_dataset == "YouTube":
        st.sidebar.subheader("YouTube Filters")
        try:
            channel_options = ["All"] + list(df['channel_title'].unique())
            selected_channel = st.sidebar.selectbox("Select YouTube Channel", channel_options)
            category_options = ["All"] + list(df['category_id'].astype(str).unique())
            selected_category = st.sidebar.selectbox("Select Category", category_options)
            tag_input = st.sidebar.text_input("Enter Tags (comma-separated)")
            selected_tags = [tag.strip() for tag in tag_input.split(",")] if tag_input else []
            if selected_channel != "All":
                filtered_df = filtered_df[filtered_df['channel_title'] == selected_channel]
            if selected_category != "All":
                filtered_df = filtered_df[filtered_df['category_id'].astype(str) == selected_category]
            if selected_tags:
                filtered_df = filtered_df[
                    filtered_df['tags'].apply(
                        lambda x: any(tag in (x or "") for tag in selected_tags)
                    )
                ]
            logging.info("YouTube filters applied successfully.")
        except KeyError as e:
            logging.error(f"Missing required column in the dataset: {e}")
            st.error(f"Missing required column in the dataset: {e}")
            st.stop()
    elif selected_dataset == "News":
        st.sidebar.subheader("News Filters")
        try:
            category_options = ["All"] + list(df['category'].unique())
            selected_category = st.sidebar.selectbox("Select News Category", category_options)
            min_date = pd.to_datetime(df['date']).min()
            max_date = pd.to_datetime(df['date']).max()
            selected_date_range = st.sidebar.date_input("Select Date Range", [min_date, max_date])
            if selected_category != "All":
                filtered_df = filtered_df[filtered_df['category'] == selected_category]
            if len(selected_date_range) == 2:
                filtered_df = filtered_df[
                    (pd.to_datetime(filtered_df['date']) >= pd.to_datetime(selected_date_range[0])) &
                    (pd.to_datetime(filtered_df['date']) <= pd.to_datetime(selected_date_range[1]))
                ]
            logging.info("News filters applied successfully.")
        except KeyError as e:
            logging.error(f"Missing required column in the dataset: {e}")
            st.error(f"Missing required column in the dataset: {e}")
            st.stop()

    st.header("Step 3: Preview Data")
    st.sidebar.header("Pagination")
    page_size = st.sidebar.number_input("Rows per page", min_value=1, max_value=100, value=10)
    if len(filtered_df) == 0:
        logging.warning("No data found for the selected filters.")
        st.warning("No data found for the selected filters.")
        total_pages = 1
    else:
        total_pages = (len(filtered_df) // page_size) + (1 if len(filtered_df) % page_size != 0 else 0)
    page_number = st.sidebar.number_input("Page number", min_value=1, max_value=max(1, total_pages), value=1)
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    paginated_df = filtered_df.iloc[start_idx:end_idx]
    if len(filtered_df) > 0:
        st.write(f"Showing rows {start_idx + 1} to {min(end_idx, len(filtered_df))} of {len(filtered_df)}")
        st.dataframe(paginated_df)
    else:
        st.write("No data to display.")

    
    if selected_dataset == "YouTube":
        st.header("YouTube Data Visualizations")
        st.subheader("Trending Videos Preview")
        for index, row in filtered_df.head(5).iterrows():
            if pd.notna(row['thumbnail_link']):
                st.image(row['thumbnail_link'], caption=row['title'])
            else:
                st.write(f"No thumbnail available for video: {row['title']}")
        st.subheader("Bar Plot: Top 10 Channels by Views")
        try:
            top_channels = filtered_df.groupby('channel_title')['views'].sum().nlargest(10).reset_index()
            st.bar_chart(top_channels.set_index('channel_title'))
            logging.info("Bar plot generated successfully.")
        except Exception as e:
            logging.error(f"An error occurred while generating the bar plot: {e}")
            st.error(f"An error occurred while generating the bar plot: {e}")
    elif selected_dataset == "News":
        st.header("News Data Visualizations")
        st.subheader("Articles by Category")
        try:
            category_counts = filtered_df['category'].value_counts()
            st.bar_chart(category_counts)
            logging.info("Bar chart generated successfully.")
        except Exception as e:
            logging.error(f"An error occurred while generating the bar chart: {e}")
            st.error(f"An error occurred while generating the bar chart: {e}")
        st.subheader("Articles Published Over Time")
        try:
            time_series_data = filtered_df.groupby(pd.to_datetime(filtered_df['date']).dt.date).size()
            st.line_chart(time_series_data)
            logging.info("Time series chart generated successfully.")
        except Exception as e:
            logging.error(f"An error occurred while generating the time series: {e}")
            st.error(f"An error occurred while generating the time series: {e}")

elif selected_dataset == "Custom Insights":
    st.header("Custom Insights")
    try:
        youtube_df = fetch_data("YouTube")
        news_df = fetch_data("News")
        st.subheader("Trending YouTube Videos (Last 7 Days)")
        last_7_days = pd.Timestamp.now() - pd.Timedelta(days=7)
        trending_videos = youtube_df[youtube_df['publish_time'] >= last_7_days].sort_values(by='views', ascending=False)
        st.write(trending_videos.head(10))
        st.subheader("Top Trending YouTube Categories")
        top_categories = youtube_df['category_id'].value_counts().head(10)
        st.bar_chart(top_categories)
        st.subheader("Most Liked YouTube Videos (All Time)")
        most_liked_videos = youtube_df.sort_values(by='likes', ascending=False)
        st.write(most_liked_videos.head(10))
        st.subheader("News Category Sentiment Analysis")
        try:
            news_df['sentiment'] = news_df['headline'].apply(
                lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0
            )
            st.write(news_df[['headline', 'sentiment']].head(10))
            logging.info("Sentiment analysis on news headlines completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred while performing sentiment analysis: {e}")
            st.error(f"An error occurred while performing sentiment analysis: {e}")
        st.subheader("YouTube Data Sentiment Analysis")
        try:
            youtube_df['sentiment'] = youtube_df['description'].apply(
                lambda x: TextBlob(str(x)).sentiment.polarity if pd.notna(x) else 0
            )
            st.write(youtube_df[['title', 'sentiment']].head(10))
            logging.info("Sentiment analysis on YouTube descriptions completed successfully.")
        except Exception as e:
            logging.error(f"An error occurred while performing sentiment analysis: {e}")
            st.error(f"An error occurred while performing sentiment analysis: {e}")
    except Exception as e:
        logging.error(f"An error occurred while generating custom insights: {e}")
        st.error(f"An error occurred while generating custom insights: {e}")

if selected_role == "BigQuery Admin":
    st.sidebar.header("Admin Operations")
    admin_operation = st.sidebar.selectbox(
        "Select Operation", 
        ["None", "Create Table", "Insert Data", "Update Data", "Delete Data"]
    )

    if admin_operation == "Create Table":
        st.subheader("Create a New Table")
        table_name = st.text_input("Enter Table Name")
        schema_input = st.text_area(
            "Enter Schema (column_name:column_type, separated by commas)", 
            "id:INTEGER,name:STRING"
        )
        if st.button("Create Table"):
            try:
                schema = [
                    bigquery.SchemaField(col.split(":")[0], col.split(":")[1]) 
                    for col in schema_input.split(",")
                ]
                table_ref = client.dataset("media_analytics").table(table_name)
                table = bigquery.Table(table_ref, schema=schema)
                client.create_table(table)
                st.success(f"Table '{table_name}' created successfully!")
                logging.info(f"Table '{table_name}' created successfully.")
            except Exception as e:
                logging.error(f"Failed to create table: {e}")
                st.error(f"Failed to create table: {e}")

    
    elif admin_operation == "Insert Data":
        st.subheader("Insert Data into a Table")
        table_name = st.text_input("Enter Table Name")
        data_input = st.text_area(
            "Enter Data (row values separated by commas)", 
            "1,Sample Name"
        )
        if st.button("Insert Data"):
            try:
                rows_to_insert = [tuple(data_input.split(","))]
                table_ref = client.dataset("media_analytics").table(table_name)
                errors = client.insert_rows(table_ref, rows_to_insert)
                if errors:
                    st.error(f"Errors occurred: {errors}")
                else:
                    st.success(f"Data inserted successfully into '{table_name}'.")
                    logging.info(f"Data inserted successfully into '{table_name}'.")
            except Exception as e:
                logging.error(f"Failed to insert data: {e}")
                st.error(f"Failed to insert data: {e}")

    
    elif admin_operation == "Update Data":
        st.subheader("Update Data in a Table")
        table_name = st.text_input("Enter Table Name")
        update_query = st.text_area(
            "Enter Update Query", 
            "UPDATE media_analytics.table_name SET column_name = 'new_value' WHERE condition"
        )
        if st.button("Run Update Query"):
            try:
                query_job = client.query(update_query)
                query_job.result()  
                st.success("Update query executed successfully.")
                logging.info("Update query executed successfully.")
            except Exception as e:
                logging.error(f"Failed to execute update query: {e}")
                st.error(f"Failed to execute update query: {e}")

    elif admin_operation == "Delete Data":
        st.subheader("Delete Data or Table")
        delete_option = st.selectbox(
            "Choose Delete Option", 
            ["Delete Rows", "Delete Table"]
        )

        if delete_option == "Delete Rows":
            table_name = st.text_input("Enter Table Name")
            delete_query = st.text_area(
                "Enter Delete Query", 
                "DELETE FROM media_analytics.table_name WHERE condition"
            )
            if st.button("Run Delete Query"):
                try:
                    query_job = client.query(delete_query)
                    query_job.result()  
                    st.success("Delete query executed successfully.")
                    logging.info("Delete query executed successfully.")
                except Exception as e:
                    logging.error(f"Failed to execute delete query: {e}")
                    st.error(f"Failed to execute delete query: {e}")

        elif delete_option == "Delete Table":
            table_name = st.text_input("Enter Table Name to Delete")
            if st.button("Delete Table"):
                try:
                    table_ref = client.dataset("media_analytics").table(table_name)
                    client.delete_table(table_ref)  
                    st.success(f"Table '{table_name}' deleted successfully!")
                    logging.info(f"Table '{table_name}' deleted successfully.")
                except Exception as e:
                    logging.error(f"Failed to delete table: {e}")
                    st.error(f"Failed to delete table: {e}")