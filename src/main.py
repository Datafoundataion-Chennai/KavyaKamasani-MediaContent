from data_fetch import get_all_channel_data
from data_processing import clean_data
import pandas as pd
def main():
    videos = get_all_channel_data()
    if videos:
        df = clean_data(videos)
        if not df.empty:
            df.to_csv("youtube_videos.csv", index=False)
            print(f"Data saved to youtube_videos.csv (Total Videos Fetched: {len(df)})")
            print(df.head()) 
        else:
            print(" No data was fetched. Please check your API key or channel IDs.")
    else:
        print("No data fetched.")
if __name__ == "__main__":
    main()
