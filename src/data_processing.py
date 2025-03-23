import pandas as pd

def clean_data(videos):
    df = pd.DataFrame(videos)
    df["title"] = df["title"].fillna("Unknown Title")
    df["channel_title"] = df["channel_title"].fillna("Unknown Channel")
    df["category_id"] = df["category_id"].fillna("N/A")
    df["publish_time"] = df["publish_time"].fillna("0000-00-00T00:00:00Z")
    df["description"] = df["description"].fillna("No description available")
    df["tags"] = df["tags"].fillna("No tags")
    df["thumbnail_link"] = df["thumbnail_link"].fillna("No thumbnail")
    df["video_link"] = df["video_link"].fillna("No link")
    df["channel_name"] = df["channel_name"].fillna("Unknown Channel")
    df["views"] = pd.to_numeric(df["views"], errors="coerce").fillna(df['views'].median())
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce").fillna(df['likes'].median())
    df["comment_count"] = pd.to_numeric(df["comment_count"], errors="coerce").fillna(df['comment_count'].median())
    df["subscribers_count"] = pd.to_numeric(df["subscribers_count"], errors="coerce").fillna(df['subscribers_count'].median())
    df["total_videos"] = pd.to_numeric(df["total_videos"], errors="coerce").fillna(df['total_videos'].median())
    df["comments_disabled"] = df["comments_disabled"].fillna("Unknown")    
    
    df["publish_time"] = pd.to_datetime(df["publish_time"], errors="coerce")

    return df
