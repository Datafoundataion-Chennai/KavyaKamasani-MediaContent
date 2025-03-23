import os
import requests # type: ignore
import pandas as pd # type: ignore
import time
from config import API_KEY, CHANNELS

def get_channel_statistics(channel_id):
    url = "https://www.googleapis.com/youtube/v3/channels"
    params = {
        "part": "statistics",
        "id": channel_id,
        "key": API_KEY
    }
    response = requests.get(url, params=params).json()

    if "items" in response and len(response["items"]) > 0:
        stats = response["items"][0]["statistics"]
        return {
            "subscribers_count": stats.get("subscriberCount", 0),
            "total_videos": stats.get("videoCount", 0),
        }
    return {"subscribers_count": 0, "total_videos": 0}

def get_channel_videos(channel_id):
    base_url = "https://www.googleapis.com/youtube/v3/search"
    video_ids = []
    next_page_token = None

    while len(video_ids) < 10000:
        params = {
            "part": "id",
            "channelId": channel_id,
            "maxResults": 50,
            "order": "date",
            "type": "video",
            "pageToken": next_page_token,
            "key": API_KEY
        }
        response = requests.get(base_url, params=params).json()

        if "items" in response:
            video_ids.extend([item["id"]["videoId"] for item in response["items"]])
        next_page_token = response.get("nextPageToken")

        if not next_page_token:
            break  

        time.sleep(1)  

    return video_ids[:10000]  


def get_video_details(video_ids):
    video_url = "https://www.googleapis.com/youtube/v3/videos"
    videos = []

    for i in range(0, len(video_ids), 50): 
        params = {
            "part": "snippet,statistics",
            "id": ",".join(video_ids[i:i+50]),
            "key": API_KEY
        }
        response = requests.get(video_url, params=params).json()

        for item in response.get("items", []):
            snippet = item["snippet"]
            stats = item.get("statistics", {})

            videos.append({
                "video_id": item["id"],
                "title": snippet["title"],
                "channel_title": snippet["channelTitle"],
                "category_id": snippet.get("categoryId", ""),
                "publish_time": snippet["publishedAt"],
                "description": snippet.get("description", ""),
                "tags": ", ".join(snippet.get("tags", [])),
                "thumbnail_link": snippet["thumbnails"]["high"]["url"],
                "video_link": f"https://www.youtube.com/watch?v={item['id']}",
                "views": stats.get("viewCount", 0),
                "likes": stats.get("likeCount", 0),
                "comment_count": stats.get("commentCount", 0),
                "comments_disabled": "commentCount" not in stats
            })
        time.sleep(1) 

    return videos

def get_all_channel_data():
    all_videos = []

    for channel_name, channel_id in CHANNELS.items():
        print(f"Fetching data for {channel_name}...")
        channel_stats = get_channel_statistics(channel_id)
        video_ids = get_channel_videos(channel_id)
        if not video_ids:
            print(f" No videos found for {channel_name}")
            continue
        videos = get_video_details(video_ids)
        for video in videos:
            video.update({
                "channel_name": channel_name,
                "subscribers_count": channel_stats["subscribers_count"],
                "total_videos": channel_stats["total_videos"]
            })

        all_videos.extend(videos)
    return pd.DataFrame(all_videos)