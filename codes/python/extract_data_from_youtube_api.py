import os
import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
API_VERSION = 'v3'

youtube = build('youtube', API_VERSION, developerKey=API_KEY)

def get_channel_stats(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part='snippet,statistics',
            id=channel_id
        )
        response = request.execute()

        if 'items' in response and response['items']:  # Check if valid data exists
            channel = response['items'][0]
            snippet = channel['snippet']
            stats = channel['statistics']

            return {
                'channel_name': snippet.get('title', ''),  # Empty string if not found
                'total_subscribers': stats.get('subscriberCount', ''),
                'total_views': stats.get('viewCount', ''),
                'total_videos': stats.get('videoCount', ''),
            }
        else:
            print(f"Skipping invalid channel ID: {channel_id}")
            return {'channel_name': '', 'total_subscribers': '', 'total_views': '', 'total_videos': ''}
    except Exception as e:
        print(f"Error fetching data for {channel_id}: {e}")
        return {'channel_name': '', 'total_subscribers': '', 'total_views': '', 'total_videos': ''}


# Read CSV into dataframe
df = pd.read_csv("youtube_data_philippines.csv")

# Extract channel IDs and remove potential duplicates
channel_ids = df['NAME'].str.split('@').str[-1].unique()

# Initialize a list to keep track of channel stats
channel_stats = []

# Loop over channel IDs and get stats
for channel_id in channel_ids:
    stats = get_channel_stats(youtube, channel_id)
    channel_stats.append(stats)

# Convert the list of stats to a DataFrame
stats_df = pd.DataFrame(channel_stats)

# Ensure both DataFrames have the same index alignment
df.reset_index(drop=True, inplace=True)
stats_df.reset_index(drop=True, inplace=True)

# Concatenate the DataFrames horizontally
combined_df = pd.concat([df, stats_df], axis=1)

# Check if 'NAME' already contains the channel name, and fill it if missing
combined_df['NAME'] = combined_df.apply(
    lambda row: row['channel_name'] if pd.isna(row['NAME']) or row['NAME'].strip() == '' else row['NAME'], axis=1
)

# Save the merged DataFrame back into a CSV file
combined_df.to_csv('updated_youtube_data_ph.csv', index=False)

# Display first 10 rows
print(combined_df.head(10))
