from googleapiclient.discovery import build
import pandas as pd
import re

#################################################################################################
# Keys and IDs
api_key = ''

# Test Channel IDs
# 'UCY8_y20lxQhhBe8GZl5A9rw'; Musicforemptyrooms; 1.1k videos (as of 28 April)
# 'UCg4CGO3owSwvZG7oXrqk_3g'; musicforemptyrooms2; 170 videos
# 'UC2CMBX0xUGWdK9SmewrU8Bg'; vinylfrontierextra; 65 videos
# 'UC870fmEVq6abXMHHA4bQOMg'; Jar of Blind Flies - Topic; 14 videos
channel_id = ''

#################################################################################################
# Set Up

# Creates a connection with the API; specifies API version and authenticates with an API key
youtube = build('youtube', 'v3', developerKey=api_key)

#################################################################################################
# Functions

# Gets the playlist ID of a channel:
def get_playlist_id(channel_id):

    # Passes parameters into GET requests along with a channel ID
    response = youtube.channels().list(
        id = channel_id,
        part = 'contentDetails'
    ).execute()

    # Gets the Uploads playlist for the channel
    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    return playlist_id

# Gets the video ID, publish date, thumbnail url, and title of each playlist items
def get_playlist_videos(playlist_id):

    # Page token facilitates pagination through a countably infinite number of pages
    npt = None #nextPageToken

    # Creates lists for descriptive data to be written into
    global video_id, published_at, thumbnail, title
    video_id = []
    published_at = []
    thumbnail = []
    title = []

    # Loops through playlist items and writes descriptive data into the lists above
    while 1:
        response_playlist = youtube.playlistItems().list(
            playlistId = playlist_id,
            part = 'contentDetails,snippet',
            maxResults = 50,
            pageToken = npt
        ).execute()
        
        for item in response_playlist['items']:
            video_id.append(item['contentDetails']['videoId'])
            published_at.append(item['snippet']['publishedAt'])
            thumbnail.append(item['snippet']['thumbnails']['high']['url'])
            title.append(item['snippet']['title'])
            npt = response_playlist.get('nextPageToken')
        if npt is None:
            break
    return video_id, published_at, thumbnail, title

def get_video_stats(video_id):

    # Lists for concatenation into a dataframe
    global duration, view_count, like_count, comment_count
    duration = []
    view_count = []
    like_count = []
    comment_count = []

   # Passes parameters into a GET request (statistics and contentDetails)
   # Locates videos using video IDs
    for video in video_id:
        request = youtube.videos().list(
            part = 'contentDetails,statistics',
                id = video
                )
        # Gets a response from the server
        response_search = request.execute()

        # Returns the duration, viewCount, likeCount, and commentCount for each video
        for stat in response_search['items']:
            duration.append(stat['contentDetails']['duration'])
            view_count.append(stat['statistics']['viewCount'])
            like_count.append(stat['statistics']['likeCount'])
            comment_count.append(stat['statistics'].get('commentCount'))
    return duration, view_count, like_count, comment_count

# Creates a DataFrame and writes a parquet file
# Video ID, Upload Date, Thumbnail, Title, Duration, ViewCount, Like Count, Comment Count
def write_parquet():
    
    # List containing parquet headers
    # For now, the thumbnail is last because I must figure out how to keep the other values
    # from being added to the thumbnail url
    parquet_header = {
        'video_id': video_id,
        'title': title,
        'published_at': published_at,
        'duration': duration,
        'view_count': view_count,
        'like_count': like_count,
        'comment_count': comment_count,
        'thumbnail': thumbnail
    }

    youtube_data = pd.DataFrame(parquet_header)
    youtube_data.to_parquet('mfer.parquet', engine='fastparquet', index=False)
    print('mfer.parquet: done')

def extract_mfer_data():
        
    mfer = pd.read_parquet(r'')

    mfer_df = pd.DataFrame(mfer)

    mfer_df['artist'] = mfer_df['title'].str.extract(r'(.*)\s-\s')[0].str.strip()
    mfer_df['song_title'] = mfer_df['title'].str.extract(r'-(.*)\[')[0].str.strip()
    mfer_df['country'] = mfer_df['title'].str.extract(r'(\[.*\])')[0].str.strip('[]')
    mfer_df['genre'] = mfer_df['title'].str.extract(r'\](.*)\(')[0].str.strip()
    mfer_df['release_year'] = mfer_df['title'].str.extract(r'\((\d{4})\)')[0].str.strip()

    mfer_df.to_parquet('mfer_df.parquet', engine='fastparquet', index=False)
    print('mfer_df.parquet: done')

#################################################################################################
# Calls and Outputs

playlist_id = get_playlist_id(channel_id)
get_playlist_videos(playlist_id)
get_video_stats(video_id)
write_parquet()
extract_mfer_data()