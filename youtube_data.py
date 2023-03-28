from googleapiclient.discovery import build
import pandas as pd
import pprint

#################################################################################################
# Keys and IDs
api_key='AIzaSyDn9VFpVNNUW9SNOS4FSBNe1tRI7nkzBcM'
channel='UCY8_y20lxQhhBe8GZl5A9rw' # Music for empty rooms

#################################################################################################
# Set Up

# Creates a connection with the API; specifies API version and authenticates with an API key
youtube=build('youtube','v3',developerKey=api_key)

#################################################################################################
# Functions

# Gets descriptive statistics for YouTube videos using the search method
# Returns a tuple containing lists (titles, publish dates, and IDs)
def get_video():

    # Lists for concatenation into a dataframe
    global videoId
    title=[]
    publishDate=[]
    videoId=[]

   # Passes parameters into a GET request
   # Requests YouTube videos ordered reverse chronologically by upload date
    request=youtube.search().list(
        part="snippet",
            channelId=channel,
            maxResults=2,
            order="date",
            type="video"
         )
    # Gets a response from the server
    response_search=request.execute()

    # Returns the title, publish date, and ID for each video
    for item in response_search['items']:
        videoId.append(item['id']['videoId'])
        title.append(item['snippet']['title'])
        publishDate.append(item['snippet']['publishedAt'])
    return videoId,title,publishDate


# Gets numerical statistics for videos 
# Returns these values as a tuple containing lists (duration, viewCount, likeCount, commentCount)
def get_video_stats(videoId):
    
    # Lists for concatenation into a dataframe
    duration=[]
    viewCount=[]
    likeCount=[]
    commentCount=[]

   # Passes parameters into a GET request (statistics and contentDetails)
   # Locates videos using video IDs
    request=youtube.videos().list(
        part='contentDetails,statistics',
            id=videoId #'vCdGmJiFZX8'  
         )
    # Gets a response from the server
    response_stats=request.execute()

    # Returns the duration, viewCount, likeCount, and commentCount for each video
    for stat in response_stats['items']:
        duration.append(stat['contentDetails']['duration'])
        viewCount.append(stat['statistics']['viewCount'])
        likeCount.append(stat['statistics']['likeCount'])
        commentCount.append(stat['statistics']['commentCount'])
    return duration,viewCount,likeCount,commentCount
        


#################################################################################################
desc=get_video()
stats=get_video_stats(videoId)
# Prints videoId, title, publish date, duration, viewCount, likeCount, and commentCount
pprint.pprint(desc+stats)