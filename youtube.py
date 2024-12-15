from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

def api_service():
    api_id = "AIzaSyASD7DvR6KOC06xKxOeWkqNy4dHyQtqIW8"

    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name,api_version,developerKey=api_id)

    return youtube

youtube = api_service()


# channel_info

def channel_info(Channel_Id):
    request = youtube.channels().list(
                    part = "snippet,contentDetails,statistics",
                    id = Channel_Id
                    )

    response = request.execute()                

    for i in response['items']:
        data = dict(Channel_Name =i['snippet']['title'],
                    Channel_Id = i['id'],
                    Subscribers = i['statistics']['subscriberCount'],
                    Views = i['statistics']['viewCount'],
                    Total_Videos = i['statistics']['videoCount'],
                    Channel_Description = i['snippet']['description'],
                    Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads']
                    )
    return data


# get video ids

def video_ids(Channel_Id):

    video_id = []
    request = youtube.channels().list(
                        part = "contentDetails",
                        id = Channel_Id
    )
    response = request.execute()

    Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    nextPageToken = None

    while True:
        response_1 = youtube.playlistItems().list(
                                            part = 'snippet',
                                            playlistId = Playlist_Id,
                                            maxResults = 50,
                                            pageToken = nextPageToken
        ).execute()

        for i in range (len(response_1['items'])):
            video_id.append(response_1['items'][i]['snippet']['resourceId']['videoId'])
        nextPageToken = response_1.get('nextPageToken')

        if nextPageToken is None:
            break
    return video_id


# get video info

def video_info(video_ids):
        Video_Data = []
        for vid_id in video_ids:
                request = youtube.videos().list(
                part = "snippet,contentDetails,statistics",
                id = vid_id
        )

                response = request.execute()

                for item in response['items']:
                        data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Description = item['snippet']['description'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        published_date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics']['viewCount'],
                        Likes = item.get('likeCount'),
                        Comments = item['statistics']['commentCount'],
                        Favorite = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Captions = item['contentDetails']['caption']
                        )
                        Video_Data.append(data)

        return Video_Data


# get comment info
def comment_details(vid_ids):
        Comment_info = []
        try:
                for video_ids in vid_ids:
                        request = youtube.commentThreads().list(
                                part = "snippet",
                                videoId = video_ids,
                                maxResults = 50
                        )
                        response = request.execute()

                        for item in response['items']:
                                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                                                Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                                                Comment = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                                Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                                Comment_Published_time = item['snippet']['topLevelComment']['snippet']['publishedAt']
                                                )

                                Comment_info.append(data)

        except:
                pass

        return Comment_info

#playlist details

def Playlist_details(Channel_Id):
    next_page_token = None
    All_Data = []
    while True:
        request = youtube.playlists().list(
                    part = "snippet,contentDetails",
                    channelId = Channel_Id,
                    maxResults = 50,
                    pageToken = next_page_token
        )

        response = request.execute()

        for item in response['items']:
            data = dict(Playlist_Id = item['id'],
                        Title = item['snippet']['title'],
                        Channel_Id = item['snippet']['channelId'],
                        Channel_Name = item['snippet']['channelTitle'],
                        Publishing_time = item['snippet']['publishedAt'],
                        Video_Count = item['contentDetails']['itemCount']

                        )
            
            All_Data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break
    
    return All_Data
    

# connecting with mongodb

Client = pymongo.MongoClient('mongodb://localhost:27017')
Db = Client['Youtube_Data'] 

def Channel_Details(Channel_Id):
    Ch_details = channel_info(Channel_Id)
    Vid_ids = video_ids(Channel_Id)
    Vid_details = video_info(Vid_ids)
    Com_details = comment_details(Vid_ids)
    Pl_details = Playlist_details(Channel_Id)


    coll1 = Db['Channel_Details']

    coll1.insert_one({"Channel_Information":Ch_details,
                      "Playlist_Information":Pl_details,
                      "Video_Information":Vid_details,
                      "Comment_Information":Com_details})
    
    return "Data Upload Completed Successfully"


# Table creation using SQL

def Channels_Table():

    db = psycopg2.connect(host ="localhost",
                        user ="postgres",
                        password ="1177",
                        database ="Youtube_Data",
                        port ="5432")

    cursor = db.cursor()


    drop = '''DROP TABLE IF EXISTS Channels'''
    cursor.execute(drop)
    db.commit()

    try:
        query = '''CREATE TABLE IF NOT EXISTS Channels(Channel_Name VARCHAR (100),
                                                    Channel_Id VARCHAR(80) PRIMARY KEY,
                                                        Subscribers BIGINT,
                                                        Views BIGINT,
                                                        Total_Videos INT,
                                                        Channel_Description TEXT,
                                                        Playlist_Id VARCHAR(80)
                                                        )'''
        
        cursor.execute(query)
        db.commit()
    except:
        print("Channel Table Already Created")



    Channel_da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for channel_data in coll1.find({},{"_id":0,"Channel_Information":1,}):
        Channel_da.append(channel_data['Channel_Information'])
    df = pd.DataFrame(Channel_da)


    for index,row in df.iterrows():
        insert = ''' INSERT INTO Channels(Channel_Name,
                                        Channel_Id,
                                        Subscribers,
                                        Views,
                                        Total_Videos,
                                        Channel_Description,
                                        Playlist_Id) 
                                        Values (%s,%s,%s,%s,%s,%s,%s)''' 
        
        values = (row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        
        try:
            cursor.execute(insert,values)
            db.commit()

        except:
            print("Channel values are already inserted")

                                      
def Playlist_Table():

    db = psycopg2.connect(host ="localhost",
                    user ="postgres",
                    password ="1177",
                    database ="Youtube_Data",
                    port ="5432")

    cursor = db.cursor()


    drop = '''DROP TABLE IF EXISTS Playlist'''
    cursor.execute(drop)
    db.commit()

    query = '''CREATE TABLE IF NOT EXISTS Playlist(Playlist_Id VARCHAR (100) PRIMARY KEY,
                                                    Title VARCHAR (100),
                                                    Channel_Id VARCHAR (100),
                                                    Channel_Name VARCHAR (100),
                                                    Publishing_time TIMESTAMP,
                                                    Video_Count INT
                                                    )'''

    cursor.execute(query)
    db.commit()

    


    Playlist_Da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for playlist_data in coll1.find({},{"_id":0,"Playlist_Information":1,}):
        for i in range(len(playlist_data['Playlist_Information'])):
            Playlist_Da.append(playlist_data['Playlist_Information'][i])
    df_P = pd.DataFrame(Playlist_Da)


    for index,row in df_P.iterrows():
        insert = ''' INSERT INTO Playlist(Playlist_Id,
                                        Title,
                                        Channel_Id,
                                        Channel_Name,
                                        Publishing_time,
                                        Video_Count
                                        ) 
                                        Values (%s,%s,%s,%s,%s,%s)''' 
        
        values = (row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['Publishing_time'],
                row['Video_Count'])
        
        
        cursor.execute(insert,values)
        db.commit()

def Videos_Table():


        db = psycopg2.connect(host ="localhost",
                        user ="postgres",
                        password ="1177",
                        database ="Youtube_Data",
                        port ="5432")

        cursor = db.cursor()


        drop = '''DROP TABLE IF EXISTS Videos'''
        cursor.execute(drop)
        db.commit()

        query = '''CREATE TABLE IF NOT EXISTS Videos (Channel_Name VARCHAR (100),
                                                        Channel_Id VARCHAR (100),
                                                        Video_Id VARCHAR (100)PRIMARY KEY,
                                                        Description TEXT,
                                                        Title VARCHAR (100),
                                                        Tags TEXT,
                                                        Thumbnail VARCHAR (300),
                                                        published_date TIMESTAMP,
                                                        Duration INTERVAL,
                                                        Views BIGINT,
                                                        Likes BIGINT,
                                                        Comments BIGINT,
                                                        Favorite BIGINT,
                                                        Definition VARCHAR (10),
                                                        Captions VARCHAR (10)
                                                        )'''

        cursor.execute(query)
        db.commit()


        Videos_Da = []
        Db = Client['Youtube_Data']
        coll1 = Db['Channel_Details']
        for video_Data in coll1.find({},{"_id":0,"Video_Information":1,}):
                for i in range(len(video_Data['Video_Information'])):
                        Videos_Da.append(video_Data['Video_Information'][i])
        df_V = pd.DataFrame(Videos_Da)


        for index,row in df_V.iterrows():
                insert = ''' INSERT INTO Videos (Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Description,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                published_date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite,
                                                Definition,
                                                Captions
                                                ) 
                                                Values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''' 
                
                values = (row['Channel_Name'],
                        row['Channel_Id'],
                        row['Video_Id'],
                        row['Description'],
                        row['Title'],
                        row['Tags'],
                        row['Thumbnail'],
                        row['published_date'],
                        row['Duration'],
                        row['Views'],
                        row['Likes'],
                        row['Comments'],
                        row['Favorite'],
                        row['Definition'],
                        row['Captions'])
                
                
                cursor.execute(insert,values)
                db.commit()



def Comments_Table():

        db = psycopg2.connect(host ="localhost",
                        user ="postgres",
                        password ="1177",
                        database ="Youtube_Data",
                        port ="5432")

        cursor = db.cursor()


        drop = '''DROP TABLE IF EXISTS Comments'''
        cursor.execute(drop)
        db.commit()

        query = '''CREATE TABLE IF NOT EXISTS Comments (Comment_Id VARCHAR (100)PRIMARY KEY,
                                                        Video_Id VARCHAR (100),
                                                        Comment TEXT,
                                                        Comment_Author VARCHAR (200),
                                                        Comment_Published_time TIMESTAMP
                                                        
                                                        )'''

        cursor.execute(query)
        db.commit()


        Comments_Da = []
        Db = Client['Youtube_Data']
        coll1 = Db['Channel_Details']
        for Comment_Data in coll1.find({},{"_id":0,"Comment_Information":1,}):
                for i in range(len(Comment_Data['Comment_Information'])):
                        Comments_Da.append(Comment_Data['Comment_Information'][i])
        df_C = pd.DataFrame(Comments_Da)

        db = psycopg2.connect(host ="localhost",
                        user ="postgres",
                        password ="1177",
                        database ="Youtube_Data",
                        port ="5432")

        cursor = db.cursor()




        for index,row in df_C.iterrows():
                        insert = ''' INSERT INTO Comments (Comment_Id,
                                                        Video_Id,
                                                        Comment,
                                                        Comment_Author,
                                                        Comment_Published_time
                                                        ) 
                                                        Values (%s,%s,%s,%s,%s)''' 
                        
                        values = (row['Comment_Id'],
                                row['Video_Id'],
                                row['Comment'],
                                row['Comment_Author'],
                                row['Comment_Published_time']
                                )
                        
                        
                        cursor.execute(insert,values)
                        db.commit()


def tables():
    Channels_Table()
    Playlist_Table()
    Videos_Table()
    Comments_Table()


    return "Tables Created Successfully"



def streamlit_channel():
    Channel_da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for channel_data in coll1.find({},{"_id":0,"Channel_Information":1,}):
        Channel_da.append(channel_data['Channel_Information'])
    df = st.dataframe(Channel_da)

    return df

def streamlit_playlist():
    Playlist_Da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for playlist_data in coll1.find({},{"_id":0,"Playlist_Information":1,}):
        for i in range(len(playlist_data['Playlist_Information'])):
            Playlist_Da.append(playlist_data['Playlist_Information'][i])
    df_P = st.dataframe(Playlist_Da)

    return df_P

def streamlit_videos():
    Videos_Da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for video_Data in coll1.find({},{"_id":0,"Video_Information":1,}):
        for i in range(len(video_Data['Video_Information'])):
                Videos_Da.append(video_Data['Video_Information'][i])
    df_V = st.dataframe(Videos_Da)

    return df_V

def streamlit_comments():
    Comments_Da = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for Comment_Data in coll1.find({},{"_id":0,"Comment_Information":1,}):
        for i in range(len(Comment_Data['Comment_Information'])):
                Comments_Da.append(Comment_Data['Comment_Information'][i])
    df_C = st.dataframe(Comments_Da)


    return df_C


# streamlit part

st.title('YOUTUBE DATA HARVESTING AND WAREHOUSING')

channel_id = st.text_input("Enter The Channel Id")

if st.button("Collect and Store Data"):

    channels_id = []
    Db = Client['Youtube_Data']
    coll1 = Db['Channel_Details']
    for channel_data in coll1.find({},{'_id':0,'Channel_Information':1}):
        channels_id.append(channel_data['Channel_Information']["Channel_Id"])

    if channel_id in channels_id:
        st.success('Channel Info Already Exists')

    else:
        insert = Channel_Details(channel_id)
        st.success(insert)

if st.button("SQL Database"):
    Tables = tables()
    st.success(Tables)

table = st.radio("Select To View",("Channels","Playlist","Videos","Comments"))

if table == "Channels":
    streamlit_channel()

if table == "Playlist":
    streamlit_playlist()

if table == "Videos":
    streamlit_videos()

if table == "Comments":
    streamlit_comments()


#SQL

db = psycopg2.connect(host ="localhost",
                    user ="postgres",
                    password ="1177",
                    database ="Youtube_Data",
                    port ="5432")

cursor = db.cursor()

questions = st.selectbox("Select Your Question",("1. All the videos and the channel name",
                                                 "2. Channels with most number of videos",
                                                 "3. Top 10 viewed videos",
                                                 "4. Comments in each video",
                                                 "5. Videos with more number of likes",
                                                 "6. Likes of all the videos",
                                                 "7. Views of each channel",
                                                 "8. Videos published in the year of 2022",
                                                 "9. Average duration of all videos in each channel",
                                                 "10. Videos with more number of comments"))

if questions == "1. All the videos and the channel name":

    q1 = ''' SELECT title,channel_name FROM videos'''
    cursor.execute(q1)
    db.commit()

    qt1 = cursor.fetchall()
    df = pd.DataFrame(qt1,columns=['Video Title','Channel Name'])
    st.write(df)


elif questions == "2. Channels with most number of videos":

    q2 = ''' SELECT channel_name,total_videos AS total FROM channels
                ORDER BY total DESC'''
    cursor.execute(q2)
    db.commit()

    qt2 = cursor.fetchall()
    df2 = pd.DataFrame(qt2,columns=['Channel Name','Total Number of Videos'])
    st.write(df2)

    
elif questions == "3. Top 10 viewed videos":

    q3 = ''' SELECT channel_name,title,views FROM videos
                WHERE views IS NOT NULL
                ORDER BY views DESC'''
    cursor.execute(q3)
    db.commit()

    qt3 = cursor.fetchall()
    df3 = pd.DataFrame(qt3,columns=['Channel Name','Video Title','Number of Views'])
    st.write(df3)

    
elif questions == "4. Comments in each video":

    q4 = ''' SELECT channel_name,title,comments FROM videos
                WHERE comments IS NOT NULL'''
    cursor.execute(q4)
    db.commit()

    qt4 = cursor.fetchall()
    df4 = pd.DataFrame(qt4,columns=['Channel Name','Video Title','Number of Comments'])
    st.write(df4)


elif questions == "5. Videos with more number of likes":

    q5 = ''' SELECT channel_name,title,likes FROM videos
                WHERE likes IS NOT NULL
                ORDER BY likes DESC'''
    cursor.execute(q5)
    db.commit()

    qt5 = cursor.fetchall()
    df5 = pd.DataFrame(qt5,columns=['Channel Name','Video Title','Number of Likes'])
    st.write(df5)

elif questions == "6. Likes of all the videos":

    q6 = ''' SELECT title,likes FROM videos
                WHERE likes IS NOT NULL'''
    cursor.execute(q6)
    db.commit()

    qt6 = cursor.fetchall()
    df6 = pd.DataFrame(qt6,columns=['Video Title','Total Number of Likes'])
    st.write(df6)


elif questions == "7. Views of each channel":

    q7 = ''' SELECT channel_name, SUM(views) AS total_views FROM videos
                GROUP BY channel_name
                ORDER BY total_views DESC'''
    cursor.execute(q7)
    db.commit()

    qt7 = cursor.fetchall()
    df7 = pd.DataFrame(qt7,columns=['Channel Name','Total Number of Views'])
    st.write(df7)


elif questions == "8. Videos published in the year of 2022":

    q8 = ''' SELECT channel_name, title, published_date FROM videos
                WHERE EXTRACT(YEAR FROM published_date) = 2022
                ORDER BY published_date'''
    cursor.execute(q8)
    db.commit()

    qt8 = cursor.fetchall()
    df8 = pd.DataFrame(qt8,columns=['Channel Name','Title','Published_Date'])
    st.write(df8)

elif questions == "9. Average duration of all videos in each channel":

    q9 = ''' SELECT channel_name, AVG(duration) AS Average FROM videos
                GROUP BY channel_name
                ORDER BY Average DESC'''
    cursor.execute(q9)
    db.commit()

    qt9 = cursor.fetchall()
    df9 = pd.DataFrame(qt9,columns=['Channel Name','Average Video Duration'])

    st9 = []
    for index,row in df9.iterrows():
        channel_title = row['Channel Name']
        average_duration = row['Average Video Duration']
        average_duration_str = str(average_duration)
        st9.append(dict(Channel_Name = channel_title,Average_duration = average_duration_str))

    df9_1 = pd.DataFrame(st9)
    st.write(df9_1)

elif questions == "10. Videos with more number of comments":

    q10 = ''' SELECT channel_name, title, comments FROM videos
                WHERE comments IS NOT NULL
                ORDER BY comments DESC'''
    cursor.execute(q10)
    db.commit()

    qt10 = cursor.fetchall()
    df10 = pd.DataFrame(qt10,columns=['Channel Name','Title','Total Number of Comments'])
    st.write(df10)

    
