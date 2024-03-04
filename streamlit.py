import streamlit as st
import pandas as pd
from pymongo import MongoClient
import mysql.connector
import googleapiclient.discovery
from streamlit_extras.colored_header import colored_header 
import scrapetube
api_service_name = "youtube"
api_version = "v3"
api_key = "AIzaSyC3W2RsrdkS3dFLhsQ9P5gAcvOghI2H49Q"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
st.title("""
         Youtube Data Visualization Application
         
         Data View On The Go
         """)
@st.cache_data
def load_data(nrows):
    data = pd.read_csv(DATA_URL, nrows=nrows)
    lowercase = lambda x: str(x).lower()
    data.rename(lowercase, axis='columns', inplace=True)
    data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
    return data
    
   # data[DATE_COLUMN] = pd.to_datetime(data[DATE_COLUMN])
def main():
  user_input=st.text_input("Enter Channel_id HERE")
  if user_input !='':
    colored_header(
        label="WELCOME TO THE VISUALIZATION OF THE CHANNEL DATA",
        description="""This application walks you through the process of scraping YouTube 
                      Channel data and visualising them as Dataframes, MySQL Datbase views 
                      and also options to load them in MongoDB Collection.""",
        color_name="violet-70",
        )
    st.markdown("<h2 style='font-family: Calibri; font-size: 36px;'>Sections</h2>",unsafe_allow_html=True)
    st.write("About the Channel")
    Channel_Contents=["Select",'Channel','Playlists','Videos','Comments']
    content_selection=st.selectbox("Choose a view",Channel_Contents)
    views=["Select","Dataframe_view","MySQL DB Load","Load MongoDB"]
    view_selection=st.selectbox("Select an option",views)
    colored_header(
        label="MySQL Database Visualization of the Channel Data",
        description="""After your data is successfully loaded in the MySQL Database, 
        there is a list of query options to visualise them and 
        you can select 'Yes' if you wish to analyse them and gain insights.""",
        color_name="violet-60",
        )
    Options=['Select','Yes','No']
    Table_View=st.selectbox("View MySQL Table contents?",Options)
    channel_id=str(user_input)
    #To collect the list of video_ids for a channel, an inbuilt library-scrapetube is used
    videos = scrapetube.get_channel(channel_id)
    video_ids=[]
    for video in videos:
      video_ids.append(video['videoId'])
    # To retreive and load channel overall information:
    if content_selection=='Channel':
      st.write("Channel Details")
      #function definition
      def channel_info():
        if view_selection=="Dataframe_view":
          channel_df=pd.DataFrame()
          #To obtain details of a channel with the input channel_id given
          #sample id for check-UC2iP6-PJXkrQ03C8WBC-NXQ
          #YouTube API requests for channel info
          #request for obtaining the columns channel_name,channel_views,channel_description
          #,channel_status and video_count
          request = youtube.channels().list(
                          part="snippet,contentDetails,statistics,status",
                          id=channel_id
                              )
          response = request.execute()
          
          #execute request for channelSections to obtain the column, channel_type for the channel table
          requestcs = youtube.channelSections().list(
                              part="snippet,contentDetails",
                              channelId=channel_id
                              )
          responsecs = requestcs.execute()
          #writing the data to a pandas dataframe
          channel_df['channel_id']=[responsecs['items'][0]['snippet']['channelId']]
          channel_df['channel_name']=[response['items'][0]['snippet']['title']]
          channel_df['channel_type']=[responsecs['items'][0]['snippet']['type']]
          channel_df['channel_views']=[response['items'][0]['statistics']['viewCount']]
          channel_df['channel_description']=[response['items'][0]['snippet']['description']]
          channel_df['channel_status']=[response['items'][0]['status']['privacyStatus']]
          channel_df['video_count']=[response['items'][0]['statistics']['videoCount']]
          #writing the dataframe contents to a csv file to be used in the next sections of the code
          channel_df.to_csv("E:/GUVI/Guvi/channel_df.csv",index=False)
          st.dataframe(channel_df)
          #To load data in Mongo DB
        if view_selection=="Load MongoDB":
          #establish the mongoDB connection
          connection=MongoClient("mongodb+srv://kamatchi:SWur7NoBGijAxyDv@atlascluster.f4fs9ek.mongodb.net/")
          db=connection['MDE86_Project1']
          col1=db['Channel_Details']
          #create a dataframe by reading the csv file created previously
          channel_df=pd.read_csv(r"E:/GUVI/Guvi/channel_df.csv")
          mdata=[]
          for index in channel_df.index:
            row=(channel_df.loc[index]).to_dict()
            mdata.append(row)
            col1.insert_many(mdata)
        #To load data in MySQL
        if view_selection=="MySQL DB Load":
          sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
          if sqlconnection:
            st.write("Connected to MySQL workbench") 
            mycursor=sqlconnection.cursor()
            query="""create table if not exists
            channel(channel_id varchar(255),
            channel_name varchar(255),
            channel_type varchar(255),
            channel_views bigint,
            channel_description text,
            channel_status varchar(255),
            video_count bigint)"""
            mycursor.execute(query)
            ch_rows=[]
            channel_df=pd.read_csv(r"E:/GUVI/Guvi/channel_df.csv")
            channel_df
            for index in channel_df.index:
              row=tuple(channel_df.loc[index].values)
              row=tuple(str(d) for d in row)
              new_row=tuple(int(x) if i in (3,6) else x for i,x in enumerate(row))
              ch_rows.append(new_row)
            query="insert into channel values(%s,%s,%s,%s,%s,%s,%s)"
            mycursor.executemany(query,ch_rows)
            sqlconnection.commit()
      channel_info()
    if content_selection=="Playlists":
      if view_selection=="Dataframe_view":
          requestpll = youtube.playlists().list(
                          part="snippet,contentDetails",
                        channelId=channel_id#"UC2iP6-PJXkrQ03C8WBC-NXQ",
                        # maxResults=25
                              )
          responsepll = requestpll.execute()
          while requestpll is not None:
              responsepll = requestpll.execute()
              requestpll = youtube.playlistItems().list_next(requestpll, responsepll)
              #playlist_ids extracted from the API response
              #writing the data to a pandas dataframe
              playlist_df=pd.DataFrame()
    # To retreive and load video overall information:
    if content_selection=="Videos":
      st.write("A gist on the available Videos")
      def get_video_details():
        if view_selection=="Dataframe_view":
          for videoid in video_ids:
            requestvd = youtube.videos().list(
                                part="snippet,contentDetails,statistics,topicDetails",
                                id=videoid#'0vcEJ-d-7Yw'
                                  )
            responsevd = requestvd.execute()
            while requestvd is not None:
              video_dtls=[]
              responsevd = requestvd.execute()
              video_dtls += responsevd["items"]
              requestvd = youtube.videos().list_next(requestvd, responsevd)
            #writing the data to a pandas dataframe
              video_df=pd.DataFrame()
              video_df['channel_id']=[channel_id]
              video_df['video_id']=[responsevd['items'][0]['id']]
              video_df['video_name']=[responsevd['items'][0]['snippet']['title']]
              video_df['video_description']=[responsevd['items'][0]['snippet']['description']]
              video_df['published_date']=[responsevd['items'][0]['snippet']['publishedAt']]
              video_df['view_count']=[responsevd['items'][0]['statistics']['viewCount']]
              #if ('likeCount' in responsevd) and ((responsevd['items'][0]['statistics']['likeCount'])!=''):
              video_df['like_count']=[responsevd['items'][0]['statistics']['likeCount']]
              #else:
              #video_df['like_count']=['0']
              video_df['dislike_count']=['0']#responsevd['items'][i]['statistics']['']]
              video_df['favourite_count']=[responsevd['items'][0]['statistics']['favoriteCount']]
              #if 'commentCount' in responsevd:
              video_df['comment_count']=[responsevd['items'][0]['statistics']['commentCount']]
              #else:
              #video_df['comment_count']=['0']
              video_df['duration']=[responsevd['items'][0]['contentDetails']['duration']]
              video_df['thumbnail']=[responsevd['items'][0]['snippet']['thumbnails']['default']['url']]
              video_df['caption_status']=[responsevd['items'][0]['contentDetails']['caption']]
              video_df['playlist_id']=playlist_df['playlist_id']
                #writing the dataframe contents to a csv file to be used in the next sections of the code        
              video_df.to_csv("E:/GUVI/Guvi/video_df.csv",index=False)
              st.dataframe(video_df)
        if view_selection=="Load MongoDB":
          connection=MongoClient("mongodb+srv://kamatchi:SWur7NoBGijAxyDv@atlascluster.f4fs9ek.mongodb.net/")
          db=connection['MDE86_Project1']
          col2=db['Video_Details']
          vdata=[]
          #create a dataframe by reading the csv file created previously
          video_df=pd.read_csv(r"E:/GUVI/Guvi/video_df.csv")
          playlist_df=pd.read_csv(r"E:/GUVI/Guvi/playlist_df.csv")
          video_df['playlist_id']=playlist_df['playlist_id']
          for index in video_df.index:
            row=(video_df.loc[index]).to_dict()
            vdata.append(row)
            col2.insert_many(vdata)
            vdata
        if view_selection=="MySQL DB Load":
            #establishing connection to MySQL database
            sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
            if sqlconnection:
              st.write("Connected to MySQL workbench") 
              mycursor=sqlconnection.cursor()
              query="""create table if not exists
                  video(
                  channel_id varchar(255),
                  video_id varchar(255),
                  video_name varchar(255),
                  video_description text,
                  published_date varchar(255),
                  view_count bigint,
                  like_count bigint,
                  dislike_count bigint,
                  favourite_count bigint,
                  comment_count bigint,
                  duration bigint,
                  thumbnail varchar(255),
                  caption_status varchar(255),
                  playlist_id varchar(255))"""
              mycursor.execute(query)
              query1="""create table if not exists
                  playllists(
                  playlist_id varchar(255),
                  channel_id varchar(255))"""
              mycursor.execute(query1)
              import re
              vrows=[]
              #create a dataframe by reading the csv file created previously
              video_df=pd.read_csv(r"E:/GUVI/Guvi/video_df.csv")
              playlist_df=pd.read_csv(r"E:/GUVI/Guvi/playlist_df.csv")
              video_df['playlist_id']=playlist_df['playlist_id']
              print(video_df)
              for index in video_df.index:
                  row=tuple(video_df.loc[index].values)
                  #print(row)
                  my_list=list(row)
                  numbers = re.findall(r'[0-9]+', my_list[10])
                  alphabets = re.findall(r'[a-zA-Z]+', my_list[10])
                  print(numbers)
                  print(alphabets)
                  if (len(numbers)<=1):
                      if alphabets[1]=='S':
                          duration=int(numbers[0])
                      elif alphabets[1]=='M':
                          duration=(int(numbers[0]))*60
                      elif alphabets[1]=='H':
                          duration=(int(numbers[0]))*60*60
                  elif len(numbers)==2:
                      if alphabets[1]=='M' and alphabets[2]=='S':
                          duration=((int(numbers[0]))*60)+int(numbers[1])
                      elif alphabets[1]=='H' and alphabets[2]=='M':
                          duration=((int(numbers[0]))*60*60)+((int(numbers[1]))*60)
                      elif alphabets[1]=='H' and alphabets[2]=='S':
                          duration=((int(numbers[0]))*60*60)+int(numbers[1])
                  elif len(numbers)==3:
                      duration=((int(numbers[0]))*60*60)+((int(numbers[1]))*60)+(int(numbers[2]))
                  my_list[10]=duration
                  
                  formatted_time_tuple=tuple(my_list)
                  new_row=tuple(int(x) if i in (5,6,7,8,9) else x for i,x in enumerate(row))
                  new_row_1=new_row[0:10]+formatted_time_tuple[10:11]+new_row[11:14]
                  vrows.append(new_row_1)
              query="insert into video values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
              mycursor.executemany(query,vrows)
              sqlconnection.commit()
              plrows=[]
              playlist_df=pd.read_csv(r"E:/GUVI/Guvi/playlist_df.csv")
              for index in playlist_df.index:
                  row=tuple(playlist_df.loc[index].values)
                  plrows.append(row)
              query="insert into playlists values(%s,%s)"
              mycursor.executemany(query,plrows)
              sqlconnection.commit()
      get_video_details()
    # To retreive and load comments on videos information:
    if content_selection=="Comments":
      st.write("Viewer Comments for the videos")
      def get_comment_details():
        if view_selection=="Dataframe_view":
          for videoid in video_ids:
              requestcmtt = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=videoid
                )
              
              while requestcmtt is not None:
                  responsecmtt = requestcmtt.execute()
                  requestcmtt = youtube.commentThreads().list_next(requestcmtt, responsecmtt)
                  #writing the data to a pandas dataframe
                  comment_df=pd.DataFrame()
                  comment_df['comment_id']=[responsecmtt['items'][0]['snippet']['topLevelComment']['id']]
                  comment_df['videoId']=[responsecmtt['items'][0]['snippet']['videoId']]
                  comment_df['comment_text']=[responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay']]
                  comment_df['comment_author']=[responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName']]
                  comment_df['comment_published_date']=[responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt']]
                  #writing the dataframe contents to a csv file to be used in the next sections of the code
                  comment_df.to_csv("E:/GUVI/Guvi/comment_df.csv",index=False)
                  st.dataframe(comment_df)                           
          if view_selection=="Load MongoDB":
            connection=MongoClient("mongodb+srv://kamatchi:SWur7NoBGijAxyDv@atlascluster.f4fs9ek.mongodb.net/")
            db=connection['MDE86_Project1']
            col3=db['Comments_Details']
            cdata=[]
            #create a dataframe by reading the csv file created previously
            comment_df=pd.read_csv(r"E:/GUVI/Guvi/comment_df.csv")
            #print(comment_df)
            for index in comment_df.index:
              row=(comment_df.loc[index]).to_dict()
              cdata.append(row)
              col3.insert_many(cdata)
              cdata
          if view_selection=="MySQL DB Load":
            sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
            cm_rows=[]
            #create a dataframe by reading the csv file created previously
            comment_df=pd.read_csv(r"E:/GUVI/Guvi/comment_df.csv")
            for index in comment_df.index:
              row=tuple(comment_df.loc[index].values)
              row=tuple(str(d) for d in row)
              cm_rows.append(row)
              #print(cm_rows)
              query="insert into comments values(%s,%s,%s,%s,%s)"
              mycursor.executemany(query,cm_rows)
              sqlconnection.commit()    
      get_comment_details()
      #to view the loaded MySQL tables- options to select from the list of user-friendly queries
    if Table_View=="Yes":
      #connecting the database
      sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
      #connection check
      if sqlconnection:
        st.write("Connected to MySQL workbench") 
        mycursor=sqlconnection.cursor()
        #accessing the info with the queries
        User_queries=['Select',
                '1.	What are the names of all the videos and their corresponding channels?',
                '2.	Which channels have the most number of videos, and how many videos do they have?',
                '3.	What are the top 10 most viewed videos and their respective channels?',
                '4.	How many comments were made on each video, and what are their corresponding video names?',
                '5.	Which videos have the highest number of likes, and what are their corresponding channel names?',
                '6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                '7.	What is the total number of views for each channel, and what are their corresponding channel names?',
                '8.	What are the names of all the channels that have published videos in the year 2022?',
                '9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                '10. Which videos have the highest number of comments, and what are their corresponding channel names?',
                '11. View table Channel_Details',
                '12. View table Videos',
                '13. View table Viewers_Comments']
        query_selection=st.selectbox("Choose a query",User_queries)
        if query_selection=='1.	What are the names of all the videos and their corresponding channels?':
          query="""select distinct 
          a.channel_id,
          a.video_name
          from `guvi`.`video` a
          """
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=='2.	Which channels have the most number of videos, and how many videos do they have?':
          query="""with cte1 as 
                  (
                  select ROW_NUMBER() over (PARTITION BY channel_id ORDER BY video_count DESC) as rn,
                  channel_name,video_count
                  from `guvi`.`channel`
                  )
                  select distinct channel_name, video_count
                      from 
                      cte1 
                      where rn=1"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=='3.	What are the top 10 most viewed videos and their respective channels?':
          query="""with cte1 as 
                        (
                          select 
                          a.channel_id,
                          b.video_name,
                          b.view_count,
                          ROW_NUMBER() OVER (PARTITION BY a.channel_id ORDER BY b.view_count DESC) as rn
                          from `guvi`.`channel` a
                          inner join `guvi`.`video` b
                          on a.channel_id=b.channel_id
                          )
                    select channel_id,video_name from cte1 where rn<=10
                  """
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="4.	How many comments were made on each video, and what are their corresponding video names?":
          query="""select comment_count,video_name
                  from `guvi`.`video`"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
          query="""with cte1 as (
                  select  a.channel_name,
                          c.video_name,
                          c.like_count
                  from 
                  `guvi`.`channel` a
                  inner join 
                  `guvi`.`video` b
                  on 
                  a.channel_id=b.channel_id
                  order by 
                  b.like_count desc
                    )
                  select channel_name,video_name from cte1 limit 1"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
          query="""select video_name,like_count,dislike_count
                  from `guvi`.`video`"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="7.	What is the total number of views for each channel, and what are their corresponding channel names?":
          query="select channel_name, channel_views as view_count from `guvi`.`channel`"
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="8.	What are the names of all the channels that have published videos in the year 2022?":
          query="""with cte1 as (
                      select 
                      a.channel_name,
                      b.published_date
                    from 
                    `guvi`.`channel` a
                    inner join
                    `guvi`.`video` b
                    on 
                    a.channel_id=b.channel_id
                    order by b.like_count desc
                    )
                    select channel_name from cte1 where YEAR(published_date)=2022"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
          query="""select 
                    a.channel_name,
                    AVG(c.duration) OVER (PARTITION BY a.channel_name) as average_duration
                    from 
                    `guvi`.`channel` a
                    inner join 
                    `guvi`.`video` b
                    on 
                    a.channel_id=b.channel_id"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
          query="""with cte1 as 
                    (
                      select a.channel_id,
                        b.video_name,
                        b.comment_count,
                        ROW_NUMBER() OVER (PARTITION BY a.channel_id ORDER BY b.comment_count DESC) as rn
                    from `guvi`.`channel` a
                    inner join `guvi`.`video` b
                    on a.channel_id=b.channel_id
                    )
                    select 
                    channel_id,video_name,comment_count 
                    from cte1 where rn=1"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="11. View table Channel_Details":
          query="Select * from `guvi`.`channel`"
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="12. View table Videos":
          query="select * from `guvi`.`video`"
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="13. View table Viewers_Comments":
          query="select * from `guvi`.`comments`"
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
main()