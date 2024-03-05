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
    #st.write("WELCOME TO THE VISUALIZATION OF THE CHANNEL DATA")
    st.markdown("<h2 style='font-family: Calibri; font-size: 36px;'>Sections</h2>",unsafe_allow_html=True)
    st.write("About the Channel")
    Channel_Contents=["Select",'Channel','Videos','Comments']
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
         #chn_lst=channel_id.split(',')
    videos = scrapetube.get_channel(channel_id)
    video_ids=[]
    for video in videos:
      video_ids.append(video['videoId'])
      # print([video['videoId']])
    #print(video_ids)
    if content_selection=='Channel':
      st.write("Channel Details")
      def channel_info():
        if view_selection=="Dataframe_view":
          channel_df=pd.DataFrame()
          #To obtain details of a channel with the input channel_ids given
          request = youtube.channels().list(
                          part="snippet,contentDetails,statistics,status",
                          id=channel_id#"UC-K0KujStsn2MLWPc_XIB6A,UC3IZKseVpdzPSBaWxBxundA,UCrthsrCaRSnnUC42FyyoNHw"
                              )
          response = request.execute()
          #print(response)
          #execute request for channelSections to obtain the column, channel_type for the channel table
          requestcs = youtube.channelSections().list(
                              part="snippet,contentDetails",
                              channelId=channel_id
                              )
          responsecs = requestcs.execute()
          #print(responsecs)
          channel_df['channel_id']=[responsecs['items'][0]['snippet']['channelId']]
          channel_df['channel_name']=[response['items'][0]['snippet']['title']]
          channel_df['channel_type']=[responsecs['items'][0]['snippet']['type']]
          channel_df['channel_views']=[response['items'][0]['statistics']['viewCount']]
          channel_df['channel_description']=[response['items'][0]['snippet']['description']]
          channel_df['channel_status']=[response['items'][0]['status']['privacyStatus']]
          channel_df['video_count']=[response['items'][0]['statistics']['videoCount']]
          channel_df.to_csv("E:/GUVI/Guvi/channel_df.csv",index=False)
          st.dataframe(channel_df)
        if view_selection=="Load MongoDB":
          connection=MongoClient("mongodb+srv://kamatchi:bmSvoSRnwPCxlABB@atlascluster.f4fs9ek.mongodb.net/")
          db=connection['MDE86_Project1']
          col1=db['Channel_Details']
          channel_df=pd.read_csv(r"E:/GUVI/Guvi/channel_df.csv")
          print("df done")
          print(channel_df)
          mdata=[]
          for index in channel_df.index:
            row=(channel_df.loc[index]).to_dict()
            mdata.append(row)
            col1.insert_many(mdata)
            mdata
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
            print(ch_rows)
            query="insert into channel values(%s,%s,%s,%s,%s,%s,%s)"
            mycursor.executemany(query,ch_rows)
            sqlconnection.commit()
      channel_info()
    if content_selection=="Videos":
      st.write("A gist on the available Videos")
      def get_video_details():
        if view_selection=="Dataframe_view":
          video_df=pd.DataFrame()
          video_dtls=[]
          channel_id_ls=[]
          video_id=[]
          video_name=[]
          video_description=[]
          published_date=[]
          view_count=[]
          like_count=[]
          dislike_count=[]
          favourite_count=[]
          comment_count=[]
          duration=[]
          thumbnail=[]
          caption_status=[]
          playlist_id=[]
          for videoid in video_ids:
            requestvd = youtube.videos().list(
                                part="snippet,contentDetails,statistics,topicDetails",
                                id=videoid#'0vcEJ-d-7Yw',#videoids,
                                #maxResults=25
                                    )
            responsevd = requestvd.execute()
            #responsevd
            while requestvd is not None:
              responsevd = requestvd.execute()
              video_dtls += responsevd["items"]
              requestvd = youtube.videos().list_next(requestvd, responsevd)
              #for i in range(1,len(video_dtls)):
              channel_id_ls.append(channel_id)
                #responsevd['items'][0]['snippet']['channelId']])
              video_id.append(responsevd['items'][0]['id'])
              video_name.append(responsevd['items'][0]['snippet']['title'])
              video_description.append(responsevd['items'][0]['snippet']['description'])
              published_date.append(responsevd['items'][0]['snippet']['publishedAt'])
              view_count.append(responsevd['items'][0]['statistics']['viewCount'])
              #if ('likeCount' in responsevd) and ((responsevd['items'][0]['statistics']['likeCount'])!=''):
              like_count.append(responsevd['items'][0]['statistics']['likeCount'])
              #else:
              #video_df['like_count']=['0']
              dislike_count.append('0')#responsevd['items'][i]['statistics']['']]
              favourite_count.append(responsevd['items'][0]['statistics']['favoriteCount'])
              #if 'commentCount' in responsevd:
              comment_count.append(responsevd['items'][0]['statistics']['commentCount'])
              #else:
              #video_df['comment_count']=['0']
              duration.append(responsevd['items'][0]['contentDetails']['duration'])
              thumbnail.append(responsevd['items'][0]['snippet']['thumbnails']['default']['url'])
              caption_status.append(responsevd['items'][0]['contentDetails']['caption'])
              playlist_id.append(['NA'])
              print(video_id)
          video_df['channel_id']=pd.Series(channel_id_ls)
          video_df['video_id']=pd.Series(video_id)
          video_df['video_name']=pd.Series(video_name)
          video_df['video_description']=pd.Series(video_description)
          video_df['published_date']=pd.Series(published_date)
          video_df['view_count']=pd.Series(view_count)
          #if ('likeCount' in responsevd) and ((responsevd['items'][0]['statistics']['likeCount'])!=''):
          video_df['like_count']=pd.Series(like_count)
          #else:
          #video_df['like_count']=['0']
          video_df['dislike_count']=pd.Series(dislike_count)#responsevd['items'][i]['statistics']['']]
          video_df['favourite_count']=pd.Series(favourite_count)
          #if 'commentCount' in responsevd:
          video_df['comment_count']=pd.Series(comment_count)
          #else:
          #video_df['comment_count']=['0']
          video_df['duration']=pd.Series(duration)
          video_df['thumbnail']=pd.Series(thumbnail)
          video_df['caption_status']=pd.Series(caption_status)
          video_df['playlist_id']=pd.Series(playlist_id)
          video_df['caption_status']=video_df['caption_status'].astype(str)
          st.dataframe(video_df)
          video_df.to_csv("E:/GUVI/Guvi/video_df.csv",index=False)
              #print("videocsv_written")
                #print("PLcsv_written")
        if view_selection=="Load MongoDB":
          connection=MongoClient("mongodb+srv://kamatchi:bmSvoSRnwPCxlABB@atlascluster.f4fs9ek.mongodb.net/")
          db=connection['MDE86_Project1']
          col2=db['Video_Details']
          vdata=[]
          video_df=pd.read_csv(r"E:/GUVI/Guvi/video_df.csv")
          for index in video_df.index:
            row=(video_df.loc[index]).to_dict()
            vdata.append(row)
            col2.insert_many(vdata)
            vdata
        if view_selection=="MySQL DB Load":
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
              video_df=pd.read_csv(r"E:/GUVI/Guvi/video_df.csv")
              video_df.fillna('',inplace=True)
              video_df['caption_status']=video_df['caption_status'].astype(str)
              #print(video_df)
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
              print(vrows)
              print(len(new_row_1))
              query="insert into video values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
              mycursor.executemany(query,vrows)
              sqlconnection.commit()
              plrows=[]
              playlist_df=pd.read_csv(r"E:/GUVI/Guvi/playlist_df.csv")
              print(playlist_df)
              for index in playlist_df.index:
                  row=tuple(playlist_df.loc[index].values)
                  plrows.append(row)
              query="insert into playlists values(%s,%s)"
              mycursor.executemany(query,plrows)
              sqlconnection.commit()
      get_video_details()
    if content_selection=="Comments":
      st.write("Viewer Comments for the videos")
      def get_comment_details():
        if view_selection=="Dataframe_view":
          comment_df=pd.DataFrame()
          comment_id=[]
          videoId=[]
          comment_text=[]
          comment_author=[]
          comment_published_date=[]
          requestcmtt = youtube.commentThreads().list(
                            part="snippet,replies",
                            allThreadsRelatedToChannelId=channel_id
                             )
          responsecmtt = requestcmtt.execute()
          while requestcmtt is not None:
            responsecmtt = requestcmtt.execute()
            requestcmtt = youtube.playlistItems().list_next(requestcmtt, responsecmtt)
            comment_id.append(responsecmtt['items'][0]['snippet']['topLevelComment']['id'])
            videoId.append(responsecmtt['items'][0]['snippet']['videoId'])
            comment_text.append(responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['textDisplay'])
            comment_author.append(responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['authorDisplayName'])
            comment_published_date.append(responsecmtt['items'][0]['snippet']['topLevelComment']['snippet']['publishedAt'])
          comment_df['comment_id']=pd.Series(comment_id)
          comment_df['videoId']=pd.Series(videoId)
          comment_df['comment_text']=pd.Series(comment_text)
          comment_df['comment_author']=pd.Series(comment_author)
          comment_df['comment_published_date']=pd.Series(comment_published_date)
          comment_df.to_csv("E:/GUVI/Guvi/comment_df.csv",index=False)
              #print("cmtcsv_written")
          st.dataframe(comment_df)
        if view_selection=="Load MongoDB":
          connection=MongoClient("mongodb+srv://kamatchi:bmSvoSRnwPCxlABB@atlascluster.f4fs9ek.mongodb.net/")
          db=connection['MDE86_Project1']
          col3=db['Comments_Details']
          cdata=[]
          comment_df=pd.read_csv(r"E:/GUVI/Guvi/comment_df.csv",index=False)
          comment_df.fillna('',inplace=True)
          #print(comment_df)
          for index in comment_df.index:
            row=(comment_df.loc[index]).to_dict()
            cdata.append(row)
            col3.insert_many(cdata)
            cdata
        if view_selection=="MySQL DB Load":
          sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
          if sqlconnection:
            st.write("Connected to MySQL workbench") 
            mycursor=sqlconnection.cursor()
            query="""create table if not exists
                  comments(comment_id varchar(255),
                  video_id varchar(255),
                  comment_text text,
                  comment_author varchar(255),
                  comment_published_date varchar(255)
                  )"""
            mycursor.execute(query)
            cm_rows=[]
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
    if Table_View=="Yes":
      sqlconnection=mysql.connector.connect(host="localhost",user="root",password="12345",database="guvi")
      mycursor=sqlconnection.cursor()
      if sqlconnection:
        st.write("Select the Query") 
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
                    select distinct channel_id,video_name from cte1 where rn<=10
                  """
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="4.	How many comments were made on each video, and what are their corresponding video names?":
          query="""select distinct comment_count,video_name
                  from `guvi`.`video`"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="5.	Which videos have the highest number of likes, and what are their corresponding channel names?":
          query="""with cte1 as (
                  select  a.channel_name,
                          b.video_name,
                          b.like_count
                  from 
                  `guvi`.`channel` a
                  inner join 
                  `guvi`.`video` b
                  on 
                  a.channel_id=b.channel_id
                  order by 
                  b.like_count desc
                    )
                  select distinct channel_name,video_name from cte1 limit 1"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="6.	What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
          query="""select distinct video_name,like_count,dislike_count
                  from `guvi`.`video`"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="7.	What is the total number of views for each channel, and what are their corresponding channel names?":
          query="select distinct channel_name, channel_views as view_count from `guvi`.`channel`"
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
                    select distinct channel_name from cte1 where substring(published_date,1,4)='2022'"""
          mycursor.execute(query)
          result=mycursor.fetchall()
          for data in result:
            st.table(data)
        if query_selection=="9.	What is the average duration of all videos in each channel, and what are their corresponding channel names?":
          query="""with channel_data as (SELECT distinctrow `channel`.`channel_id`,
              `channel`.`channel_name`,
              `channel`.`channel_type`,
              `channel`.`channel_views`,
              `channel`.`channel_description`,
              `channel`.`channel_status`,
              `channel`.`video_count`
          FROM `guvi`.`channel`),
          video_data as
          (select 
          distinctrow `video`.`channel_id`,
              `video`.`video_id`,
              `video`.`video_name`,
              `video`.`video_description`,
              `video`.`published_date`,
              `video`.`view_count`,
              `video`.`like_count`,
              `video`.`dislike_count`,
              `video`.`favourite_count`,
              `video`.`comment_count`,
              `video`.`duration`,
              `video`.`thumbnail`,
              `video`.`caption_status`,
              `video`.`playlist_id`
          FROM `guvi`.`video`)
          select distinct a.channel_name,
          AVG(b.duration) OVER (PARTITION BY a.channel_name) as average_duration
                              from 
                              channel_data a
                              inner join 
                              video_data b
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
                    select distinct
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