import lib.apiyt as apyt
from call_api import get_links
import datetime
from logger import Colorlog
class Link:
    def __init__(self):
        pass
    def get_link_search(self,queue_link,keyword,list_searched):
        try:
            json_link= get_links(object_id='crawled',table_name='youtube_video')
            list_searched=json_link['links']
        except:
            list_searched=[]
        print(f"{Colorlog.yellow_color}{datetime.datetime.now()} ---------->>>>>  GET LINK VIDEO FROM {keyword}{Colorlog.reset_color}")
        arr_videos=[]
        videos= apyt.get_search(f'{keyword}')
        for video in videos:
            if video['videoId'] in list_searched:
                break
            arr_videos.append(video['videoId'])
        for v in reversed(arr_videos):
            print(f'------> PUT https://www.youtube.com/watch?v={v} TO QUEUE ')
            queue_link.put(f"https://www.youtube.com/watch?v={v}")

    def get_link_channel(self,queue_link,channel_url,max):
        name_channel=str(channel_url).split('/@')[-1]
        x=0
        y=0
        print(f"{Colorlog.yellow_color}{datetime.datetime.now()} ---------->>>>>  GET LINK VIDEO FROM {channel_url}{Colorlog.reset_color}")
        try:
            json_link= get_links(object_id=f'@{name_channel}',table_name='youtube_video')
            list_searched=json_link['links']
        except:
            list_searched=[]
    
        arr_videos=[]
        try:
            videos= apyt.get_channel(channel_username=name_channel)
            for video in videos:
                if video['videoId'] in list_searched or x == max:
                    break
                arr_videos.append(video['videoId'])
                x+=1
        except Exception as e:
            print(e)
            pass
        try:
            videos= apyt.get_channel_stream(channel_username=name_channel)
            for video in videos:
                if video['videoId'] in list_searched or y == max:
                    break
                arr_videos.append(video['videoId'])
                y+=1
        except:
            pass
        for v in reversed(arr_videos):
            queue_link.put(f"https://www.youtube.com/watch?v={v}")
            print(f"------>>>>> put {v} to queue link")




            
