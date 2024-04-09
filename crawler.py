import json
import re
import time
import requests
import datetime
import json
from pytube import YouTube
from kafka_ncs_temp import push_kafka,push_kafka_update
from result import Post
from call_api import insert
from logger import Colorlog
from youtube_comment_downloader import *
downloader = YoutubeCommentDownloader()
class DetailCrawler:
    def __init__(self):
        pass

    def extract_post_infor(self,video_url):
        list_json=[]
        result_request= requests.get(video_url)
        result_json_1=json.loads(result_request.text.split('var ytInitialPlayerResponse = ')[-1].split(';</script>')[0])
        result_json_2=json.loads(result_request.text.split('var ytInitialData = ')[-1].split(';</script>')[0])
        data= result_json_1['videoDetails']
        print(data["viewCount"])
        data2 = result_json_1['microformat']
        avatar = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][1]['videoSecondaryInfoRenderer']['owner']['videoOwnerRenderer']['thumbnail']['thumbnails'][0]['url']
        hashtag = []
        try:
            list_hashtag = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['superTitleLink']['runs']
            for l in list_hashtag:
                if l['text']!=' ' and l['text']!=None :
                    hashtag.append(l['text'])
        except:
            pass
        comment = 0
        try:
            comment_element = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][2]['itemSectionRenderer']['contents'][0]['commentsEntryPointHeaderRenderer']['commentCount']
            comment = int(comment_element['simpleText'])
        except:
            pass

        like=0
        try:
            like_txt=result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['videoActions']['menuRenderer']['topLevelButtons'][0]['segmentedLikeDislikeButtonViewModel']['likeButtonViewModel']['likeButtonViewModel']['toggleButtonViewModel']['toggleButtonViewModel']['defaultButtonViewModel']['buttonViewModel']['accessibilityText']
            likes = re.findall(r'\d+[\.,\d]*', like_txt)
            like = [int(num.replace('.', '').replace(',', '')) for num in likes][0]
        except:
            pass
        list_json.append(data)
        list_json.append(data2)
        list_json.append(hashtag)
        list_json.append(comment)
        list_json.append(like)
        list_json.append(avatar)
        return list_json


    def extract_video_info_json(self,video_url,proxies):
        try:
            if proxies['http']=='' or proxies['http']==None:
                result_request= requests.get(video_url)
            else:
                result_request= requests.get(video_url,proxies)
            json_list=[]
            result_json_1=json.loads(result_request.text.split('var ytInitialPlayerResponse = ')[-1].split(';</script>')[0])
            result_json_2=json.loads(result_request.text.split('var ytInitialData = ')[-1].split(';</script>')[0])
            data= result_json_1['videoDetails']
            data2 = result_json_1['microformat']
            avatar = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][1]['videoSecondaryInfoRenderer']['owner']['videoOwnerRenderer']['thumbnail']['thumbnails'][0]['url']
            hashtag = []
            try:
                list_hashtag = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['superTitleLink']['runs']
                for l in list_hashtag:
                    if l['text']!=' ' and l['text']!=None :
                        hashtag.append(l['text'])
            except:
                pass
            comment = 0
            try:
                comment_element = result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][2]['itemSectionRenderer']['contents'][0]['commentsEntryPointHeaderRenderer']['commentCount']
                comment = int(comment_element['simpleText'])
            except:
                pass

            like=0
            try:
                like_txt=result_json_2['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']['videoActions']['menuRenderer']['topLevelButtons'][0]['segmentedLikeDislikeButtonViewModel']['likeButtonViewModel']['likeButtonViewModel']['toggleButtonViewModel']['toggleButtonViewModel']['defaultButtonViewModel']['buttonViewModel']['accessibilityText']
                likes = re.findall(r'\d+[\.,\d]*', like_txt)
                like = [int(num.replace('.', '').replace(',', '')) for num in likes][0]
            except:
                pass
            json_list.append(data)
            json_list.append(data2)
            json_list.append(hashtag)
            json_list.append(comment)
            json_list.append(like)
            json_list.append(avatar)

            data= eval(str(json_list[0]))
            data2= eval(str(json_list[1]))
            video_info = {}
            current_time = int(datetime.datetime.now().timestamp())
            video_info["time_crawl"] = current_time
            video_info["author"]=data["author"]
            video_info["author_link"]=data2["playerMicroformatRenderer"]["ownerProfileUrl"]
            video_info["author_id"]=data['channelId']
            video_info["link"] = video_url
            video_info["id"] = f'yt_{data["videoId"]}'
            id=data["videoId"]
            video_info["title"] = data["title"]
            video_info["source_id"]=''
            try:
                video_info["description"] = data2["playerMicroformatRenderer"]["description"]["simpleText"]
            except:
                video_info["description"]=''
            try:
                video_info["hashtag"]=json_list[2]
            except:
                video_info["hashtag"]=[]
            video_info["type"] = "youtube video"
            video_info["domain"] = "www.youtube.com"
            
            try: 
                video_info["like"] = int(json_list[4])
            except: 
                video_info["like"]= 0
            video_info["avatar"] = json_list[5]
            try:
                video_info["view"]=int(data["viewCount"])
            except:
                video_info["view"]=0
            video_info['created_time'] = int(datetime.datetime.strptime(data2["playerMicroformatRenderer"]["uploadDate"], "%Y-%m-%dT%H:%M:%S%z").timestamp())
            video_info["duration"] = int(data["lengthSeconds"])
            video_info['content']= ''
            time.sleep(2)
            try: 
                video_info["comment"] = int(json_list[3])
            except: 
                video_info["comment"]=0
            with open(f"data/{video_info['id']}.txt",'w+',encoding='utf-8') as file:
                for key, value in video_info.items():
                     file.write(f'{key}: {value}\n')
                file.close()
            return Post(**video_info),video_info["comment"]
        except Exception as e:
            print("Exception at extract_video_info_json(): ",e)
            return
            # if mode ==1:
            #         with open(f'error/error_link.txt', "a") as file:
            #             file.write(id)
            #             file.write('\n')
            # if mode ==2:
            #         with open(f'error/{str(video_info["author_link"]).split("/")[-1]}_error.txt', "a") as file:
            #             file.write(id)
            #             file.write('\n')
            # return None

    

    def extract_video_id(self, video_link):
        pattern = r"(?:watch\?v=)([a-zA-Z0-9_-]{11})"
        match = re.search(pattern, video_link)
        if match:
            return match.group(1)
        return None

    def preprocess(self, text):
        return re.findall(r"\w+", text.lower())

    def create_index(self, documents):
        index = {}
        for doc_id, text in enumerate(documents):
            for word in self.preprocess(text):
                if word not in index:
                    index[word] = []
                index[word].append(doc_id)
        return index

    def is_exist(self, index, word):
        query_words = self.preprocess(word)
        for word in query_words:
            if word in index:
                return True
        return False

    def link_to_id(self,link):
        yt = YouTube(link)
        video_id = yt.video_id
        return video_id
    #mode 1: keyword
    #mode 2: channel

    def run(self,queue_link,mode,proxies):
        while not queue_link.empty():
            try:
                video_url=queue_link.get()
                time.sleep(5)
                print(f"{Colorlog.green_color}{datetime.datetime.now()} ---------->>>>>  GET INFORMATION OF {video_url}{Colorlog.reset_color}")
                (video_info,cmt)=self.extract_video_info_json(video_url=video_url,proxies=proxies)
                if video_info is None:
                    return None,[]
                if int(mode)==3:
                    push_kafka_update(posts=[video_info],comments=None)
                else:
                    push_kafka(posts=[video_info],comments=None)
                if cmt>0:
                    try:
                        comments = downloader.get_comments_from_url(youtube_url=video_url,sort_by=SORT_BY_RECENT)
                        for comment in comments:
                            comment_p=Post(**comment)
                            push_kafka_update(posts=[comment_p],comments=None)
                    except Exception as e:
                        print(e)
            except:
                pass
            # if mode==1:

            #     # lưu link vào local
            #     with open('link_crawled/crawled.txt', "a") as file:
            #         file.write(f'{self.link_to_id(video_url)}\n')

            #     # lưu link vào db
            #     insert('youtube_video','crawled',self.link_to_id(video_url))

            # if mode==2:
            #     # lưu link vào local
            #     with open(f'link_crawled/{str(video_info.author_link).split("/")[-1]}.txt', "a+") as file:
            #         file.write(f'{self.link_to_id(video_url)}\n')
                
                # lưu link vào db
                insert('youtube_video',str(video_info.author_link).split("/")[-1],self.link_to_id(video_url))

            return
            # except Exception as e:
                # print(e)
                # if mode ==1:
                #     with open(f'error/error_link.txt.txt', "a") as file:
                #         file.write(self.link_to_id(video_url))
                #         file.write('\n')
                # if mode ==2:
                #     with open(f'error/{str(video_info.author_link).split("/")[-1]}_error.txt', "a") as file:
                #         file.write(self.link_to_id(video_url))
                #         file.write('\n')
                #self.driver.switch_to.window(self.driver.window_handles[1])
                # return None,[]
    
    def run_update(self, video_url,mode,check):
            if check==True:
                print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Bắt đầu crawl link : : {video_url}{Colorlog.reset_color}")
                time.sleep(2)
                try:
                    
                    time.sleep(4)
                    print(f"{Colorlog.green_color}{datetime.datetime.now()} »» Crawl thông tin video có link là: {video_url}{Colorlog.reset_color}")
                    video_info=self.extract_video_info_json(video_url=video_url,mode=mode)
                    if video_info is None:
                        return None,[]
                    if int(mode)==3:
                        push_kafka_update(posts=[video_info],comments=None)
                    else:
                        push_kafka(posts=[video_info],comments=None)
                    comments=[]
                    # if video_info.comment > 0 :
                    #     try:
                    #         comments= self.get_comments_from_url(youtube_url=video_url,mode=mode)
                    #     except Exception as e:
                    #         print(e)
                    # if mode==1:

                        # lưu link vào local
                        # with open('link_crawled/crawled.txt', "a") as file:
                        #     file.write(f'{self.link_to_id(video_url)}\n')

                        # lưu link vào db
                        #insert('youtube_video','crawled',self.link_to_id(video_url))

                    if mode==2:
                        # lưu link vào local
                        with open(f'link_crawled/{str(video_info.author_link).split("/")[-1]}.txt', "a") as file:
                            file.write(f'{self.link_to_id(video_url)}\n')
                        
                        # lưu link vào db
                        #insert('youtube_video',str(video_info.author_link).split("/")[-1],self.link_to_id(video_url))

                    
                    return video_info, comments
                except Exception as e:
                    print(e)
                    if mode ==1:
                        with open(f'error/error_link.txt.txt', "a") as file:
                            file.write(self.link_to_id(video_url))
                            file.write('\n')
                    if mode ==2:
                        with open(f'error/{str(video_info.author_link).split("/")[-1]}_error.txt', "a") as file:
                            file.write(self.link_to_id(video_url))
                            file.write('\n')
                    
                    #self.driver.switch_to.window(self.driver.window_handles[1])
                    return None,[]
            else:
                pass

