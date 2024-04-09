import device_config_ultils
import time
import threading
import queue
from elasticsearch import Elasticsearch
from datetime import timedelta,datetime
import datetime as dt
from logger import Colorlog
from dotenv import load_dotenv
import os
from get_link import Link
from crawler import DetailCrawler
queue_lock = threading.Lock()
tool_lock = threading.Lock()
list_driver = set()
queue_link_channel = queue.Queue()
queue_link_keyword = queue.Queue()
queue_link_update = queue.Queue()

def read_lines_from_file(url,mode):
    if mode ==1:
        try:
            with open('error/error_link.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    if mode ==2:
        try:
            with open(f'error/{str(url).split("/")[-1]}_error.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    return lines
def read_lines_from_file_done(url,mode):
    if mode ==1:
        try:
            with open('link_crawled/crawled.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    if mode ==2:
        try:
            with open(f'link_crawled/{str(url).split("/")[-1]}.txt', 'r') as file:
                lines = file.readlines()
        except:
            lines=[]
    return lines

def work_keyword(local_device_config):
    username=local_device_config[0]['account']['username']
    password=local_device_config[0]['account']['password']
    key_word_lists=local_device_config[0]['mode']['keyword']
    proxies=local_device_config[0]['proxy'][0]
    if username == "null" or username=="" or username==None:
        username=None
    if password == "null" or password=="" or password==None:
        password=None
    list_searched=[]
    try:
            for keyword in key_word_lists:
                tool=DetailCrawler()
                link=Link()
                try:
                    link.get_link_search(queue_link_keyword,keyword,list_searched)
                except:
                    pass
                while not queue_link_keyword.empty():
                    crawl_thread4 = threading.Thread(target=tool.run, args=(queue_link_keyword,1,proxies)) 
                    crawl_thread4.start()
                    crawl_thread5 = threading.Thread(target=tool.run, args=(queue_link_keyword,1,proxies))
                    crawl_thread5.start()
                    crawl_thread6 = threading.Thread(target=tool.run, args=(queue_link_keyword,1,proxies))
                    crawl_thread6.start()
                    crawl_thread4.join()
                    crawl_thread5.join()
                    crawl_thread6.join()

    except Exception as e:
        print(e)
        pass   


def work1(local_device_config):
    channel_urls=local_device_config[0]['mode']['page_id']
    max_post = local_device_config[0]['mode']['max_size_post']
    username=local_device_config[0]['account']['username']
    password=local_device_config[0]['account']['password']
    proxies=local_device_config[0]['proxy'][0]
    if username == "null" or username=="" or username==None:
        username=None
    if password == "null" or password=="" or password==None:
        password=None
    try:
        for channel in channel_urls:
            tool=DetailCrawler()
            link=Link()
            try:
                link.get_link_channel(queue_link_channel,channel,max=max_post)
            except:
                pass
            # queue_link_channel.put('https://www.youtube.com/watch?v=DEwc39NdB8s')
            while not queue_link_channel.empty():
                crawl_thread4 = threading.Thread(target=tool.run, args=(queue_link_channel,2,proxies)) 
                crawl_thread5 = threading.Thread(target=tool.run, args=(queue_link_channel,2,proxies))
                crawl_thread6 = threading.Thread(target=tool.run, args=(queue_link_channel,2,proxies))
                crawl_thread4.start()
                crawl_thread5.start()
                crawl_thread6.start()
                crawl_thread4.join()
                crawl_thread5.join()
                crawl_thread6.join()
    except Exception as e:
        print(e)
        pass

pass

def get_link(queue_link,gte,lte):
    load_dotenv()
    es_address = os.getenv("Elasticsearch")
    es = Elasticsearch([f"{es_address}"])
    body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"type.keyword": "youtube video"}},
                    {
                        "range": {
                            "created_time": {
                                "gte": gte,
                                "lte": lte,
                                "format": "MM/dd/yyyy HH:mm:ss"
                            }
                        }
                    }
                ]
            }
        },
        "size": 10000,
        "sort": [
            {
                "created_time": {
                    "order": "asc"
                }
            }
        ],
        "_source": ["link"]
    }

    result = es.search(index="posts", body=body)
    link=[]
    for hit in result['hits']['hits']:
        link.append(hit['_source']['link'])
        queue_link.put(hit['_source']['link'])
    es.close()
    print(f"{Colorlog.cyan_color}====== GET {len(link)}  from {gte} to {lte} for updating ====== {Colorlog.reset_color}")
   
def get_link_es(queue, time_start_update, range_date):
    current_time = dt.datetime.now().time()
    start_time = dt.datetime.strptime(time_start_update, "%H:%M").time()
    if current_time.hour == start_time.hour and (
        current_time.minute >= start_time.minute and
        current_time.minute <= start_time.minute + 1
    ):
        queue.queue.clear()
        time.sleep(3)
        print(f' Thời gian hiện tại là {time_start_update}. Bắt đầu thực hiện cập nhật')

        current_date = dt.datetime.now()
        current_date1 = dt.datetime.now() - dt.timedelta(days=int(range_date[0])-1)
        one_day_ago = current_date - dt.timedelta(days=int(range_date[0]))
        formatted_date = current_date1.strftime("%m/%d/%Y")
        one_day_ago_formatted_date = one_day_ago.strftime("%m/%d/%Y")
        six_day_ago = current_date - dt.timedelta(days=int(range_date[1]) - 1)
        seven_day_ago = current_date - dt.timedelta(days=int(range_date[1]))
        six_day_ago_formatted_date = six_day_ago.strftime("%m/%d/%Y")
        seven_day_ago_formatted_date = seven_day_ago.strftime("%m/%d/%Y")

        print(f"💻💻💻 Bắt đầu lấy link của ngày {one_day_ago_formatted_date} và ngày {seven_day_ago_formatted_date}")

        gte = f'{one_day_ago_formatted_date} 00:00:00'
        lte = f'{formatted_date} 00:00:00'
        try:
            get_link(queue_link=queue, gte=gte, lte=lte)
        except:
            pass

        time.sleep(5)

        gte = f'{seven_day_ago_formatted_date} 00:00:00'
        lte = f'{six_day_ago_formatted_date} 00:00:00'
        try:
            get_link(queue_link=queue, gte=gte, lte=lte)
        except:
            pass

        print(f" ☑ ☑ ☑ Đã lấy hết link của ngày {one_day_ago_formatted_date} và ngày {seven_day_ago_formatted_date}")


def update(local_device_config):
    options_list=local_device_config[0]['listArgument']
    time_start_upate=local_device_config[0]['mode']['start_time_run']
    range_date=local_device_config[0]['mode']['range_date']
    proxies=local_device_config[0]['proxy'][0]
    
    get_link_es(queue_link_update,time_start_upate,range_date)
    tool=DetailCrawler()
    while not queue_link_update.empty():
        crawl_thread4 = threading.Thread(target=tool.run, args=(queue_link_update,3,proxies)) 
        crawl_thread5 = threading.Thread(target=tool.run, args=(queue_link_update,3,proxies))
        crawl_thread6 = threading.Thread(target=tool.run, args=(queue_link_update,3,proxies))
        crawl_thread4.start()
        crawl_thread5.start()
        crawl_thread6.start()
        crawl_thread4.join()
        crawl_thread5.join()
        crawl_thread6.join()
