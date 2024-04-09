from downloader import * 
downloader = YoutubeCommentDownloader()
video_url='https://www.youtube.com/watch?v=DEwc39NdB8s'
comments = downloader.get_comments_from_url(youtube_url=video_url)
for comment in comments:
    print(comment)