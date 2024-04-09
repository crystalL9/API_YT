from youtube_comment_downloader import *
downloader = YoutubeCommentDownloader()
comments = downloader.get_comments_from_url('https://www.youtube.com/watch?v=DEwc39NdB8s', sort_by=SORT_BY_RECENT)
for comment in comments:
    print(comment)