import os
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build

API_KEY = os.environ["YOUTUBE_API_KEY"]

youtube = build("youtube", "v3", developerKey=API_KEY)

VIDEO_IDS = ['voGbw_OA6lg',
 'M-NEKccJ5B4',
 'wEf_5sWHhsA',
 '6m3S5gUSFoI',
 'tnY9xbNm990',
 'nWO7AZTBSqE',
 'dhjKOZtfoPc',
 'Gz8pwS6bA9o',
 'iw2_03u9mi0',
 'XfpzOkmnIGM',
 'dAEurV0HXS8',
 'Gs9RYf_XNlk',
 'KmSGHrJI0Dk',
 '__UZRU1ecR8',
 'OCQcLyMmg1s',
 'qTzEIOnCa1s',
 'rfAoOujyRxY',
 'dgGi2FNPTRE',
 '_Ah5aup-1I4',
 'q-tbv7kI2Tg',
 'Mnt_egSP8Og',
 'OL7SRI8oZps',
 '_P4GDNxKQbo',
 'bxMa1QZvhxQ',
 'YUw9Jwb1PC4',
 'eg6C-TLAtlY',
 'aGyLZ6VGyUY',
 'KWo4OgHVzH8',
 'Q4lWpi_1GtA',
 'ejpBbUR0e5g',
 'cXeRr4eaOLM',
 '-BxjgYF_FWc',
 'G0dSrZeJJ9U',
 'WZXzLFNqjOw',
 'onc6JoF0SiI',
 'XtVqmmSwKJI',
 'fBscECRcv5A',
 'YPSvJarJv-o',
 'DBFu7MjEkVM',
 '_pqJ4kTPAOc',
 'lBM1WAsoHig',
 'O8ZmfJ3fBJg',
 'RAHjNK8Avtk',
 '6pLXf3haFpw',
 'o63J7Owg6CQ',
 'FwdWYiJeb14',
 'D5wvVlcX5YU',
 'OgRa63LNqSE',
 'm65h8s0RqEY',
 'E7zlPURIfXg',
 'Lp0dZlZkFb8',
 'Iz7rJLSIIyU',
 'z3a9VMP9m6k',
 'QZ6R7_4SwbA',
 'rAod7V_o5_M',
 'tVzVIeY5vXI'
]
def chunk_list(lst, chunk_size=50):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]
def fetch_video_metadata(video_ids):
    rows = []
    snapshot_date = datetime.utcnow().date()

    for batch in chunk_list(video_ids, 50):
        request = youtube.videos().list(
            part="snippet,statistics",
            id=",".join(batch)
        )
        response = request.execute()

        for item in response.get("items", []):
            rows.append({
                "video_id": item["id"],
                "snapshot_date": snapshot_date,
                "title": item["snippet"]["title"],
                "views": int(item["statistics"].get("viewCount", 0)),
                "likes": int(item["statistics"].get("likeCount", 0)),
                "comments": int(item["statistics"].get("commentCount", 0))
            })
    return pd.DataFrame(rows)
FILE_PATH = "youtube_title_snapshots.csv"
df_today = fetch_video_metadata(VIDEO_IDS)
if not df_today.empty:
    if os.path.exists(FILE_PATH):
        old_df = pd.read_csv(FILE_PATH)
        final_df = pd.concat([old_df, df_today], ignore_index=True)
    else:
        final_df = df_today
    final_df.to_csv(FILE_PATH, index=False)
