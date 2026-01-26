import time
from typing import List, Optional, Dict
import requests
from ..utils import safe_json
from datetime import datetime
import logging
import re
logger = logging.getLogger(__name__)
from urllib.parse import urlparse


def youtube_channel_id_from_url(api_key: str, channel_url: str) -> str:
    """
    Supports:
      - https://www.youtube.com/channel/UCxxxx...
      - https://www.youtube.com/@handle
      - https://www.youtube.com/@handle/videos (và các tab khác)
    """
    if not channel_url or not isinstance(channel_url, str):
        raise ValueError("Invalid channel_url")

    u = channel_url.strip()

    # Thêm scheme nếu thiếu
    if not re.match(r"^https?://", u, re.IGNORECASE):
        u = "https://" + u

    parsed = urlparse(u)
    host = (parsed.netloc or "").lower()
    path = parsed.path or ""

    # Normalize path
    path = re.sub(r"/{2,}", "/", path).rstrip("/")

    if "youtube.com" not in host:
        raise ValueError(f"Not a YouTube URL: {channel_url}")

    # 1. Dạng /channel/UC...
    m = re.search(r"/channel/(UC[a-zA-Z0-9_-]{22})", path)
    if m:
        return m.group(1)

    # 2. Dạng /@handle...
    m = re.search(r"^/(@[^/]+)", path)
    if m:
        handle = m.group(1)  # bao gồm dấu @

        # Cách hiện đại (từ 2024+): dùng channels.list + forHandle
        url = "https://www.googleapis.com/youtube/v3/channels"
        params = {
            "part": "id",
            "forHandle": handle,
            "key": api_key,
        }
        r = requests.get(url, params=params, timeout=12)
        r.raise_for_status()  # tự raise nếu 4xx/5xx

        data = r.json()
        items = data.get("items", [])
        if not items:
            raise ValueError(f"Cannot resolve channel id from handle: {handle}")

        return items[0]["id"]

    raise ValueError(
        "Unsupported YouTube channel URL format. "
        "Supported: /channel/UC... or /@handle"
	)


def youtube_get_channel_title(yt_api_key: str, channel_id: str) -> str:
	r = requests.get(
		"https://www.googleapis.com/youtube/v3/channels",
		params={"id": channel_id, "key": yt_api_key, "part": "snippet"}
	)
	jd = safe_json(r)
	if not jd.get("items"):
		logger.error("youtube_get_channel_title: channel not found (snippet) %s (resp: %s)", channel_id, jd)
		raise ValueError("Channel not found (snippet)")
	return jd["items"][0]["snippet"]["title"]


def youtube_get_channel_stats(yt_api_key: str, channel_id: str) -> dict:
	r = requests.get(
		"https://www.googleapis.com/youtube/v3/channels",
		params={"id": channel_id, "key": yt_api_key, "part": "snippet,statistics"}
	)
	jd = safe_json(r)
	if not jd.get("items"):
		logger.error("youtube_get_channel_stats: channel not found %s (resp: %s)", channel_id, jd)
		raise ValueError("Channel not found (snippet,statistics)")
	it = jd["items"][0]
	sn = it.get("snippet", {})
	st = it.get("statistics", {})
	return {
		"title": sn.get("title", ""),
		"subscriberCount": int(st.get("subscriberCount") or 0),
		"videoCount": int(st.get("videoCount") or 0),
		"viewCount": int(st.get("viewCount") or 0),
	}


def youtube_uploads_playlist_id(yt_api_key: str, channel_id: str) -> str:
	r = requests.get(
		"https://www.googleapis.com/youtube/v3/channels",
		params={"id": channel_id, "key": yt_api_key, "part": "contentDetails"}
	)
	jd = safe_json(r)
	if not jd.get("items"):
		logger.error("youtube_uploads_playlist_id: channel not found %s (resp: %s)", channel_id, jd)
		raise ValueError("Channel not found (contentDetails)")
	return jd["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]


def youtube_playlist_videos_basic(
	yt_api_key: str,
	uploads_playlist_id: str,
	limit: Optional[int] = None
) -> List[dict]:
	videos: List[dict] = []
	next_page_token: Optional[str] = None

	while True:
		params = {
			"playlistId": uploads_playlist_id,
			"key": yt_api_key,
			"part": "snippet",
			"maxResults": 50
		}
		if next_page_token:
			params["pageToken"] = next_page_token

		r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
		jd = safe_json(r)

		items = jd.get("items", [])
		items = [
			it for it in items
			if it.get("snippet", {}).get("title") not in ("Private video", "Deleted video")
		]

		videos.extend(items)

		if limit is not None and len(videos) >= limit:
			return videos[:limit]

		next_page_token = jd.get("nextPageToken")
		if not next_page_token:
			break

		time.sleep(0.05)

	return videos


def youtube_get_view_counts(yt_api_key: str, video_ids: List[str]) -> Dict[str, int]:
	out: Dict[str, int] = {}

	for i in range(0, len(video_ids), 50):
		batch = video_ids[i:i+50]
		r = requests.get(
			"https://www.googleapis.com/youtube/v3/videos",
			params={"id": ",".join(batch), "key": yt_api_key, "part": "statistics"}
		)
		jd = safe_json(r)
		for item in jd.get("items", []):
			vid = item.get("id")
			vc = item.get("statistics", {}).get("viewCount")
			if vid and vc is not None:
				try:
					out[vid] = int(vc)
				except Exception:
					out[vid] = 0

		time.sleep(0.15)

	return out


def get_upload_frequency(yt_api_key: str, channel_id: str) -> str:
	try:
		r = requests.get(
			"https://www.googleapis.com/youtube/v3/search",
			params={
				"part": "snippet",
				"channelId": channel_id,
				"maxResults": 10,
				"order": "date",
				"type": "video",
				"key": yt_api_key,
			},
		)
		jd = safe_json(r)
		items = jd.get("items", [])
		if not items or len(items) < 2:
			return "Không đủ video để tính chu kỳ"

		dates: List[datetime] = []
		for it in items:
			p = it.get("snippet", {}).get("publishedAt")
			if not p:
				continue
			try:
				dates.append(datetime.fromisoformat(p.replace("Z", "+00:00")))
			except Exception:
				logger.debug("get_upload_frequency: failed parsing publishedAt: %s", p, exc_info=True)
				pass

		dates.sort(reverse=True)
		if len(dates) < 2:
			return "Không đủ dữ liệu thời gian hợp lệ"

		diffs: List[float] = []
		for i in range(len(dates) - 1):
			diff_days = (dates[i] - dates[i + 1]).total_seconds() / (60 * 60 * 24)
			if diff_days >= 0:
				diffs.append(max(diff_days, 0.1))

		if not diffs:
			return "Không thể tính chu kỳ"

		avg_days = sum(diffs) / len(diffs)

		if avg_days <= 1:
			videos_per_day = round(1 / avg_days)
			if videos_per_day <= 1:
				return "1 video / 1 ngày"
			return f"{videos_per_day} video / 1 ngày"
		return f"1 video / {round(avg_days)} ngày"
	except Exception:
		logger.exception("get_upload_frequency failed for channel %s", channel_id)
		return "Lỗi khi lấy dữ liệu"



def youtube_get_video_comments(yt_api_key: str, video_id: str, max_results: int = 20) -> List[dict]:
    """
    Lấy comment thread và replies.
    QUAN TRỌNG: params['part'] phải có 'replies'.
    """
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet,replies",  # <--- BẮT BUỘC PHẢI CÓ 'replies' Ở ĐÂY
        "videoId": video_id,
        "key": yt_api_key,
        "maxResults": min(100, max_results),
        "textFormat": "plainText",
        "order": "relevance"
    }

    comments_data = []
    
    try:
        r = requests.get(url, params=params)
        
        # Nếu video tắt comment hoặc lỗi permission
        if r.status_code == 403 or r.status_code == 404:
            logger.warning(f"Video {video_id}: Comments disabled or not found.")
            return []
            
        data = safe_json(r)
        items = data.get("items", [])
        
        for item in items:
            # 1. Parse Comment Gốc (Top Level)
            top_obj = item.get("snippet", {}).get("topLevelComment", {}).get("snippet", {})
            
            thread = {
                "author": top_obj.get("authorDisplayName", "Anonymous"),
                "text": top_obj.get("textDisplay", ""),
                "likes": top_obj.get("likeCount", 0),
                "replies": []
            }
            
            # 2. Parse Replies (Nằm trong field 'replies')
            # Lưu ý: YouTube chỉ trả về tối đa 5 reply trong request này.
            # Nếu item không có key 'replies', nghĩa là không có reply.
            replies_wrapper = item.get("replies", {})
            if replies_wrapper:
                replies_list = replies_wrapper.get("comments", [])
                
                # Sắp xếp reply cũ nhất -> mới nhất để dễ đọc (hoặc ngược lại tùy ý)
                # replies_list.reverse() 
                
                for rep in replies_list:
                    r_snip = rep.get("snippet", {})
                    thread["replies"].append({
                        "author": r_snip.get("authorDisplayName", "Anonymous"),
                        "text": r_snip.get("textDisplay", ""),
                        "likes": r_snip.get("likeCount", 0)
                    })
            
            comments_data.append(thread)
            
    except Exception as e:
        logger.error(f"Error parsing comments for video {video_id}: {e}")
        # Không raise lỗi để code tiếp tục chạy video khác
        pass

    return comments_data
