import time
from typing import List, Optional, Dict
import requests
from ..utils import safe_json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def youtube_channel_id_from_url(yt_api_key: str, channel_url: str) -> str:
	if "/channel/" in channel_url:
		return channel_url.split("/channel/")[1].split("?")[0].strip("/")
	if "/@" in channel_url:
		handle = channel_url.split("/@")[1].split("?")[0].strip("/")
		r = requests.get(
			"https://www.googleapis.com/youtube/v3/channels",
			params={"forHandle": handle, "key": yt_api_key, "part": "id"}
		)
		jd = safe_json(r)
		if not jd.get("items"):
			logger.error("youtube_channel_id_from_url: channel not found by handle %s (resp: %s)", handle, jd)
			raise ValueError("Channel not found by handle")
		return jd["items"][0]["id"]
	raise ValueError("Invalid channel URL format. Use /channel/UC... or /@handle")


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

