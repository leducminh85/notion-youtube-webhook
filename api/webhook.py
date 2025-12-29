from flask import Flask, request, jsonify
from dotenv import load_dotenv  # dùng local thì mở, deploy vercel thì tắt
import os
import json
import time
import requests
from typing import Dict, List, Optional
from datetime import datetime

load_dotenv()

app = Flask(__name__)
NOTION_VERSION = "2022-06-28"

# -------------------------
# Helpers
# -------------------------
def notion_headers(notion_api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {notion_api_key}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_VERSION
    }

def safe_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {"_raw": resp.text}

def get_property_value(props: dict, prop_name: str):
    """Read value from Notion property inside webhook payload."""
    prop = props.get(prop_name)
    if prop is None:
        return None
    if isinstance(prop, str):
        return prop
    if isinstance(prop, dict) and prop.get("text", {}).get("content"):
        return prop["text"]["content"]
    if isinstance(prop, dict) and "rich_text" in prop and prop["rich_text"]:
        return "".join(seg.get("text", {}).get("content", "") for seg in prop["rich_text"]) or None
    if isinstance(prop, dict) and "title" in prop and prop["title"]:
        return "".join(seg.get("text", {}).get("content", "") for seg in prop["title"]) or None
    if isinstance(prop, dict) and "url" in prop:
        return prop["url"]
    return None

def get_page_tong_id_from_database(notion_api_key: str, database_id: str) -> str:
    """Get container page_id (page tổng) that contains the triggering database."""
    r = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers=notion_headers(notion_api_key)
    )
    if r.status_code >= 300:
        raise ValueError(f"Retrieve database failed: {r.status_code} {r.text}")

    db = r.json()
    parent = db.get("parent", {})
    if parent.get("type") == "page_id":
        return parent["page_id"]
    if parent.get("type") == "database_id":
        return parent["database_id"]
    raise ValueError(f"Unsupported database parent type: {parent}")

def notion_get_database_schema(notion_api_key: str, database_id: str) -> dict:
    """Return {property_name: property_type} for that database."""
    r = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers=notion_headers(notion_api_key),
    )
    if r.status_code >= 300:
        raise ValueError(f"Retrieve database schema failed: {r.status_code} {r.text}")

    db = r.json()
    props = db.get("properties", {})
    out = {}
    for name, meta in props.items():
        t = meta.get("type")
        if t:
            out[name] = t
    return out

def notion_update_page_properties(notion_api_key: str, page_id: str, props: dict):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers(notion_api_key),
        json={"properties": props},
    )
    if r.status_code >= 300:
        raise ValueError(f"Update page failed: {r.status_code} {r.text}")
    return r.json()

def notion_prop_text(value: str, prop_type: str) -> dict:
    """Build correct Notion prop object for title/rich_text."""
    value = (value or "").strip()
    if prop_type == "title":
        return {"title": [{"type": "text", "text": {"content": value}}]} if value else {"title": []}
    # default rich_text
    return {"rich_text": [{"type": "text", "text": {"content": value}}]} if value else {"rich_text": []}

# -------------------------
# YouTube helpers
# -------------------------
def youtube_channel_id_from_url(yt_api_key: str, channel_url: str) -> str:
    """Support /channel/UC... or /@handle."""
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
        raise ValueError("Channel not found (snippet)")
    return jd["items"][0]["snippet"]["title"]

def youtube_get_channel_stats(yt_api_key: str, channel_id: str) -> dict:
    r = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params={"id": channel_id, "key": yt_api_key, "part": "snippet,statistics"}
    )
    jd = safe_json(r)
    if not jd.get("items"):
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
    """Average upload cycle from latest 10 videos."""
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
        return "Lỗi khi lấy dữ liệu"

# -------------------------
# Notion video DB helpers
# -------------------------
def notion_create_database_under_page(notion_api_key: str, parent_page_id: str, db_title: str) -> str:
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": db_title}}],
        "properties": {
            "Title": {"title": {}},
            "Video URL": {"url": {}},
            "Published": {"date": {}},
            "Views": {"number": {"format": "number"}},
            "Description": {"rich_text": {}},
            "Thumbnail": {"files": {}}
        }
    }

    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(notion_api_key),
        json=payload
    )
    if r.status_code >= 300:
        raise ValueError(f"Create database failed: {r.status_code} {r.text}")

    return r.json()["id"]

def notion_insert_video_row(
    notion_api_key: str,
    database_id: str,
    title: str,
    video_url: str,
    published_at_iso: str,
    views: int,
    description: str,
    thumbnail_url: Optional[str]
):
    desc = (description or "").strip()
    if len(desc) > 1800:
        desc = desc[:1800] + "…"

    props = {
        "Title": {"title": [{"text": {"content": title}}]},
        "Video URL": {"url": video_url},
        "Published": {"date": {"start": published_at_iso}},
        "Views": {"number": views},
        "Description": {"rich_text": [{"text": {"content": desc}}]} if desc else {"rich_text": []},
    }

    if thumbnail_url:
        props["Thumbnail"] = {
            "files": [{"name": "thumbnail", "type": "external", "external": {"url": thumbnail_url}}]
        }
    else:
        props["Thumbnail"] = {"files": []}

    payload = {"parent": {"database_id": database_id}, "properties": props}

    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=notion_headers(notion_api_key),
        json=payload
    )

    if r.status_code == 429:
        time.sleep(1.5)
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(notion_api_key),
            json=payload
        )

    if r.status_code >= 300:
        print("Insert failed:", r.status_code, r.text)

# -------------------------
# Route
# -------------------------
@app.route("/webhook", methods=["POST"])
def webhook():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not yt_api_key or not notion_api_key:
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        data = payload.get("data", {})
        props = data.get("properties", {})

        # (A) Lấy channel_url như code đang chạy
        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        # (B) Lấy database_id & page_tong_id như code đang chạy
        parent = data.get("parent", {})
        triggering_db_id = parent.get("database_id")
        if not triggering_db_id:
            raise ValueError("Missing data.parent.database_id (trigger must be a database row)")

        page_tong_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)

        # (C) NEW: page_id của row hiện tại để update stats vào database Channels
        page_id = data.get("id")
        if not page_id:
            raise ValueError("Missing data.id (page id of the row)")

        # YouTube: channel stats + frequency
        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)

        # (D) NEW: update row hiện tại theo schema thật của database Channels
        schema = notion_get_database_schema(notion_api_key, triggering_db_id)

        # Tên cột output của bạn
        OUT_TITLE = "Tên kênh"
        OUT_SUBS = "Subcriber"
        OUT_VIDEOS = "Số video"
        OUT_VIEWS = "Tổng view"
        OUT_FREQ = "Chu kì đăng video"

        update_props = {}

        # Text fields: title or rich_text
        if OUT_TITLE in schema and schema[OUT_TITLE] in ("title", "rich_text"):
            update_props[OUT_TITLE] = notion_prop_text(stats["title"], schema[OUT_TITLE])
        if OUT_FREQ in schema and schema[OUT_FREQ] in ("title", "rich_text"):
            update_props[OUT_FREQ] = notion_prop_text(freq, schema[OUT_FREQ])

        # Number fields
        if OUT_SUBS in schema and schema[OUT_SUBS] == "number":
            update_props[OUT_SUBS] = {"number": stats["subscriberCount"]}
        if OUT_VIDEOS in schema and schema[OUT_VIDEOS] == "number":
            update_props[OUT_VIDEOS] = {"number": stats["videoCount"]}
        if OUT_VIEWS in schema and schema[OUT_VIEWS] == "number":
            update_props[OUT_VIEWS] = {"number": stats["viewCount"]}

        # chỉ update nếu có cột khớp schema
        if update_props:
            notion_update_page_properties(notion_api_key, page_id, update_props)

        # (E) Phần tạo DB video như code đang chạy
        channel_title = stats["title"]
        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id, limit=None)

        video_ids = []
        for it in items:
            vid = it.get("snippet", {}).get("resourceId", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        views_map = youtube_get_view_counts(yt_api_key, video_ids)

        new_db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)

        for it in items:
            sn = it.get("snippet", {})
            title = sn.get("title", "Untitled")
            published = sn.get("publishedAt")
            description = sn.get("description", "")
            vid = sn.get("resourceId", {}).get("videoId")

            if not vid or not published:
                continue

            thumbs = sn.get("thumbnails", {})
            thumb_url = (
                thumbs.get("high", {}).get("url")
                or thumbs.get("medium", {}).get("url")
                or thumbs.get("default", {}).get("url")
            )

            notion_insert_video_row(
                notion_api_key=notion_api_key,
                database_id=new_db_id,
                title=title,
                video_url=f"https://www.youtube.com/watch?v={vid}",
                published_at_iso=published,
                views=views_map.get(vid, 0),
                description=description,
                thumbnail_url=thumb_url
            )

            time.sleep(0.35)

        return jsonify({
            "status": "success",
            "message": "Updated channel row + created channel videos database",
            "page_id": page_id,
            "triggering_db_id": triggering_db_id,
            "page_tong_id": page_tong_id,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "subscriber": stats["subscriberCount"],
            "video_count_channel": stats["videoCount"],
            "total_views_channel": stats["viewCount"],
            "upload_frequency": freq,
            "new_database_id": new_db_id,
            "video_count_inserted": len(items)
        }), 200

    except Exception as e:
        # trả lỗi rõ để debug nhanh
        return jsonify({"status": "error", "message": str(e)}), 500


# ⚠️ Deploy Vercel: KHÔNG dùng app.run()
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
