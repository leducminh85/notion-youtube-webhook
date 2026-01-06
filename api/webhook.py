from flask import Flask, request, jsonify
# from dotenv import load_dotenv  # dùng local thì mở, deploy vercel thì tắt
import os
import json
import time
import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# load_dotenv()

app = Flask(__name__)
NOTION_VERSION = "2022-06-28"

# Logging setup
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

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
        logger.debug("safe_json: failed to parse JSON, returning raw text", exc_info=True)
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
        logger.error("Retrieve database failed: %s %s", r.status_code, r.text)
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
        logger.error("Retrieve database schema failed: %s %s", r.status_code, r.text)
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
        logger.error("Update page failed for %s: %s %s", page_id, r.status_code, r.text)
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
        logger.error("Create database failed under page %s: %s %s", parent_page_id, r.status_code, r.text)
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
        logger.warning("notion_insert_video_row: rate limited when inserting into %s; retrying once", database_id)
        time.sleep(1.5)
        r = requests.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(notion_api_key),
            json=payload
        )

    if r.status_code >= 300:
        logger.error("Insert failed into %s: %s %s", database_id, r.status_code, r.text)
        raise ValueError(f"Insert failed: {r.status_code} {r.text}")

def insert_video_batch(
    notion_api_key: str,
    database_id: str,
    videos_data: List[Dict[str, any]],
    max_workers: int = 8  # Số lượng concurrent requests tối đa
):
    def insert_single(video: Dict[str, any]):
        retries = 0
        max_retries = 3
        while retries < max_retries:
            try:
                notion_insert_video_row(
                    notion_api_key=notion_api_key,
                    database_id=database_id,
                    **video
                )
                return True, None
            except ValueError as e:
                err_str = str(e)
                if "429" in err_str:
                    # Xử lý rate limit: sleep dựa trên Retry-After nếu có, hoặc default 2s
                    retry_after = 2
                    logger.warning("insert_single: rate limited for video %s; sleeping %ss (attempt %s)", video.get("video_url"), retry_after, retries+1)
                    try:
                        # best-effort: if the last response was captured in exception message, we cannot access r here
                        pass
                    except Exception:
                        pass
                    time.sleep(retry_after)
                    retries += 1
                else:
                    logger.error("insert_single: failed inserting video %s: %s", video.get("video_url"), err_str)
                    return False, err_str
        return False, "Max retries exceeded"

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(insert_single, video) for video in videos_data]
        
        for future in as_completed(futures):
            success, err = future.result()
            if not success:
                logger.error("Error inserting video: %s", err)

# -------------------------
# Route
# -------------------------
@app.route("/get-channel-detail", methods=["POST"])
def get_channel_detail():
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

        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        triggering_db_id = data.get("parent", {}).get("database_id")
        if not triggering_db_id:
            raise ValueError("Missing data.parent.database_id")

        page_tong_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)
        page_id = data.get("id")
        if not page_id:
            raise ValueError("Missing data.id (page id of the row)")

        # --- YouTube data ---
        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)
        channel_title = stats["title"]

        # --- Update channel row stats ---
        schema = notion_get_database_schema(notion_api_key, triggering_db_id)
        OUT_TITLE = "Tên kênh"
        OUT_SUBS = "Subcriber"
        OUT_VIDEOS = "Số video"
        OUT_VIEWS = "Tổng view"
        OUT_FREQ = "Chu kì đăng video"

        update_props = {}
        if OUT_TITLE in schema and schema[OUT_TITLE] in ("title", "rich_text"):
            update_props[OUT_TITLE] = notion_prop_text(channel_title, schema[OUT_TITLE])
        if OUT_FREQ in schema and schema[OUT_FREQ] in ("title", "rich_text"):
            update_props[OUT_FREQ] = notion_prop_text(freq, schema[OUT_FREQ])
        if OUT_SUBS in schema and schema[OUT_SUBS] == "number":
            update_props[OUT_SUBS] = {"number": stats["subscriberCount"]}
        if OUT_VIDEOS in schema and schema[OUT_VIDEOS] == "number":
            update_props[OUT_VIDEOS] = {"number": stats["videoCount"]}
        if OUT_VIEWS in schema and schema[OUT_VIEWS] == "number":
            update_props[OUT_VIEWS] = {"number": stats["viewCount"]}

        if update_props:
            notion_update_page_properties(notion_api_key, page_id, update_props)

        # --- Video database handling ---
        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)
        # VIDEO_LIMIT = 200  # Giới hạn để tránh timeout
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id, limit=None)

        video_ids = [it["snippet"]["resourceId"]["videoId"] for it in items if it.get("snippet", {}).get("resourceId", {}).get("videoId")]
        views_map = youtube_get_view_counts(yt_api_key, video_ids)

        # Tìm database video hiện có (hỗ trợ cả title format cũ và mới)
        existing_db_id = None
        r = requests.get(
            f"https://api.notion.com/v1/blocks/{page_tong_id}/children",
            headers=notion_headers(notion_api_key),
            params={"page_size": 100}
        )
        if r.status_code >= 300:
            raise ValueError(f"List children failed: {r.status_code} {r.text}")

        children = r.json().get("results", [])
        for child in children:
            if child.get("type") == "child_database":
                title_parts = child.get("child_database", {}).get("title", [])
                db_title = ""
                for part in title_parts:
                    if isinstance(part, dict):
                        db_title += part.get("text", {}).get("content", "") or part.get("plain_text", "")
                    elif isinstance(part, str):
                        db_title += part
                if db_title.strip() == channel_title.strip():
                    existing_db_id = child["id"]
                    break

        if not existing_db_id:
            # Chưa có → tạo mới
            db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)
            logger.info("Created new video database %s", db_id)
            should_clear_old = False
        else:
            # Đã có → dùng lại và sẽ clear toàn bộ rows cũ
            db_id = existing_db_id
            logger.info("Found existing video database %s → will refresh all videos", db_id)
            should_clear_old = True

        # Nếu database đã tồn tại → xóa toàn bộ rows cũ trước khi insert mới
        if should_clear_old:
            logger.info("Archiving (deleting) all existing rows in video database %s", db_id)
            has_more = True
            next_cursor = None
            pages_to_archive = []

            while has_more:
                query_payload = {"page_size": 100}
                if next_cursor:
                    query_payload["start_cursor"] = next_cursor

                r = requests.post(
                    f"https://api.notion.com/v1/databases/{db_id}/query",
                    headers=notion_headers(notion_api_key),
                    json=query_payload
                )
                if r.status_code >= 300:
                    logger.warning("Query failed during archive, skipping clear: %s %s", r.status_code, r.text)
                    should_clear_old = False
                    break

                res = r.json()
                for page in res.get("results", []):
                    if not page.get("archived", False):  # Chỉ lấy những row chưa bị archive
                        pages_to_archive.append(page["id"])

                has_more = res.get("has_more", False)
                next_cursor = res.get("next_cursor")

            # Archive parallel (nhanh và đúng cách)
            if pages_to_archive:
                def archive_page(page_id_archive):
                    r_patch = requests.patch(
                        f"https://api.notion.com/v1/pages/{page_id_archive}",
                        headers=notion_headers(notion_api_key),
                        json={"archived": True}
                    )
                    if r_patch.status_code >= 300:
                        logger.warning("Failed to archive page %s: %s %s", page_id_archive, r_patch.status_code, r_patch.text)

                with ThreadPoolExecutor(max_workers=10) as executor:
                    executor.map(archive_page, pages_to_archive)

                logger.info("Successfully archived %s old video rows", len(pages_to_archive))
            else:
                logger.info("No old rows to archive")

        # --- Chuẩn bị và insert video mới ---
        videos_data = []
        for it in items:
            sn = it.get("snippet", {})
            title = sn.get("title", "Untitled")
            published = sn.get("publishedAt")
            description = sn.get("description", "")
            vid = sn.get("resourceId", {}).get("videoId")
            if not vid or not published:
                continue

            thumbs = sn.get("thumbnails", {})
            thumb_url = thumbs.get("high", {}).get("url") or thumbs.get("medium", {}).get("url") or thumbs.get("default", {}).get("url")

            videos_data.append({
                "title": title,
                "video_url": f"https://www.youtube.com/watch?v={vid}",
                "published_at_iso": published,
                "views": views_map.get(vid, 0),
                "description": description,
                "thumbnail_url": thumb_url
            })

        if videos_data:
            logger.info("Inserting %s videos (full refresh) into database %s", len(videos_data), db_id)
            insert_video_batch(notion_api_key, db_id, videos_data, max_workers=8)
        else:
            logger.info("No videos to insert")

        return jsonify({
            "status": "success",
            "message": "Channel stats updated + video database refreshed completely",
            "page_id": page_id,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "subscriber": stats["subscriberCount"],
            "video_count_channel": stats["videoCount"],
            "total_views_channel": stats["viewCount"],
            "upload_frequency": freq,
            "video_database_id": db_id,
            "videos_refreshed": len(videos_data)
        }), 200

    except Exception as e:
        logger.exception("/get-channel-detail failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500

def notion_retrieve_page(notion_api_key: str, page_id: str) -> dict:
    r = requests.get(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers(notion_api_key),
    )
    if r.status_code >= 300:
        raise ValueError(f"Retrieve page failed: {r.status_code} {r.text}")
    return r.json()

def notion_get_database_schema(notion_api_key: str, database_id: str) -> dict:
    r = requests.get(
        f"https://api.notion.com/v1/databases/{database_id}",
        headers=notion_headers(notion_api_key),
    )
    if r.status_code >= 300:
        raise ValueError(f"Retrieve database schema failed: {r.status_code} {r.text}")

    db = r.json()
    props = db.get("properties", {})
    return {name: meta.get("type") for name, meta in props.items() if meta.get("type")}

def notion_update_page_properties(notion_api_key: str, page_id: str, props: dict) -> dict:
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=notion_headers(notion_api_key),
        json={"properties": props},
    )
    if r.status_code >= 300:
        raise ValueError(f"Update page failed: {r.status_code} {r.text}")
    return r.json()

def notion_prop_text(value: str, prop_type: str) -> dict:
    value = (value or "").strip()
    if prop_type == "title":
        return {"title": [{"type": "text", "text": {"content": value}}]} if value else {"title": []}
    return {"rich_text": [{"type": "text", "text": {"content": value}}]} if value else {"rich_text": []}

@app.route("/update-channel-info", methods=["POST"])
def update_channel_only():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400
    logger.debug("/update received payload keys: %s", list(payload.keys()) if isinstance(payload, dict) else str(type(payload)))

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        logger.debug("env presence - YT: %s, NOTION: %s", bool(yt_api_key), bool(notion_api_key))
        if not yt_api_key or not notion_api_key:
            logger.error("Missing required environment variables YOUTUBE_API_KEY or NOTION_API_KEY")
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        # Accept 2 formats:
        # A) webhook style: { data: { id, parent: {database_id}, properties: {...}}}
        # B) manual style: { "page_id": "xxxx" }
        data = payload.get("data", {})
        page_id = (data.get("id") or payload.get("page_id"))
        logger.debug("update_channel_only page_id resolved: %s", page_id)
        if not page_id:
            logger.error("Missing page_id (expected data.id or page_id); payload keys: %s", list(payload.keys()))
            raise ValueError("Missing page_id (expected data.id or page_id)")

        # Always retrieve full page to get reliable properties & parent database
        logger.info("Retrieving Notion page %s for update", page_id)
        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})

        channel_url = get_property_value(props, "Channel URL")
        logger.debug("Channel URL from page: %s", channel_url)
        if not channel_url:
            logger.error("Missing property: Channel URL on page %s", page_id)
            raise ValueError("Missing property: Channel URL")

        # Find the database that contains this row (Channels DB)
        triggering_db_id = page.get("parent", {}).get("database_id") or data.get("parent", {}).get("database_id")
        if not triggering_db_id:
            logger.error("Missing parent database_id for page %s (cannot identify Channels database)", page_id)
            raise ValueError("Missing parent database_id (cannot identify Channels database)")

        # YouTube: stats + upload frequency
        logger.info("Resolving channel info for page %s: url=%s", page_id, channel_url)
        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)
        logger.info("Fetched stats for channel %s: subs=%s videos=%s views=%s", channel_id, stats.get("subscriberCount"), stats.get("videoCount"), stats.get("viewCount"))

        # Read schema to avoid type mismatch
        schema = notion_get_database_schema(notion_api_key, triggering_db_id)

        OUT_TITLE = "Tên kênh"
        OUT_SUBS = "Subcriber"
        OUT_VIDEOS = "Số video"
        OUT_VIEWS = "Tổng view"
        OUT_FREQ = "Chu kì đăng video"

        update_props = {}

        # Text fields
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

        if not update_props:
            logger.error("No matching output properties found in database schema %s. Schema keys: %s", triggering_db_id, list(schema.keys()))
            raise ValueError("No matching output properties found in database schema. Check column names/types.")

        logger.info("Updating page %s with props: %s", page_id, update_props)
        notion_update_page_properties(notion_api_key, page_id, update_props)

        return jsonify({
            "status": "success",
            "message": "Updated channel stats only (no video sync)",
            "page_id": page_id,
            "database_id": triggering_db_id,
            "channel_id": channel_id,
            "channel_title": stats["title"],
            "subscriber": stats["subscriberCount"],
            "video_count": stats["videoCount"],
            "total_views": stats["viewCount"],
            "upload_frequency": freq
        }), 200

    except Exception as e:
        logger.exception("/update failed for page %s: %s", payload.get("page_id") or payload.get("data", {}).get("id"), e)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/update-channel-detail", methods=["POST"])
def update_channel_detail():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400

    logger.debug("/update-channel-detail received payload keys: %s", list(payload.keys()) if isinstance(payload, dict) else str(type(payload)))

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not yt_api_key or not notion_api_key:
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        # Hỗ trợ cả 2 kiểu payload: webhook hoặc manual { "page_id": "xxx" }
        data = payload.get("data", {})
        page_id = data.get("id") or payload.get("page_id")
        if not page_id:
            raise ValueError("Missing page_id (expected data.id or page_id)")

        # Lấy thông tin page để có properties và parent database_id
        logger.info("Retrieving Notion page %s", page_id)
        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})

        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        triggering_db_id = page.get("parent", {}).get("database_id")
        if not triggering_db_id:
            raise ValueError("Cannot find parent database_id of the channel row")

        page_tong_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)

        # --- YouTube: lấy thông tin kênh ---
        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)
        channel_title = stats["title"]

        # --- Cập nhật stats kênh vào row hiện tại ---
        schema = notion_get_database_schema(notion_api_key, triggering_db_id)

        OUT_TITLE = "Tên kênh"
        OUT_SUBS = "Subcriber"
        OUT_VIDEOS = "Số video"
        OUT_VIEWS = "Tổng view"
        OUT_FREQ = "Chu kì đăng video"

        update_props = {}
        if OUT_TITLE in schema and schema[OUT_TITLE] in ("title", "rich_text"):
            update_props[OUT_TITLE] = notion_prop_text(channel_title, schema[OUT_TITLE])
        if OUT_FREQ in schema and schema[OUT_FREQ] in ("title", "rich_text"):
            update_props[OUT_FREQ] = notion_prop_text(freq, schema[OUT_FREQ])
        if OUT_SUBS in schema and schema[OUT_SUBS] == "number":
            update_props[OUT_SUBS] = {"number": stats["subscriberCount"]}
        if OUT_VIDEOS in schema and schema[OUT_VIDEOS] == "number":
            update_props[OUT_VIDEOS] = {"number": stats["videoCount"]}
        if OUT_VIEWS in schema and schema[OUT_VIEWS] == "number":
            update_props[OUT_VIEWS] = {"number": stats["viewCount"]}

        if update_props:
            logger.info("Updating channel stats for page %s", page_id)
            notion_update_page_properties(notion_api_key, page_id, update_props)

        # --- Kiểm tra/tạo database video ---
        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)

        # Giới hạn 200 video mới nhất để an toàn timeout + quota
        VIDEO_LIMIT = 200
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id, limit=VIDEO_LIMIT)
        logger.info("Fetched %s latest videos (limit %s)", len(items), VIDEO_LIMIT)

        # Lấy view counts
        video_ids = [it["snippet"]["resourceId"]["videoId"] for it in items if it.get("snippet", {}).get("resourceId", {}).get("videoId")]
        views_map = youtube_get_view_counts(yt_api_key, video_ids)

        # Tìm database video hiện có
        existing_db_id = None
        r = requests.get(
            f"https://api.notion.com/v1/blocks/{page_tong_id}/children",
            headers=notion_headers(notion_api_key),
            params={"page_size": 100}
        )
        if r.status_code >= 300:
            raise ValueError(f"List children failed: {r.status_code} {r.text}")

        children = r.json().get("results", [])
        for child in children:
            if child.get("type") == "child_database":
                title_parts = child.get("child_database", {}).get("title", [])
                # Xử lý linh hoạt: title_parts có thể là list[dict] hoặc list[str]
                db_title = ""
                for part in title_parts:
                    if isinstance(part, dict):
                        # Cấu trúc rich text cũ
                        db_title += part.get("text", {}).get("content", "")
                    elif isinstance(part, str):
                        # Cấu trúc mới: plain text
                        db_title += part
                if db_title.strip() == channel_title.strip():
                    existing_db_id = child["id"]
                    break

        if not existing_db_id:
            # Chưa có → tạo mới và insert toàn bộ
            new_db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)
            logger.info("Created new video database %s for channel %s", new_db_id, channel_title)
            videos_to_insert = items
            insert_mode = "full"
        else:
            # Đã có → chỉ insert video mới (missing)
            new_db_id = existing_db_id
            logger.info("Found existing video database %s → will add missing videos only", new_db_id)

            # Query toàn bộ page trong database video để lấy các Video URL hiện có
            existing_urls = set()
            has_more = True
            next_cursor = None
            while has_more:
                query_payload = {"page_size": 100}
                if next_cursor:
                    query_payload["start_cursor"] = next_cursor

                r = requests.post(
                    f"https://api.notion.com/v1/databases/{new_db_id}/query",
                    headers=notion_headers(notion_api_key),
                    json=query_payload
                )
                if r.status_code >= 300:
                    raise ValueError(f"Query video database failed: {r.status_code} {r.text}")

                res = r.json()
                for page in res.get("results", []):
                    page_props = page.get("properties", {})
                    url_val = get_property_value(page_props, "Video URL")
                    if url_val:
                        existing_urls.add(url_val)

                has_more = res.get("has_more", False)
                next_cursor = res.get("next_cursor")

            logger.info("Found %s existing video rows in database", len(existing_urls))

            # Lọc chỉ video chưa có
            videos_to_insert = []
            for it in items:
                vid = it.get("snippet", {}).get("resourceId", {}).get("videoId")
                if not vid:
                    continue
                video_url = f"https://www.youtube.com/watch?v={vid}"
                if video_url not in existing_urls:
                    videos_to_insert.append(it)

            logger.info("%s new/missing videos to insert", len(videos_to_insert))
            insert_mode = "incremental"

        # Chuẩn bị data insert
        videos_data = []
        for it in videos_to_insert:
            sn = it.get("snippet", {})
            title = sn.get("title", "Untitled")
            published = sn.get("publishedAt")
            description = sn.get("description", "")
            vid = sn.get("resourceId", {}).get("videoId")
            if not vid or not published:
                continue

            thumbs = sn.get("thumbnails", {})
            thumb_url = thumbs.get("high", {}).get("url") or thumbs.get("medium", {}).get("url") or thumbs.get("default", {}).get("url")

            videos_data.append({
                "title": title,
                "video_url": f"https://www.youtube.com/watch?v={vid}",
                "published_at_iso": published,
                "views": views_map.get(vid, 0),
                "description": description,
                "thumbnail_url": thumb_url
            })

        # Insert batch nếu có video cần thêm
        if videos_data:
            logger.info("Inserting %s videos (%s mode) into database %s", len(videos_data), insert_mode, new_db_id)
            insert_video_batch(notion_api_key, new_db_id, videos_data, max_workers=6)
        else:
            logger.info("No new videos to insert")

        return jsonify({
            "status": "success",
            "message": f"Channel stats updated + video database {'created and filled' if insert_mode == 'full' else 'updated with missing videos'}",
            "page_id": page_id,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "subscriber": stats["subscriberCount"],
            "video_count_channel": stats["videoCount"],
            "total_views_channel": stats["viewCount"],
            "upload_frequency": freq,
            "video_database_id": new_db_id,
            "videos_inserted": len(videos_data),
            "insert_mode": insert_mode
        }), 200

    except Exception as e:
        logger.exception("/update-channel-detail failed: %s", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    
# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)
