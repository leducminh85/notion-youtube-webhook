from flask import Flask, request, jsonify
# from dotenv import load_dotenv  
import os
import json
import time
import requests
from typing import Dict, List, Optional

# load_dotenv()

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

    # Rare case: database inside another database
    if parent.get("type") == "database_id":
        return parent["database_id"]

    raise ValueError(f"Unsupported database parent type: {parent}")

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

def youtube_uploads_playlist_id(yt_api_key: str, channel_id: str) -> str:
    r = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params={"id": channel_id, "key": yt_api_key, "part": "contentDetails"}
    )
    jd = safe_json(r)
    if not jd.get("items"):
        raise ValueError("Channel not found (contentDetails)")
    return jd["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

def youtube_playlist_videos_basic(yt_api_key: str, uploads_playlist_id: str, limit: int = 100) -> List[dict]:
    """
    Returns list of items with snippet.resourceId.videoId, snippet.title, snippet.publishedAt, snippet.description, snippet.thumbnails
    """
    videos = []
    next_page_token = None

    while len(videos) < limit:
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

        next_page_token = jd.get("nextPageToken")
        if not next_page_token:
            break

    return videos[:limit]

def youtube_get_view_counts(yt_api_key: str, video_ids: List[str]) -> Dict[str, int]:
    """
    Fetch viewCount for many videos using videos.list (max 50 ids/request).
    Returns: { video_id: view_count_int }
    """
    out: Dict[str, int] = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        r = requests.get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={
                "id": ",".join(batch),
                "key": yt_api_key,
                "part": "statistics"
            }
        )
        jd = safe_json(r)
        for item in jd.get("items", []):
            vid = item.get("id")
            stats = item.get("statistics", {})
            vc = stats.get("viewCount")
            if vid and vc is not None:
                try:
                    out[vid] = int(vc)
                except Exception:
                    out[vid] = 0

        # nhẹ nhàng tránh quota spike
        time.sleep(0.15)

    return out

# -------------------------
# Notion helpers
# -------------------------
def notion_create_database_under_page(
    notion_api_key: str,
    parent_page_id: str,
    db_title: str
) -> str:
    """
    Create a database under page tổng with required properties.
    Adds Views (number) + Description (rich_text).
    Published is date (includes time).
    """
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": db_title}}],
        "properties": {
            "Title": {"title": {}},
            "Video URL": {"url": {}},
            "Published": {"date": {}},          # includes time from ISO publishedAt
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
    # Notion rich_text has limits; keep description reasonable
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
    print("=== Headers ===")
    print(request.headers)

    payload = request.get_json(silent=True)
    if not payload:
        print("=== Raw Body (not JSON) ===")
        print(request.data.decode("utf-8", errors="replace"))
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400

    print("=== Payload JSON ===")
    print(json.dumps(payload, indent=4, ensure_ascii=False))

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not yt_api_key or not notion_api_key:
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        data = payload.get("data", {})
        props = data.get("properties", {})

        # Property name per your real payload
        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        # Database that triggered this row
        parent = data.get("parent", {})
        triggering_db_id = parent.get("database_id")
        if not triggering_db_id:
            raise ValueError("Missing data.parent.database_id (trigger must be a database row)")

        # page tổng that contains the triggering database
        page_tong_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)

        # YouTube: channel_id + channel title (for DB title)
        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        channel_title = youtube_get_channel_title(yt_api_key, channel_id)

        # YouTube: uploads playlist + basic video list
        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id, limit=100)

        # Collect video IDs then fetch views in batch
        video_ids = []
        for it in items:
            vid = it.get("snippet", {}).get("resourceId", {}).get("videoId")
            if vid:
                video_ids.append(vid)

        views_map = youtube_get_view_counts(yt_api_key, video_ids)

        # Create new database under page tổng: title = channel name
        new_db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)

        # Insert each video
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

            video_url = f"https://www.youtube.com/watch?v={vid}"
            views = views_map.get(vid, 0)

            notion_insert_video_row(
                notion_api_key=notion_api_key,
                database_id=new_db_id,
                title=title,
                video_url=video_url,
                published_at_iso=published,  # includes time => "giờ đăng"
                views=views,
                description=description,
                thumbnail_url=thumb_url
            )

            time.sleep(0.35)  # tránh Notion 429

        return jsonify({
            "status": "success",
            "message": "Created channel database under page tổng",
            "page_tong_id": page_tong_id,
            "channel_id": channel_id,
            "channel_title": channel_title,
            "new_database_id": new_db_id,
            "video_count": len(items)
        }), 200

    except Exception as e:
        print("Error:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500


# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 5000))
#     app.run(host="0.0.0.0", port=port)
