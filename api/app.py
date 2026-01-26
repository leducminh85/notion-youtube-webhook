from datetime import datetime
from flask import Flask, request, jsonify
import os
import logging
import requests
from .helpers.youtube import (
    youtube_channel_id_from_url,
    youtube_get_channel_stats,
    youtube_uploads_playlist_id,
    youtube_playlist_videos_basic,
    youtube_get_view_counts,
    get_upload_frequency,
)
from .helpers.notion import (
    calculate_monthly_views_gained,
    ensure_combined_monthly_stats_database,
    notion_headers,
    notion_get_database_schema,
    notion_update_page_properties,
    notion_create_database_under_page,
    insert_video_batch,
    notion_retrieve_page,
    notion_prop_text,
    get_page_tong_id_from_database,
    ensure_combined_daily_stats_database,
    sync_combined_daily_stats_rows,
    sync_combined_monthly_stats_rows,
)
from .helpers.vidiq import vidiq_fetch_data
from .utils import get_property_value

from concurrent.futures import ThreadPoolExecutor
from .helpers.youtube import youtube_get_video_comments
from .helpers.notion import ensure_child_page_exists, format_comment_blocks, append_blocks_to_page_safe


app = Flask(__name__)

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)


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

        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)
        channel_title = stats["title"]

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

        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id, limit=None)

        video_ids = [it["snippet"]["resourceId"]["videoId"] for it in items if it.get("snippet", {}).get("resourceId", {}).get("videoId")]
        views_map = youtube_get_view_counts(yt_api_key, video_ids)

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
            db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)
            should_clear_old = False
        else:
            db_id = existing_db_id
            should_clear_old = True

        if should_clear_old:
            pages_to_archive = []
            has_more = True
            next_cursor = None
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
                    should_clear_old = False
                    break
                res = r.json()
                for page in res.get("results", []):
                    if not page.get("archived", False):
                        pages_to_archive.append(page["id"])
                has_more = res.get("has_more", False)
                next_cursor = res.get("next_cursor")

            if pages_to_archive:
                from concurrent.futures import ThreadPoolExecutor
                def archive_page(page_id_archive):
                    import requests as _req
                    r_patch = _req.patch(
                        f"https://api.notion.com/v1/pages/{page_id_archive}",
                        headers=notion_headers(notion_api_key),
                        json={"archived": True}
                    )
                    if r_patch.status_code >= 300:
                        logger.warning("Failed to archive page %s: %s %s", page_id_archive, r_patch.status_code, r_patch.text)

                with ThreadPoolExecutor(max_workers=10) as executor:
                    executor.map(archive_page, pages_to_archive)

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
            insert_video_batch(notion_api_key, db_id, videos_data, max_workers=8)

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


@app.route("/update-channel-info", methods=["POST"])
def update_channel_only():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not yt_api_key or not notion_api_key:
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        data = payload.get("data", {})
        page_id = (data.get("id") or payload.get("page_id"))
        if not page_id:
            raise ValueError("Missing page_id (expected data.id or page_id)")

        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})

        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        triggering_db_id = page.get("parent", {}).get("database_id") or data.get("parent", {}).get("database_id")
        if not triggering_db_id:
            raise ValueError("Missing parent database_id (cannot identify Channels database)")

        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)

        schema = notion_get_database_schema(notion_api_key, triggering_db_id)

        OUT_TITLE = "Tên kênh"
        OUT_SUBS = "Subcriber"
        OUT_VIDEOS = "Số video"
        OUT_VIEWS = "Tổng view"
        OUT_FREQ = "Chu kì đăng video"

        update_props = {}
        if OUT_TITLE in schema and schema[OUT_TITLE] in ("title", "rich_text"):
            update_props[OUT_TITLE] = notion_prop_text(stats["title"], schema[OUT_TITLE])
        if OUT_FREQ in schema and schema[OUT_FREQ] in ("title", "rich_text"):
            update_props[OUT_FREQ] = notion_prop_text(freq, schema[OUT_FREQ])
        if OUT_SUBS in schema and schema[OUT_SUBS] == "number":
            update_props[OUT_SUBS] = {"number": stats["subscriberCount"]}
        if OUT_VIDEOS in schema and schema[OUT_VIDEOS] == "number":
            update_props[OUT_VIDEOS] = {"number": stats["videoCount"]}
        if OUT_VIEWS in schema and schema[OUT_VIEWS] == "number":
            update_props[OUT_VIEWS] = {"number": stats["viewCount"]}

        if not update_props:
            raise ValueError("No matching output properties found in database schema. Check column names/types.")

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

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        if not yt_api_key or not notion_api_key:
            raise ValueError("Missing env: YOUTUBE_API_KEY or NOTION_API_KEY")

        data = payload.get("data", {})
        page_id = data.get("id") or payload.get("page_id")
        if not page_id:
            raise ValueError("Missing page_id (expected data.id or page_id)")

        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})

        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        triggering_db_id = page.get("parent", {}).get("database_id")
        if not triggering_db_id:
            raise ValueError("Cannot find parent database_id of the channel row")

        page_tong_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)

        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        stats = youtube_get_channel_stats(yt_api_key, channel_id)
        freq = get_upload_frequency(yt_api_key, channel_id)
        channel_title = stats["title"]

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

        uploads_id = youtube_uploads_playlist_id(yt_api_key, channel_id)
        items = youtube_playlist_videos_basic(yt_api_key, uploads_id)

        video_ids = [it["snippet"]["resourceId"]["videoId"] for it in items if it.get("snippet", {}).get("resourceId", {}).get("videoId")]
        views_map = youtube_get_view_counts(yt_api_key, video_ids)

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
                        db_title += part.get("text", {}).get("content", "")
                    elif isinstance(part, str):
                        db_title += part
                if db_title.strip() == channel_title.strip():
                    existing_db_id = child["id"]
                    break

        if not existing_db_id:
            new_db_id = notion_create_database_under_page(notion_api_key, page_tong_id, channel_title)
            videos_to_insert = items
            insert_mode = "full"
        else:
            new_db_id = existing_db_id
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

            videos_to_insert = []
            for it in items:
                vid = it.get("snippet", {}).get("resourceId", {}).get("videoId")
                if not vid:
                    continue
                video_url = f"https://www.youtube.com/watch?v={vid}"
                if video_url not in existing_urls:
                    videos_to_insert.append(it)

            insert_mode = "incremental"

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

        if videos_data:
            insert_video_batch(notion_api_key, new_db_id, videos_data, max_workers=6)

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


@app.route("/get-channel-views-monthly", methods=["POST"])
def get_channel_views_monthly():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid or missing JSON"}), 400

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        vidiq_token = os.environ.get("VIDIQ_BEARER_TOKEN")
        if not yt_api_key or not notion_api_key or not vidiq_token:
            raise ValueError("Missing env: YOUTUBE_API_KEY, NOTION_API_KEY or VIDIQ_BEARER_TOKEN")

        data = payload.get("data", {})
        page_id = data.get("id") or payload.get("page_id")
        if not page_id:
            raise ValueError("Missing page_id")

        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})
        channel_url = get_property_value(props, "Channel URL")
        if not channel_url:
            raise ValueError("Missing property: Channel URL")

        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)
        channel_stats = youtube_get_channel_stats(yt_api_key, channel_id)
        channel_title = channel_stats.get("title", "Unknown Channel")
        views_30_days, daily_stats_list, monthly_stats_list = vidiq_fetch_data(channel_id)  # Thêm monthly

        # === Cập nhật Views (30 ngày) trên page channel ===
        triggering_db_id = page.get("parent", {}).get("database_id")
        if triggering_db_id:
            schema = notion_get_database_schema(notion_api_key, triggering_db_id)
            OUT_VIEWS_MONTHLY = "Views (30 ngày)"
            update_props = {}
            if OUT_VIEWS_MONTHLY in schema and schema[OUT_VIEWS_MONTHLY] == "number":
                update_props[OUT_VIEWS_MONTHLY] = {"number": views_30_days}
            if update_props:
                notion_update_page_properties(notion_api_key, page_id, update_props)

        # === Đồng bộ Daily và Monthly ===
        inserted_daily = 0
        inserted_monthly = 0

        if daily_stats_list or monthly_stats_list:
            parent_page_id = get_page_tong_id_from_database(notion_api_key, triggering_db_id)

            # Daily
            combined_daily_db_id = ensure_combined_daily_stats_database(notion_api_key, parent_page_id)
            inserted_daily = sync_combined_daily_stats_rows(
                notion_api_key, combined_daily_db_id, channel_title, daily_stats_list
            )

            # Monthly
            combined_monthly_db_id = ensure_combined_monthly_stats_database(notion_api_key, parent_page_id)
            inserted_monthly = sync_combined_monthly_stats_rows(
                notion_api_key, combined_monthly_db_id, channel_title, monthly_stats_list[:24]  # Limit 24 tháng
            )

        return jsonify({
            "status": "success",
            "page_id": page_id,
            "channel_id": channel_id,
            "channel_name": channel_title,
            "views_30_days": views_30_days,
            "monthly_stats_count": len(monthly_stats_list),
            "combined_daily_inserted": inserted_daily,
            "combined_monthly_inserted": inserted_monthly,
            "latest_month_gained": monthly_stats_list[0]["views_gained"] if monthly_stats_list else None
        }), 200

    except Exception as e:
        logger.exception(f"/get-channel-views-monthly failed: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

comment_executor = ThreadPoolExecutor(max_workers=4)

def process_single_video_comments(video, yt_key, notion_key, repo_page_id):
    """Hàm worker xử lý 1 video riêng biệt"""
    try:
        snip = video.get("snippet", {})
        v_id = snip.get("resourceId", {}).get("videoId")
        v_title = snip.get("title", "No Title")

        if not v_id: 
            return 0

        # 1. Lấy comments từ YouTube (IO Bound)
        # Giảm max_results xuống một chút nếu muốn nhanh hơn, hoặc giữ nguyên
        comments = youtube_get_video_comments(yt_key, v_id, max_results=None)

        if not comments:
            return 0

        # Sort theo like để comment chất lượng lên đầu
        comments.sort(key=lambda c: c.get("likes", 0), reverse=True)

        # 2. Format dữ liệu (CPU Bound - rất nhanh)
        blocks = format_comment_blocks(v_title, f"https://youtu.be/{v_id}", comments)

        # 3. Đẩy lên Notion (IO Bound - Chậm nhất)
        success = append_blocks_to_page_safe(notion_key, repo_page_id, blocks)
        
        if success:
            logger.info(f"✅ Saved comments for: {v_title}")
            return 1
        return 0

    except Exception as e:
        logger.error(f"❌ Error processing video {video.get('snippet', {}).get('title')}: {e}")
        return 0

def task_fetch_comments(yt_key, notion_key, channel_id, parent_page_id):
    logger.info(f"🚀 [START] Background task fetch comments for Channel ID: {channel_id}")
    
    try:
        # 1. Chuẩn bị Page Notion
        repo_page_id = ensure_child_page_exists(notion_key, parent_page_id, "💬 Comments Repository")
        
        # Header Log thời gian
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header_block = [{
            "object": "block",
            "type": "heading_3",
            "heading_3": {
                "rich_text": [{"type": "text", "text": {"content": f"Update Batch: {update_time}", "annotations": {"color": "gray"}}}]
            }
        }]
        append_blocks_to_page_safe(notion_key, repo_page_id, header_block)

        # 2. Lấy danh sách video
        uploads_id = youtube_uploads_playlist_id(yt_key, channel_id)
        logger.info("... Fetching video list ...")
        videos = youtube_playlist_videos_basic(yt_key, uploads_id, limit=None) # Lấy toàn bộ
        logger.info(f"✅ Found {len(videos)} videos. Starting parallel processing...")

        # 3. CHẠY SONG SONG (Multithreading)
        total_success = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Tạo danh sách các task
            futures = [
                executor.submit(process_single_video_comments, vid, yt_key, notion_key, repo_page_id) 
                for vid in videos
            ]
            
            # Theo dõi tiến độ
            for i, future in enumerate(futures):
                try:
                    result = future.result() # Chờ task hoàn thành
                    total_success += result
                    if i % 10 == 0:
                        logger.info(f"Creating progress: {i}/{len(videos)} videos processed...")
                except Exception as e:
                    logger.error(f"Worker exception: {e}")

        logger.info(f"🏁 [END] Finished. Successfully updated {total_success}/{len(videos)} videos.")

    except Exception as e:
        logger.exception(f"❌ CRITICAL ERROR in task_fetch_comments: {e}")

@app.route("/fetch-channel-comments", methods=["POST"])
def fetch_channel_comments():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    try:
        yt_api_key = os.environ.get("YOUTUBE_API_KEY")
        notion_api_key = os.environ.get("NOTION_API_KEY")
        
        # Lấy page_id từ payload (Notion Automation gửi id của page channel)
        # Cấu trúc payload tùy thuộc vào cách bạn trigger, thường là {"page_id": "..."} hoặc {"data": {"id": "..."}}
        data = payload.get("data", {})
        page_id = data.get("id") or payload.get("page_id")

        if not page_id:
            return jsonify({"status": "error", "message": "Missing page_id"}), 400

        # Lấy thông tin Page để tìm URL kênh -> tìm Channel ID
        page = notion_retrieve_page(notion_api_key, page_id)
        props = page.get("properties", {})
        channel_url = get_property_value(props, "Channel URL")
        
        if not channel_url:
            return jsonify({"status": "error", "message": "Channel URL empty"}), 400

        channel_id = youtube_channel_id_from_url(yt_api_key, channel_url)

        # Đẩy vào background chạy để trả response ngay cho Notion đỡ đợi
        comment_executor.submit(task_fetch_comments, yt_api_key, notion_api_key, channel_id, page_id)

        return jsonify({
            "status": "success", 
            "message": "Processing comments in background. Check '💬 Comments Repository' page shortly."
        }), 200

    except Exception as e:
        logger.exception(f"Endpoint error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
