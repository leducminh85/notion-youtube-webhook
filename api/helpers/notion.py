import time
import logging
from typing import Dict, List, Optional
import requests
from ..config import NOTION_VERSION
from ..utils import safe_json, get_property_value
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def notion_headers(notion_api_key: str) -> dict:
	return {
		"Authorization": f"Bearer {notion_api_key}",
		"Content-Type": "application/json",
		"Notion-Version": NOTION_VERSION
	}


def get_page_tong_id_from_database(notion_api_key: str, database_id: str) -> str:
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
	value = (value or "").strip()
	if prop_type == "title":
		return {"title": [{"type": "text", "text": {"content": value}}]} if value else {"title": []}
	return {"rich_text": [{"type": "text", "text": {"content": value}}]} if value else {"rich_text": []}


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
	max_workers: int = 8
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
					retry_after = 2
					logger.warning("insert_single: rate limited for video %s; sleeping %ss (attempt %s)", video.get("video_url"), retry_after, retries+1)
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


def notion_retrieve_page(notion_api_key: str, page_id: str) -> dict:
	r = requests.get(
		f"https://api.notion.com/v1/pages/{page_id}",
		headers=notion_headers(notion_api_key),
	)
	if r.status_code >= 300:
		raise ValueError(f"Retrieve page failed: {r.status_code} {r.text}")
	return r.json()


# === Helpers cho logic Update/Insert song song ===

def _execute_batch_actions(notion_api_key: str, to_update: list, to_insert: list, to_delete: list):
	"""
	Hàm helper thực thi song song các hành động: Update, Insert, Delete (Archive).
	"""
	workers = 20
	total_ops = len(to_update) + len(to_insert) + len(to_delete)
	if total_ops == 0:
		return

	def do_update(item):
		# item = (page_id, props)
		pid, props = item
		try:
			r = requests.patch(
				f"https://api.notion.com/v1/pages/{pid}",
				headers=notion_headers(notion_api_key),
				json={"properties": props}
			)
			if r.status_code >= 300:
				logger.warning(f"Failed update page {pid}: {r.text}")
		except Exception as e:
			logger.error(f"Ex update {pid}: {e}")

	def do_insert(item):
		# item = (db_id, props)
		db_id, props = item
		try:
			r = requests.post(
				"https://api.notion.com/v1/pages",
				headers=notion_headers(notion_api_key),
				json={"parent": {"database_id": db_id}, "properties": props}
			)
			if r.status_code >= 300:
				logger.warning(f"Failed insert: {r.text}")
		except Exception as e:
			logger.error(f"Ex insert: {e}")

	def do_delete(pid):
		try:
			requests.patch(
				f"https://api.notion.com/v1/pages/{pid}",
				headers=notion_headers(notion_api_key),
				json={"archived": True}
			)
		except Exception as e:
			logger.error(f"Ex delete {pid}: {e}")

	with ThreadPoolExecutor(max_workers=workers) as executor:
		# Submit updates
		for item in to_update:
			executor.submit(do_update, item)
		# Submit inserts
		for item in to_insert:
			executor.submit(do_insert, item)
		# Submit deletes
		for pid in to_delete:
			executor.submit(do_delete, pid)


def ensure_daily_stats_database(notion_api_key: str, parent_page_id: str) -> str:
	r = requests.get(
		f"https://api.notion.com/v1/blocks/{parent_page_id}/children",
		headers=notion_headers(notion_api_key),
		params={"page_size": 100}
	)
	if r.status_code == 200:
		results = r.json().get("results", [])
		for block in results:
			if block.get("type") == "child_database":
				title = block.get("child_database", {}).get("title", "")
				if title == "Daily Stats":
					return block["id"]

	payload = {
		"parent": {"type": "page_id", "page_id": parent_page_id},
		"title": [{"type": "text", "text": {"content": "Daily Stats"}}],
		"properties": {
			"Date": {"title": {}},
			"Views": {"number": {"format": "number"}},
			"Views Change": {"number": {"format": "number"}},
			"Subscribers": {"number": {"format": "number"}},
			"Subscribers Change": {"number": {"format": "number"}}
		}
	}
	r = requests.post(
		"https://api.notion.com/v1/databases",
		headers=notion_headers(notion_api_key),
		json=payload
	)
	if r.status_code >= 300:
		logger.error(f"Create DB Error: {r.text}")
		raise ValueError(f"Không thể tạo Daily Stats DB: {r.text}")

	return r.json()["id"]


def ensure_combined_daily_stats_database(notion_api_key: str, parent_page_id: str) -> str:
    r = requests.get(
        f"https://api.notion.com/v1/blocks/{parent_page_id}/children",
        headers=notion_headers(notion_api_key),
        params={"page_size": 100}
    )
    if r.status_code == 200:
        results = r.json().get("results", [])
        for block in results:
            if block.get("type") == "child_database":
                title_parts = block.get("child_database", {}).get("title", [])
                title = ""
                for part in title_parts:
                    if isinstance(part, dict):
                        title += part.get("plain_text", "") or part.get("text", {}).get("content", "")
                    elif isinstance(part, str):
                        title += part
                if title.strip() == "Combined Daily Stats":
                    return block["id"]

    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": "Combined Daily Stats"}}],
        "properties": {
            "Channel": {"title": {}},
            "Date": {"date": {}},
            "Subscribers": {"number": {"format": "number"}},
            "Total Views": {"number": {"format": "number"}},
            "Views Change": {"number": {"format": "number"}},
        }
    }
    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(notion_api_key),
        json=payload
    )
    if r.status_code >= 300:
        logger.error(f"Create Combined Daily Stats DB Error: {r.text}")
        raise ValueError(f"Không thể tạo Combined Daily Stats DB: {r.text}")

    logger.info("Đã tạo mới Combined Daily Stats database")
    return r.json()["id"]


def sync_combined_daily_stats_rows(
    notion_api_key: str,
    db_id: str,
    channel_name: str,
    daily_stats: List[dict]
) -> int:
    """
    Tối ưu: 
    1. Query lấy danh sách row hiện tại (key = Date).
    2. So khớp với dữ liệu mới (VidIQ).
    3. Update nếu tồn tại, Insert nếu chưa có, Delete nếu dư thừa (quá cũ).
    """
    channel_name = channel_name.strip()
    if not daily_stats:
        return 0

    # 1. Lấy dữ liệu hiện có từ Notion
    existing_map = {}  # "2025-01-01" -> page_id
    has_more = True
    next_cursor = None
    
    while has_more:
        query_payload = {
            "page_size": 100,
            "filter": {
                "property": "Channel",
                "title": {"equals": channel_name}
            }
        }
        if next_cursor:
            query_payload["start_cursor"] = next_cursor

        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=notion_headers(notion_api_key),
            json=query_payload
        )
        if r.status_code != 200:
            logger.error(f"Query daily existing failed: {r.text}")
            break
        
        res = r.json()
        for page in res.get("results", []):
            # Lấy Date từ property
            props = page.get("properties", {})
            date_prop = props.get("Date", {})
            date_val = date_prop.get("date", {})
            if date_val:
                d_str = date_val.get("start") # "2025-01-01"
                if d_str:
                    existing_map[d_str] = page["id"]

        has_more = res.get("has_more", False)
        next_cursor = res.get("next_cursor")

    # 2. Chuẩn bị lists Update/Insert
    to_update = []
    to_insert = []
    
    # Chỉ lấy 30 ngày gần nhất để sync
    sorted_stats = sorted(daily_stats, key=lambda x: x.get("date", 0), reverse=True)
    recent_30_days = sorted_stats[:30]
    
    processed_dates = set()

    for stat in recent_30_days:
        ts = stat.get("date")
        if not ts:
            continue
        dt_str = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
        
        processed_dates.add(dt_str)
        
        row_props = {
            "Channel": {"title": [{"text": {"content": channel_name}}]},
            "Date": {"date": {"start": dt_str}},
            "Subscribers": {"number": stat.get("subscribers")},
            "Total Views": {"number": stat.get("views")},
            "Views Change": {"number": stat.get("views_change")},
        }

        if dt_str in existing_map:
            # Row đã tồn tại -> Update
            page_id = existing_map[dt_str]
            to_update.append((page_id, row_props))
        else:
            # Row chưa tồn tại -> Insert
            to_insert.append((db_id, row_props))

    # 3. Tìm các row cũ không còn trong 30 ngày này để xóa (Cleanup)
    to_delete = []
    for date_str, page_id in existing_map.items():
        if date_str not in processed_dates:
            to_delete.append(page_id)

    # 4. Thực thi song song
    logger.info(f"Daily Sync '{channel_name}': {len(to_update)} updates, {len(to_insert)} inserts, {len(to_delete)} deletes.")
    _execute_batch_actions(notion_api_key, to_update, to_insert, to_delete)
    
    return len(to_update) + len(to_insert)


def calculate_monthly_views_gained(daily_stats: List[dict]) -> List[dict]:
    if not daily_stats:
        return []

    sorted_stats = sorted(daily_stats, key=lambda x: x.get("date", 0))
    monthly_gained = defaultdict(int)
    monthly_total_views = {}

    for stat in sorted_stats:
        ts = stat.get("date")
        if not ts:
            continue
        dt = datetime.utcfromtimestamp(ts)
        month_key = dt.strftime('%Y-%m')

        views_change = stat.get("views_change", 0)
        if views_change > 0:
            monthly_gained[month_key] += views_change

        monthly_total_views[month_key] = stat.get("views", 0)

    result = []
    for month_key in sorted(monthly_gained.keys(), reverse=True):
        result.append({
            "month": month_key,
            "views_gained": monthly_gained[month_key],
            "total_views_at_end": monthly_total_views.get(month_key, 0)
        })

    return result[:24]


def ensure_combined_monthly_stats_database(notion_api_key: str, parent_page_id: str) -> str:
    r = requests.get(
        f"https://api.notion.com/v1/blocks/{parent_page_id}/children",
        headers=notion_headers(notion_api_key),
        params={"page_size": 100}
    )
    if r.status_code == 200:
        results = r.json().get("results", [])
        for block in results:
            if block.get("type") == "child_database":
                title_parts = block.get("child_database", {}).get("title", [])
                title = "".join(
                    part.get("plain_text", "") or part.get("text", {}).get("content", "") 
                    if isinstance(part, dict) else part
                    for part in title_parts
                )
                if title.strip() == "Combined Monthly Stats":
                    return block["id"]

    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": "Combined Monthly Stats"}}],
        "properties": {
            "Channel": {"title": {}},
            "Month": {"date": {}},
            "Views Gained": {"number": {"format": "number"}},
            "Total Views": {"number": {"format": "number"}},
        }
    }
    r = requests.post(
        "https://api.notion.com/v1/databases",
        headers=notion_headers(notion_api_key),
        json=payload
    )
    if r.status_code >= 300:
        logger.error(f"Create Combined Monthly Stats DB Error: {r.text}")
        raise ValueError(f"Không thể tạo Combined Monthly Stats DB: {r.text}")

    logger.info("Đã tạo mới Combined Monthly Stats database")
    return r.json()["id"]


def sync_combined_monthly_stats_rows(
    notion_api_key: str,
    db_id: str,
    channel_name: str,
    monthly_stats: List[dict]
) -> int:
    """
    Tối ưu Monthly Sync: Upsert (Update existing, Insert new).
    """
    channel_name = channel_name.strip()
    if not monthly_stats:
        return 0

    # 1. Lấy dữ liệu hiện có
    existing_map = {} # "2025-01-01" -> page_id
    has_more = True
    next_cursor = None

    while has_more:
        query_payload = {
            "page_size": 100,
            "filter": {
                "property": "Channel",
                "title": {"equals": channel_name}
            }
        }
        if next_cursor:
            query_payload["start_cursor"] = next_cursor

        r = requests.post(
            f"https://api.notion.com/v1/databases/{db_id}/query",
            headers=notion_headers(notion_api_key),
            json=query_payload
        )
        if r.status_code != 200:
            logger.error(f"Query monthly existing failed: {r.text}")
            break

        res = r.json()
        for page in res.get("results", []):
            props = page.get("properties", {})
            date_prop = props.get("Month", {})
            date_val = date_prop.get("date", {})
            if date_val:
                d_str = date_val.get("start")
                if d_str:
                    existing_map[d_str] = page["id"]

        has_more = res.get("has_more", False)
        next_cursor = res.get("next_cursor")

    # 2. Phân loại Update/Insert
    to_update = []
    to_insert = []
    processed_months = set()

    for item in monthly_stats:
        # item['month'] = "2025-01" -> Cần chuyển thành "2025-01-01" để lưu vào Notion Date
        year, month = item["month"].split("-")
        date_start = f"{year}-{month}-01"
        processed_months.add(date_start)

        row_props = {
            "Channel": {"title": [{"text": {"content": channel_name}}]},
            "Month": {"date": {"start": date_start}},
            "Views Gained": {"number": item["views_gained"]},
            "Total Views": {"number": item["total_views_at_end"]},
        }

        if date_start in existing_map:
            to_update.append((existing_map[date_start], row_props))
        else:
            to_insert.append((db_id, row_props))

    # 3. Cleanup (Xóa các tháng cũ không còn trong list trả về - optional, nhưng tốt cho sync)
    to_delete = []
    for d_str, pid in existing_map.items():
        if d_str not in processed_months:
            to_delete.append(pid)

    # 4. Thực thi song song
    logger.info(f"Monthly Sync '{channel_name}': {len(to_update)} updates, {len(to_insert)} inserts.")
    _execute_batch_actions(notion_api_key, to_update, to_insert, to_delete)

    return len(to_update) + len(to_insert)