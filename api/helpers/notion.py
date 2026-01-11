import time
import logging
from typing import Dict, List, Optional
import requests
from ..config import NOTION_VERSION
from ..utils import safe_json, get_property_value
from datetime import datetime
from collections import defaultdict


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
	from concurrent.futures import ThreadPoolExecutor, as_completed

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


def sync_daily_stats_rows(notion_api_key: str, db_id: str, daily_stats: List[dict]):
	existing_dates = set()
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
		if r.status_code != 200:
			logger.error(f"Query Daily DB Error: {r.text}")
			break

		res = r.json()
		for page in res.get("results", []):
			props = page.get("properties", {})
			date_prop = props.get("Date", {})
			title_parts = date_prop.get("title", [])
			if title_parts:
				content = "".join([t.get("text", {}).get("content", "") for t in title_parts])
				if content:
					existing_dates.add(content.strip())

		has_more = res.get("has_more", False)
		next_cursor = res.get("next_cursor")

	to_insert = []
	for stat in daily_stats:
		ts = stat.get("date")
		if not ts:
			continue
		dt_str = __import__("datetime").datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
		if dt_str not in existing_dates:
			to_insert.append({
				"date_str": dt_str,
				"views": stat.get("views"),
				"views_change": stat.get("views_change"),
				"subs": stat.get("subscribers"),
				"subs_change": stat.get("subscribers_change")
			})

	if not to_insert:
		logger.info(f"Daily Stats: Không có dữ liệu mới cho DB {db_id}")
		return 0

	def insert_row(item):
		props = {
			"Date": {"title": [{"text": {"content": item["date_str"]}}]},
			"Views": {"number": item["views"]},
			"Views Change": {"number": item["views_change"]},
			"Subscribers": {"number": item["subs"]},
			"Subscribers Change": {"number": item["subs_change"]},
		}
		r_ins = requests.post(
			"https://api.notion.com/v1/pages",
			headers=notion_headers(notion_api_key),
			json={"parent": {"database_id": db_id}, "properties": props}
		)
		if r_ins.status_code >= 300:
			logger.error(f"Insert Daily Row Error: {r_ins.text}")

	from concurrent.futures import ThreadPoolExecutor
	with ThreadPoolExecutor(max_workers=5) as executor:
		executor.map(insert_row, to_insert)

	logger.info(f"Đã insert {len(to_insert)} dòng vào Daily Stats DB {db_id}")
	return len(to_insert)

# Thêm vào cuối file notion.py

def ensure_combined_daily_stats_database(notion_api_key: str, parent_page_id: str) -> str:
    """
    Tìm hoặc tạo database "Combined Daily Stats" dưới parent_page_id
    """
    r = requests.get(
        f"https://api.notion.com/v1/blocks/{parent_page_id}/children",
        headers=notion_headers(notion_api_key),
        params={"page_size": 100}
    )
    if r.status_code == 200:
        results = r.json().get("results", [])
        for block in results:
            if block.get("type") == "child_database":
                # Lấy title an toàn hơn: hỗ trợ cả list dict và list string
                title_parts = block.get("child_database", {}).get("title", [])
                title = ""
                for part in title_parts:
                    if isinstance(part, dict):
                        title += part.get("plain_text", "") or part.get("text", {}).get("content", "")
                    elif isinstance(part, str):
                        title += part
                if title.strip() == "Combined Daily Stats":
                    return block["id"]

    # Nếu chưa có thì tạo mới
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
    Cập nhật daily stats cho một channel vào Combined Daily Stats:
    - Xóa hết các row cũ của channel đó trước.
    - Chỉ insert 30 ngày gần nhất từ dữ liệu mới.
    """
    channel_name = channel_name.strip()

    # === Bước 1: Xóa tất cả các row cũ của channel này ===
    has_more = True
    next_cursor = None
    deleted_count = 0

    while has_more:
        query_payload = {
            "page_size": 100,
            "filter": {
                "property": "Channel",
                "title": {
                    "equals": channel_name
                }
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
            logger.error(f"Query to delete old rows failed: {r.text}")
            break

        res = r.json()
        pages = res.get("results", [])

        # Xóa từng page (Notion không hỗ trợ bulk delete, phải xóa từng cái)
        for page in pages:
            page_id = page["id"]
            # Archive (soft delete) thay vì delete cứng để an toàn
            archive_r = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=notion_headers(notion_api_key),
                json={"archived": True}
            )
            if archive_r.status_code == 200:
                deleted_count += 1
            else:
                logger.warning(f"Failed to archive old row {page_id}: {archive_r.text}")

        has_more = res.get("has_more", False)
        next_cursor = res.get("next_cursor")

    logger.info(f"Đã xóa/archived {deleted_count} dòng cũ của channel '{channel_name}'")

    # === Bước 2: Chỉ lấy 30 ngày gần nhất từ daily_stats ===
    if not daily_stats:
        logger.info(f"Không có daily stats để insert cho {channel_name}")
        return 0

    # Sắp xếp theo date giảm dần (mới nhất trước)
    sorted_stats = sorted(daily_stats, key=lambda x: x.get("date", 0), reverse=True)
    recent_30_days = sorted_stats[:30]

    if len(recent_30_days) < len(sorted_stats):
        logger.info(f"Chỉ lấy 30 ngày gần nhất (bỏ qua {len(sorted_stats) - 30} ngày cũ hơn)")

    # === Bước 3: Insert dữ liệu mới ===
    to_insert = []
    for stat in recent_30_days:
        ts = stat.get("date")
        if not ts:
            continue
        dt_str = __import__("datetime").datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')

        to_insert.append({
            "channel": channel_name,
            "date_str": dt_str,
            "subscribers": stat.get("subscribers"),
            "total_views": stat.get("views"),
            "views_change": stat.get("views_change"),
        })

    if not to_insert:
        logger.info(f"Không có dữ liệu hợp lệ để insert cho {channel_name}")
        return 0

    def insert_row(item: dict):
        props = {
            "Channel": {"title": [{"text": {"content": item["channel"]}}]},
            "Date": {"date": {"start": item["date_str"]}},
            "Subscribers": {"number": item["subscribers"] if item["subscribers"] is not None else None},
            "Total Views": {"number": item["total_views"] if item["total_views"] is not None else None},
            "Views Change": {"number": item["views_change"] if item["views_change"] is not None else None},
        }
        r_ins = requests.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(notion_api_key),
            json={"parent": {"database_id": db_id}, "properties": props}
        )
        if r_ins.status_code >= 300:
            logger.error(f"Insert Combined Row Error: {r_ins.text}")

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(insert_row, to_insert)

    logger.info(f"Đã insert {len(to_insert)} dòng (30 ngày gần nhất) cho {channel_name}")
    return len(to_insert)




def calculate_monthly_views_gained(daily_stats: List[dict]) -> List[dict]:
    """
    Từ daily_stats của VidIQ, tính tổng views gained theo từng tháng.
    Trả về list các dict: {month_str: '2025-01', views_gained: 1234567}
    Chỉ lấy các tháng có dữ liệu đầy đủ (hoặc gần đầy).
    """
    if not daily_stats:
        return []

    # Sắp xếp daily_stats theo date tăng dần (cũ → mới)
    sorted_stats = sorted(daily_stats, key=lambda x: x.get("date", 0))

    monthly_gained = defaultdict(int)
    monthly_total_views = {}  # để lưu total views cuối tháng

    for stat in sorted_stats:
        ts = stat.get("date")
        if not ts:
            continue
        dt = datetime.utcfromtimestamp(ts)
        month_key = dt.strftime('%Y-%m')  # ví dụ: "2025-01"

        views_change = stat.get("views_change", 0)
        if views_change > 0:
            monthly_gained[month_key] += views_change

        # Cập nhật total views cuối cùng của tháng
        monthly_total_views[month_key] = stat.get("views", 0)

    # Chuyển sang list và sort theo tháng mới nhất trước
    result = []
    for month_key in sorted(monthly_gained.keys(), reverse=True):
        result.append({
            "month": month_key,                    # '2025-01'
            "views_gained": monthly_gained[month_key],
            "total_views_at_end": monthly_total_views.get(month_key, 0)
        })

    # Chỉ lấy 24 tháng gần nhất (đủ để biểu đồ đẹp mà không quá dài)
    return result[:24]


def ensure_combined_monthly_stats_database(notion_api_key: str, parent_page_id: str) -> str:
    """
    Tìm hoặc tạo database "Combined Monthly Stats" dưới parent_page_id
    """
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

    # Tạo mới nếu chưa có
    payload = {
        "parent": {"type": "page_id", "page_id": parent_page_id},
        "title": [{"type": "text", "text": {"content": "Combined Monthly Stats"}}],
        "properties": {
            "Channel": {"title": {}},
            "Month": {"date": {}},                    # dùng date để dễ sort và filter
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
    Đồng bộ monthly stats cho một kênh vào Combined Monthly Stats DB
    - Xóa hết row cũ của channel đó
    - Insert các tháng gần nhất (từ monthly_stats)
    """
    channel_name = channel_name.strip()

    # === Xóa row cũ của channel ===
    has_more = True
    next_cursor = None
    deleted_count = 0

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
            logger.error(f"Query monthly delete failed: {r.text}")
            break

        res = r.json()
        for page in res.get("results", []):
            page_id = page["id"]
            archive_r = requests.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=notion_headers(notion_api_key),
                json={"archived": True}
            )
            if archive_r.status_code == 200:
                deleted_count += 1

        has_more = res.get("has_more", False)
        next_cursor = res.get("next_cursor")

    logger.info(f"Đã xóa {deleted_count} dòng monthly cũ của '{channel_name}'")

    # === Insert dữ liệu mới ===
    if not monthly_stats:
        return 0

    def insert_row(item: dict):
        # Month format: "2025-01" → dùng ngày đầu tháng làm date
        year, month = item["month"].split("-")
        date_start = f"{year}-{month}-01"

        props = {
            "Channel": {"title": [{"text": {"content": channel_name}}]},
            "Month": {"date": {"start": date_start}},
            "Views Gained": {"number": item["views_gained"]},
            "Total Views": {"number": item["total_views_at_end"]},
        }
        r_ins = requests.post(
            "https://api.notion.com/v1/pages",
            headers=notion_headers(notion_api_key),
            json={"parent": {"database_id": db_id}, "properties": props}
        )
        if r_ins.status_code >= 300:
            logger.error(f"Insert Monthly Row Error: {r_ins.text}")

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(insert_row, monthly_stats)

    logger.info(f"Đã insert {len(monthly_stats)} dòng monthly cho '{channel_name}'")
    return len(monthly_stats)