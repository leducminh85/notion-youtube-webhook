import time
import logging
from typing import Dict, List, Optional
import requests
from ..config import NOTION_VERSION
from ..utils import safe_json, get_property_value

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

