import datetime
import os
import logging
from typing import List, Tuple
from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)


def vidiq_get_30_day_views(channel_id: str) -> int:
	vidiq_token = os.environ.get("VIDIQ_BEARER_TOKEN")
	url = f"https://api.vidiq.com/youtube/channels/public/channel-pages/{channel_id}"
	headers = {
		"accept": "*/*",
		"accept-language": "vi,en;q=0.9,vi-VN;q=0.8",
		"authorization": f"Bearer {vidiq_token}",
		"content-type": "application/json",
		"user-agent": "Mozilla/5.0",
		"x-vidiq-client": "ext vch/3.168.0"
	}

	try:
		r = cffi_requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
		if r.status_code >= 300:
			logger.error(f"VidIQ API Error: {r.status_code} {getattr(r, 'text', '')[:200]}")
			return 0

		data = r.json()
		current_stats = data.get("current_stats", {}).get("views", {})
		daily_stats = data.get("daily_stats", [])

		if not daily_stats or len(daily_stats) < 2:
			logger.warning("VidIQ: Không đủ dữ liệu daily_stats để tính toán")
			return 0

		current_total_views = current_stats.get("count", 0)
		yesterday_total_views = daily_stats[1].get("views", 0)
		views_today_realtime = max(0, current_total_views - yesterday_total_views)
		past_days_stats = daily_stats[1:30]
		views_past_29_days = sum(day.get("views_change", 0) for day in past_days_stats)

		total_30_days = views_today_realtime + views_past_29_days
		logger.info(f"VidIQ calc for {channel_id}: {total_30_days}")
		return total_30_days

	except Exception as e:
		logger.exception(f"Lỗi khi tính toán view VidIQ cho {channel_id}: {e}")
		return 0


def vidiq_fetch_data(channel_id: str) -> Tuple[int, List[dict], List[dict]]:
    vidiq_token = os.environ.get("VIDIQ_BEARER_TOKEN")
    url = f"https://api.vidiq.com/youtube/channels/public/channel-pages/{channel_id}"
    headers = {
        "accept": "*/*",
        "authorization": f"Bearer {vidiq_token}",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",
        "x-vidiq-client": "ext vch/3.168.0"
    }

    try:
        r = cffi_requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
        if r.status_code >= 300:
            logger.error(f"VidIQ API Error: {r.status_code} {getattr(r, 'text', '')[:200]}")
            return 0, [], []

        data = r.json()
        daily_stats = data.get("daily_stats", [])
        monthly_stats_raw = data.get("monthly_stats", [])  # Lấy monthly_stats
        current_stats = data.get("current_stats", {}).get("views", {})

        if not daily_stats or len(daily_stats) < 2:
            return 0, [], []

        current_total_views = current_stats.get("count", 0)
        yesterday_total_views = daily_stats[1].get("views", 0)
        views_today_realtime = max(0, current_total_views - yesterday_total_views)
        past_days_stats = daily_stats[1:30]
        views_past_29_days = sum(day.get("views_change", 0) for day in past_days_stats)
        total_30_days = views_today_realtime + views_past_29_days

        # Format monthly_stats thành list dict (sắp xếp mới nhất trước)
        monthly_stats = []
        for stat in monthly_stats_raw:
            ts = stat.get("date")
            if ts:
                dt = datetime.datetime.utcfromtimestamp(ts)
                monthly_stats.append({
                    "month": dt.strftime('%Y-%m'),  # '2026-01'
                    "views_gained": stat.get("views_change", 0),
                    "total_views_at_end": stat.get("views", 0),
                    "subscribers": stat.get("subscribers", 0),  # Bonus: nếu cần subs
                    "subscribers_change": stat.get("subscribers_change", 0)
                })
        logger.info(f"VidIQ fetch for {channel_id}: {daily_stats}")
        return total_30_days, daily_stats, monthly_stats

    except Exception as e:
        logger.exception(f"Lỗi khi fetch VidIQ cho {channel_id}: {e}")
        return 0, [], []