import os
import logging
from datetime import datetime, timedelta
from typing import List, Tuple, Dict
from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

def vidiq_fetch_data(channel_id: str) -> Tuple[int, List[dict], List[dict]]:
    vidiq_token = os.environ.get("VIDIQ_BEARER_TOKEN")
    if not vidiq_token:
        logger.error("Missing VIDIQ_BEARER_TOKEN")
        return 0, [], []

    url = f"https://api.vidiq.com/youtube/channels/public/channel-pages/{channel_id}"
    headers = {
        "accept": "*/*",
        "authorization": f"Bearer {vidiq_token}",
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",
        "x-vidiq-client": "ext vch/3.168.0"
    }

    try:
        # Thêm verify=False nếu gặp lỗi SSL, nhưng mặc định nên để True/Default
        r = cffi_requests.get(url, headers=headers, impersonate="chrome110", timeout=30)
        
        if r.status_code >= 300:
            logger.error(f"VidIQ API Error: {r.status_code} {getattr(r, 'text', '')[:200]}")
            return 0, [], []

        data = r.json()
        daily_stats = data.get("daily_stats", [])
        monthly_stats_raw = data.get("monthly_stats", [])
        
        # Lấy current stats an toàn hơn
        current_stats_obj = data.get("current_stats", {})
        current_views_obj = current_stats_obj.get("views", {})
        current_total_views = current_views_obj.get("count", 0)
        current_subs_count = current_stats_obj.get("subscribers", {}).get("count", 0)

        # --- Xử lý 30 Day Views ---
        if not daily_stats or len(daily_stats) < 2:
            # Nếu không đủ daily stats, trả về 0 nhưng vẫn cố gắng trả về monthly stats nếu có
            logger.warning(f"VidIQ: Không đủ daily_stats cho {channel_id}")
            total_30_days = 0
        else:
            yesterday_total_views = daily_stats[1].get("views", 0)
            views_today_realtime = max(0, current_total_views - yesterday_total_views)
            
            # Lấy tối đa 29 ngày quá khứ (slice an toàn dù list ngắn hơn)
            past_days_stats = daily_stats[1:30]
            views_past_29_days = sum(day.get("views_change", 0) for day in past_days_stats)
            total_30_days = views_today_realtime + views_past_29_days

        # --- Xử lý Monthly Stats ---
        monthly_stats = []
        
        for stat in monthly_stats_raw:
            ts = stat.get("date")
            if ts:
                dt = datetime.utcfromtimestamp(ts)
                
                # FIX: Logic lùi 2 tháng chính xác bằng toán học thay vì timedelta(days=60)
                # Logic: Giữ nguyên năm, lùi tháng. Nếu tháng <= 0 thì lùi năm.
                new_month = dt.month - 2
                new_year = dt.year
                if new_month <= 0:
                    new_month += 12
                    new_year -= 1
                
                # Format thành 'YYYY-MM'
                month_str = f"{new_year}-{new_month:02d}"
                
                monthly_stats.append({
                    "month": month_str,
                    "views_gained": stat.get("views_change", 0),
                    "total_views_at_end": stat.get("views", 0),
                    "subscribers": stat.get("subscribers", 0),
                    "subscribers_change": stat.get("subscribers_change", 0)
                })

        # --- Bổ sung tháng hiện tại (Next Month logic) ---
        if monthly_stats:
            # Sort: Mới nhất lên đầu để lấy tháng gần nhất
            monthly_stats.sort(key=lambda x: x["month"], reverse=True)
            
            latest_item = monthly_stats[0]
            latest_month_str = latest_item["month"] # vd: '2025-10'
            latest_total_views = latest_item["total_views_at_end"]
            
            # Tính tháng tiếp theo
            y, m = map(int, latest_month_str.split('-'))
            next_m = m + 1
            next_y = y
            if next_m > 12:
                next_m = 1
                next_y += 1
            next_month_str = f"{next_y}-{next_m:02d}"

            # Chỉ insert nếu chưa tồn tại
            if next_month_str != latest_month_str:
                next_views_gained = max(0, current_total_views - latest_total_views)
                
                monthly_stats.insert(0, { # Insert vào đầu luôn vì là mới nhất
                    "month": next_month_str,
                    "views_gained": next_views_gained,
                    "total_views_at_end": current_total_views,
                    "subscribers": current_subs_count,
                    "subscribers_change": 0 # VidIQ thường ko có real-time subs change chính xác ở đây
                })

        # Đảm bảo return đúng thứ tự (Mới nhất trước)
        monthly_stats.sort(key=lambda x: x["month"], reverse=True)
        
        logger.info(f"VidIQ fetch for {channel_id}: 30days={total_30_days}, monthly_len={monthly_stats}")
        return total_30_days, daily_stats, monthly_stats

    except Exception as e:
        logger.exception(f"Lỗi khi fetch VidIQ cho {channel_id}: {e}")
        return 0, [], []