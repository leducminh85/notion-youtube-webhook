import logging
from typing import Optional, Dict, List
import requests

logger = logging.getLogger(__name__)


def safe_json(resp: requests.Response) -> Dict:
	try:
		return resp.json()
	except Exception:
		logger.debug("safe_json: failed to parse JSON, returning raw text", exc_info=True)
		return {"_raw": getattr(resp, "text", "")}


def get_property_value(props: Dict, prop_name: str) -> Optional[str]:
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
