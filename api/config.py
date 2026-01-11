# from dotenv import load_dotenv
import os

# Load local .env when running locally
# load_dotenv()

NOTION_VERSION = os.environ.get("NOTION_VERSION", "2022-06-28")
