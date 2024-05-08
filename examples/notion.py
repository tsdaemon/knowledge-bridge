import os

from notion_client import Client

from .helpers import get_driver


driver = get_driver()
notion_api_key = os.getenv("NOTION_API_KEY")
notion = Client(auth=os.environ["NOTION_TOKEN"])

# Load all notion blocks
print(notion.search(query=""))
