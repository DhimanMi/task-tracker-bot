import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "/data/tasks.db")
GOOGLE_SA_FILE = os.getenv("GOOGLE_SA_FILE", "/secrets/google-sa.json")
SHEET_ID = os.getenv("SHEET_ID")
ADMIN_IDS = [int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
EXPORT_ENABLED_KEY = "export_enabled"