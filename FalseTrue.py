import os
import zipfile
import rarfile
import py7zr
import pandas as pd
import requests
import sqlite3
from flask import Flask, request
import telebot

# =========================
# Config
# =========================
BOT_TOKEN = "8384623873:AAH1BFcheGw_Mwzkt2ighSm4JAyqtODQ3Pg"
WEBHOOK_URL = os.environ.get("WEBHOOK_URL")  # e.g., https://your-app.onrender.com/
bot = telebot.TeleBot(BOT_TOKEN, threaded=False)

# =========================
# Storage Paths
# =========================
DOWNLOAD_DIR = "downloads"
EXTRACT_DIR = "extracted_files"
DB_FILE = "data.db"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(EXTRACT_DIR, exist_ok=True)

# =========================
# Database Functions
# =========================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS data")
    c.execute("CREATE TABLE data (row TEXT)")
    conn.commit()
    conn.close()

def insert_rows(rows):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.executemany("INSERT INTO data (row) VALUES (?)", [(r,) for r in rows])
    conn.commit()
    conn.close()

def search_db(query):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("SELECT row FROM data WHERE row LIKE ?", (f"%{query}%",))
    results = [r[0] for r in c.fetchall()]
    conn.close()
    return results

# =========================
# File Handling
# =========================
def convert_google_drive_link(url: str) -> str:
    if "drive.google.com" in url:
        if "id=" in url:
            file_id = url.split("id=")[1].split("&")[0]
        elif "/d/" in url:
            file_id = url.split("/d/")[1].split("/")[0]
        else:
            return url
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    return url

def download_file(url, chat_id):
    local_filename = os.path.join(DOWNLOAD_DIR, url.split("/")[-1])
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_size = int(r.headers.get("content-length", 0))
        downloaded = 0
        last_percent = 0
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size > 0:
                        percent = int(downloaded * 100 / total_size)
                        if percent >= last_percent + 10:
                            bot.send_message(chat_id, f"⬇️ Download progress: {percent}%")
                            last_percent = percent
    return local_filename

def extract_archive(file_path):
    try:
        if zipfile.is_zipfile(file_path):
            with zipfile.ZipFile(file_path, "r") as zf:
                zf.extractall(EXTRACT_DIR)
        elif rarfile.is_rarfile(file_path):
            with rarfile.RarFile(file_path, "r") as rf:
                rf.extractall(EXTRACT_DIR)
        elif file_path.endswith(".7z"):
            with py7zr.SevenZipFile(file_path, "r") as z:
                z.extractall(EXTRACT_DIR)
    except RuntimeError:
        return False  # Possibly password-protected
    return True

def load_csv_to_db():
    init_db()
    for root, _, files in os.walk(EXTRACT_DIR):
        for file in files:
            if file.endswith(".csv"):
                csv_path = os.path.join(root, file)
                try:
                    df = pd.read_csv(csv_path, dtype=str, low_memory=False)
                    df = df.fillna("")
                    # Fix scientific notation numbers
                    df = df.applymap(lambda x: str(x).replace(".0", "") if "E+" in str(x) or "e+" in str(x) else str(x))
                    rows = df.astype(str).apply(lambda row: ", ".join(row), axis=1).tolist()
                    insert_rows(rows)
                except Exception as e:
                    print(f"⚠️ Error reading {csv_path}: {e}")

# =========================
# Bot Handlers
# =========================
@bot.message_handler(commands=["start"])
def start_command(message):
    welcome_text = (
        "🤖 *Welcome to CSV Search Bot!*\n\n"
        "📂 *Import Data:* `/import <link>` → Download & extract ZIP/RAR/7Z (Google Drive, Dropbox, direct URL)\n"
        "🔍 *Search Data:* `/search <keyword>` → Search all CSVs and return matching rows\n\n"
        "ℹ️ Notes:\n"
        "- Fixes numbers like `91...E+11` → `9123456789`\n"
        "- Google Drive links auto-converted to direct download\n"
        "- Telegram upload limit = 2GB → use `/import` for larger files"
    )
    bot.send_message(message.chat.id, welcome_text, parse_mode="Markdown")

@bot.message_handler(commands=["import"])
def import_command(message):
    try:
        url = message.text.split(" ", 1)[1].strip()
    except IndexError:
        bot.reply_to(message, "⚠️ Please provide a valid link.")
        return

    url = convert_google_drive_link(url)
    bot.reply_to(message, "⏳ Starting download...")

    try:
        file_path = download_file(url, message.chat.id)
        bot.send_message(message.chat.id, f"✅ File downloaded: `{file_path}`\n⏳ Extracting now...", parse_mode="Markdown")

        success = extract_archive(file_path)
        if not success:
            bot.send_message(message.chat.id, "⚠️ Archive is password-protected or cannot be opened.")
            return

        bot.send_message(message.chat.id, "✅ Archive extracted.\n⏳ Loading CSV data into database...")
        load_csv_to_db()
        bot.send_message(message.chat.id, "🎉 Data imported successfully! Use `/search <keyword>` to find entries.")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Import failed: {e}")

@bot.message_handler(commands=["search"])
def search_command(message):
    try:
        query = message.text.split(" ", 1)[1]
    except IndexError:
        bot.reply_to(message, "⚠️ Please provide a search keyword.")
        return

    results = search_db(query)
    if not results:
        bot.reply_to(message, "❌ No matches found.")
    else:
        for row in results[:10]:
            bot.send_message(message.chat.id, row)

# =========================
# Flask App for Webhook
# =========================
app = Flask(__name__)

@app.route("/")
def home():
    return "✅ Telegram CSV Search Bot is running!"

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "!", 200

# =========================
# Run Flask + Set Webhook
# =========================
if __name__ == "__main__":
    if WEBHOOK_URL:
        bot.remove_webhook()
        bot.set_webhook(url=WEBHOOK_URL + BOT_TOKEN)
        print(f"Webhook set to {WEBHOOK_URL + BOT_TOKEN}")

    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
