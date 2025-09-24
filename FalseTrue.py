import os
import requests
import zipfile
import rarfile
import py7zr
import pandas as pd
from flask import Flask, request
import telebot

# =====================
# CONFIG
# =====================
BOT_TOKEN = "8384623873:AAH1BFcheGw_Mwzkt2ighSm4JAyqtODQ3Pg"
bot = telebot.TeleBot(BOT_TOKEN)

DATA_FOLDER = "data"
os.makedirs(DATA_FOLDER, exist_ok=True)

CSV_FILE = os.path.join(DATA_FOLDER, "data.csv")

app = Flask(__name__)

# =====================
# ROUTES
# =====================

@app.route("/", methods=["GET"])
def home():
    return "✅ Telegram CSV Search Bot is running!", 200

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    json_str = request.get_data().decode("utf-8")
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "!", 200

# =====================
# UTILITIES
# =====================

def download_file_from_url(url, save_path):
    response = requests.get(url, stream=True)
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    return save_path

def extract_file(file_path, extract_to=DATA_FOLDER):
    if file_path.endswith(".zip"):
        with zipfile.ZipFile(file_path, "r") as zf:
            zf.extractall(extract_to)
    elif file_path.endswith(".rar") or file_path.endswith(".rar.ab") or file_path.endswith(".rar.ac") or file_path.endswith(".rar.ad"):
        with rarfile.RarFile(file_path, "r") as rf:
            rf.extractall(extract_to)
    elif file_path.endswith(".7z"):
        with py7zr.SevenZipFile(file_path, "r") as zf:
            zf.extractall(extract_to)
    else:
        raise ValueError("Unsupported file type")

def load_csv():
    # Automatically load first CSV found
    for root, _, files in os.walk(DATA_FOLDER):
        for file in files:
            if file.endswith(".csv"):
                return os.path.join(root, file)
    return None

def clean_number(num_str):
    """Fix numbers like 9.1E+11 → 9123456789"""
    try:
        if "E+" in str(num_str).upper():
            return str(int(float(num_str)))
        return str(num_str)
    except:
        return str(num_str)

# =====================
# BOT HANDLERS
# =====================

@bot.message_handler(commands=["start"])
def start(message):
    welcome_text = (
        "👋 Welcome to the CSV Search Bot!\n\n"
        "Here’s what I can do:\n"
        "📂 /import <link> → Import and extract data from Google Drive or direct link\n"
        "🔎 /search <name> → Find a row in the extracted CSV (exact match)\n"
        "ℹ️ /start → Show this help message again\n\n"
        "⚡ Tip: Large archives (GBs) may take time, but status updates will be sent."
    )
    bot.reply_to(message, welcome_text)

@bot.message_handler(commands=["import"])
def import_file(message):
    try:
        url = message.text.replace("/import", "").strip()
        if not url:
            bot.reply_to(message, "❌ Please provide a file link.\nExample:\n`/import https://drive.google.com/uc?id=FILE_ID`", parse_mode="Markdown")
            return

        bot.reply_to(message, "⏳ Downloading file...")

        file_path = os.path.join(DATA_FOLDER, "datafile")
        download_file_from_url(url, file_path)

        bot.reply_to(message, "📂 Extracting file...")
        extract_file(file_path, DATA_FOLDER)

        csv_path = load_csv()
        if not csv_path:
            bot.reply_to(message, "❌ No CSV file found after extraction.")
            return

        # Normalize CSV and save cleaned version
        df = pd.read_csv(csv_path, dtype=str)
        df = df.applymap(clean_number)
        df.to_csv(CSV_FILE, index=False)

        bot.reply_to(message, f"✅ File imported and CSV ready!\nLoaded rows: {len(df)}")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Import failed: {e}")

@bot.message_handler(commands=["search"])
def search(message):
    query = message.text.replace("/search", "").strip()
    if not query:
        bot.reply_to(message, "❌ Please provide a search term.\nExample:\n`/search John Doe`", parse_mode="Markdown")
        return

    try:
        if not os.path.exists(CSV_FILE):
            bot.reply_to(message, "❌ No CSV data loaded. Use /import first.")
            return

        df = pd.read_csv(CSV_FILE, dtype=str).fillna("")
        result = df[df.apply(lambda row: row.astype(str).str.fullmatch(query).any(), axis=1)]

        if not result.empty:
            rows = result.astype(str).to_dict(orient="records")
            for row in rows:
                response = "\n".join([f"{k}: {v}" for k, v in row.items()])
                bot.reply_to(message, f"🔎 Match found:\n\n{response}")
        else:
            bot.reply_to(message, f"❌ No exact match found for: {query}")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Search failed: {e}")

# =====================
# MAIN ENTRY
# =====================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
