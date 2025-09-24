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
    for root, _, files in os.walk(DATA_FOLDER):
        for file in files:
            if file.endswith(".csv"):
                return os.path.join(root, file)
    return None

def clean_number(num_str):
    try:
        if "E+" in str(num_str).upper():
            return str(int(float(num_str)))
        return str(num_str)
    except:
        return str(num_str)

# =====================
# BOT HANDLERS
# =====================

@bot.message_handler(commands=['start'])
def start_message(message):
    welcome_text = (
        "👋 Welcome to the CSV Search Bot!\n\n"
        "✅ Features:\n"
        " • /import <link> → Import and extract file (zip, rar, 7z)\n"
        " • /search <name> → Search in extracted CSV\n"
        " • Replies to any text (catch-all)\n"
        " • Status updates on import/extraction\n\n"
        "📌 Try sending /import or any message!"
    )
    bot.reply_to(message, welcome_text)

@bot.message_handler(commands=['import'])
def import_file(message):
    try:
        url = message.text.replace("/import", "").strip()
        if not url:
            bot.reply_to(message, "❌ Please provide a file link. Example:\n`/import https://drive.google.com/uc?id=FILE_ID`", parse_mode="Markdown")
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

        df = pd.read_csv(csv_path, dtype=str)
        df = df.applymap(clean_number)
        df.to_csv(CSV_FILE, index=False)

        bot.reply_to(message, f"✅ File imported and CSV ready! Rows loaded: {len(df)}")
    except Exception as e:
        bot.reply_to(message, f"⚠️ Import failed: {e}")

@bot.message_handler(commands=['search'])
def search(message):
    query = message.text.replace("/search", "").strip()
    if not query:
        bot.reply_to(message, "❌ Provide a search term. Example:\n`/search John Doe`", parse_mode="Markdown")
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

# Catch-all handler for any text
@bot.message_handler(func=lambda message: True)
def echo_all(message):
    bot.reply_to(message, f"📩 You said: {message.text}")

# =====================
# WEBHOOK ROUTE
# =====================

@app.route(f"/{BOT_TOKEN}", methods=['POST'])
def webhook():
    json_str = request.get_data().decode("utf-8")
    print("📩 Incoming update:", json_str, flush=True)  # Debug logs
    update = telebot.types.Update.de_json(json_str)
    bot.process_new_updates([update])
    return "ok", 200

# Root for testing
@app.route("/", methods=['GET'])
def index():
    return "Bot is running fine ✅"

# =====================
# MAIN
# =====================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
