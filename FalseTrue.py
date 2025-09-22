#!/usr/bin/env python3
import os
import io
import zipfile
import rarfile
import py7zr
import pandas as pd
import sqlite3
import telebot

# =========================
# CONFIGURATION
# =========================
BOT_TOKEN = "8384623873:AAH1BFcheGw_Mwzkt2ighSm4JAyqtODQ3Pg"  # Your bot token
DATA_DIR = "extracted_files"
DB_FILE = "data.db"

os.makedirs(DATA_DIR, exist_ok=True)
bot = telebot.TeleBot(BOT_TOKEN)

# =========================
# Database setup
# =========================
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

# Table to store CSV data dynamically
cursor.execute("""
CREATE TABLE IF NOT EXISTS csv_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_text TEXT
)
""")
conn.commit()

# =========================
# Extraction functions
# =========================
def extract_file(file_path):
    extracted_files = []
    filename = os.path.basename(file_path)
    try:
        if filename.endswith(".zip"):
            with zipfile.ZipFile(file_path, 'r') as z:
                z.extractall(DATA_DIR)
                extracted_files = z.namelist()
        elif filename.endswith((".rar", ".rar.ab", ".rar.ac", ".rar.ad")):
            with rarfile.RarFile(file_path) as r:
                r.extractall(DATA_DIR)
                extracted_files = r.namelist()
        elif filename.endswith(".7z"):
            with py7zr.SevenZipFile(file_path, mode='r') as s:
                s.extractall(path=DATA_DIR)
                extracted_files = s.getnames()
        else:
            return False, []
        return True, extracted_files
    except Exception as e:
        print(f"Error extracting {file_path}: {e}")
        return False, []

# =========================
# Load CSVs into SQLite
# =========================
def load_csv_to_db():
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".csv"):
                path = os.path.join(root, file)
                try:
                    # Read CSV in chunks
                    for chunk in pd.read_csv(path, dtype=str, chunksize=100000):
                        # Fix scientific notation numbers
                        chunk = chunk.applymap(lambda x: '{0:.0f}'.format(float(x)) if isinstance(x, str) and 'E' in x else x)
                        # Insert rows into SQLite
                        for _, row in chunk.iterrows():
                            row_text = ", ".join(row.values)
                            cursor.execute("INSERT INTO csv_data (row_text) VALUES (?)", (row_text,))
                    conn.commit()
                except Exception as e:
                    print(f"Error reading CSV {path}: {e}")

# =========================
# Telegram Handlers
# =========================
@bot.message_handler(content_types=['document'])
def handle_file(message):
    try:
        file_info = bot.get_file(message.document.file_id)
        saved_path = os.path.join(DATA_DIR, message.document.file_name)
        downloaded_file = bot.download_file(file_info.file_path)
        with open(saved_path, 'wb') as f:
            f.write(downloaded_file)
        
        success, extracted = extract_file(saved_path)
        if success:
            load_csv_to_db()
            bot.reply_to(message, f"✅ File extracted and data loaded! {len(extracted)} files found.")
        else:
            bot.reply_to(message, "❌ Failed to extract the file.")
    except Exception as e:
        bot.reply_to(message, f"❌ Error processing file: {e}")

@bot.message_handler(commands=['search'])
def handle_search(message):
    query = message.text.replace('/search', '').strip()
    if not query:
        bot.reply_to(message, "⚠️ Please provide a name to search.")
        return
    
    try:
        cursor.execute("SELECT row_text FROM csv_data WHERE row_text LIKE ?", (f"%{query}%",))
        results = cursor.fetchall()
        if results:
            for r in results[:10]:  # limit 10 results per message
                bot.send_message(message.chat.id, r[0])
        else:
            bot.reply_to(message, "❌ No match found.")
    except Exception as e:
        bot.reply_to(message, f"❌ Error searching database: {e}")

# =========================
# Start bot
# =========================
print("✅ Bot is running...")
bot.infinity_polling()
