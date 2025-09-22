#!/usr/bin/env python3
import os
import logging
import pandas as pd
import zipfile
import rarfile
import py7zr
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

# =========================
# Credentials
# =========================
BOT_TOKEN = "8449504199:AAEk3b780z2Ts8MS2YTPqdcZs090DO4ygeM"
API_ID = 23627016
API_HASH = "d8c9b9dabe3bc5d9905ba5c0160ab5a7"
ADMIN_ID = 8343668073
DATA_DIR = "data"

os.makedirs(DATA_DIR, exist_ok=True)

# =========================
# Logging setup
# =========================
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# Utility functions
# =========================
def fix_extension(file_path):
    name, ext = os.path.splitext(file_path)
    ext = ext.lower()
    if ext not in [".zip", ".rar", ".7z", ".csv"]:
        new_ext = ".zip" if "zip" in ext else ".rar" if "rar" in ext else ".zip"
        new_file = name + new_ext
        os.rename(file_path, new_file)
        return new_file
    return file_path

def extract_file(file_path, extract_dir):
    file_path = fix_extension(file_path)
    try:
        if file_path.lower().endswith(".zip"):
            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_dir)
        elif file_path.lower().endswith(".rar"):
            with rarfile.RarFile(file_path, "r") as rar_ref:
                rar_ref.extractall(extract_dir)
        elif file_path.lower().endswith(".7z"):
            with py7zr.SevenZipFile(file_path, "r") as sz:
                sz.extractall(path=extract_dir)
    except Exception as e:
        logger.error("Extraction failed for %s: %s", file_path, e)
        return False
    return True

def load_csvs_recursive(root_dir):
    csv_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            if f.lower().endswith(".csv"):
                csv_files.append(os.path.join(dirpath, f))
    return csv_files

def search_csvs(keyword):
    results = []
    csv_paths = load_csvs_recursive(DATA_DIR)
    for csv_file in csv_paths:
        try:
            for chunk in pd.read_csv(csv_file, chunksize=50000, dtype=str, encoding="utf-8", on_bad_lines="skip"):
                chunk.fillna("", inplace=True)
                mask = chunk.apply(lambda row: row.astype(str).str.contains(keyword, case=False, na=False)).any(axis=1)
                for _, row in chunk[mask].iterrows():
                    formatted = []
                    for v in row.values:
                        v = str(v)
                        if "E+" in v or "e+" in v:
                            try:
                                v = str(int(float(v)))
                            except:
                                pass
                        formatted.append(v)
                    results.append("\n".join(formatted))
        except Exception as e:
            logger.error("Error reading CSV %s: %s", csv_file, e)
    return results

# =========================
# Telegram Handlers
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🤖 Kaiivaro bhagwan 🙏\nWelcome to the bot!")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    help_text = (
        "Available commands:\n"
        "/start - Start the bot\n"
        "/help - Show this help message\n"
        "/list - List all CSV files\n"
        "/search <keyword> - Search CSV files\n"
        "/upload - Upload a file (ZIP/RAR/CSV)"
    )
    await update.message.reply_text(help_text)

async def list_files(update: Update, context: ContextTypes.DEFAULT_TYPE):
    csv_files = load_csvs_recursive(DATA_DIR)
    if csv_files:
        reply = "CSV files:\n" + "\n".join(csv_files)
    else:
        reply = "No CSV files found."
    await update.message.reply_text(reply)

async def search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /search <keyword>")
        return
    keyword = " ".join(context.args)
    results = search_csvs(keyword)
    # Notify admin
    try:
        admin_msg = f"User: @{update.message.from_user.username or 'Unknown'} ({update.message.from_user.id})\nCommand: /search {keyword}"
        await context.bot.send_message(chat_id=ADMIN_ID, text=admin_msg)
    except:
        logger.warning("Admin notification failed")
    if results:
        for r in results[:10]:
            await update.message.reply_text(r)
        if len(results) > 10:
            await update.message.reply_text(f"...and {len(results)-10} more results")
    else:
        await update.message.reply_text("No matches found.")

async def upload_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.document:
        file = update.message.document
        file_path = os.path.join(DATA_DIR, file.file_name)
        await file.get_file().download_to_drive(file_path)
        success = extract_file(file_path, DATA_DIR)
        if success:
            await update.message.reply_text(f"File {file.file_name} uploaded and extracted successfully!")
        else:
            await update.message.reply_text(f"⚠️ Could not extract {file.file_name}. Please check the format.")
    else:
        await update.message.reply_text("Please send a document (ZIP/RAR/CSV).")

async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        logger.info("Received message: %s", update.message.text)
        await update.message.reply_text(f"Echo: {update.message.text}")

# =========================
# Minimal web server for Render Web Service
# =========================
def start_web_server():
    class SimpleHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()
            self.wfile.write(b"🤖 Bot is running")
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), SimpleHandler)
    logger.info(f"Web server running on port {port}")
    server.serve_forever()

# =========================
# Main
# =========================
def main():
    print("🤖 Bot is starting...")  # Console log for Pydroid/Render
    logger.info("🤖 Bot has started successfully!")

    # Start web server in a separate thread
    threading.Thread(target=start_web_server, daemon=True).start()

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("list", list_files))
    app.add_handler(CommandHandler("search", search))
    app.add_handler(MessageHandler(filters.Document.ALL, upload_file))
    app.add_handler(MessageHandler(filters.ALL, echo))
    app.run_polling()

if __name__ == "__main__":
    main()