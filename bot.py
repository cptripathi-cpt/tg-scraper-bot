import os, re, io
import pandas as pd
from telethon import TelegramClient, utils
from telethon.sessions import StringSession
from telethon.tl import types
from telethon.errors import FloodWaitError, RPCError
from telethon.tl.functions.messages import ImportChatInviteRequest

from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# -------- ENV VARS (Render/Railway par set karoge) --------
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION_STRING = os.environ["SESSION_STRING"]  # generate_session.py se
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))  # optional: restrict to your user id

def build_telethon_client():
    return TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

def safe_filename(name: str) -> str:
    name = name or "telegram_channel"
    return re.sub(r'[\\/*?:\"<>|]', "_", name).strip()[:100]

def build_tme_link(entity, message_id: int):
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{message_id}"
    if isinstance(entity, types.Channel) or isinstance(entity, types.Chat):
        return f"https://t.me/c/{getattr(entity, 'id', '')}/{message_id}"
    return None

def best_file_name(msg):
    if not msg.file:
        return None
    if getattr(msg.file, "name", None):
        return msg.file.name
    ext = getattr(msg.file, "ext", None)
    if not ext and getattr(msg.file, "mime_type", None):
        import mimetypes
        ext = mimetypes.guess_extension(msg.file.mime_type) or ""
    if msg.photo and not ext:
        ext = ".jpg"
    return f"{msg.id}{ext or ''}"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "👋 Send command like:\n"
        "`/scrape <channel_link_or_username> [start_id] [end_id]`\n\n"
        "**Examples**\n"
        "• `/scrape @testchannel`\n"
        "• `/scrape https://t.me/testchannel 100 300`\n"
        "• `/scrape https://t.me/c/1234567890 1 500`\n\n"
        "_Private channels: your user session must be a member._"
    )
    await update.message.reply_markdown(text)

async def scrape(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if OWNER_ID and update.effective_user.id != OWNER_ID:
        await update.message.reply_text("Unauthorized. Ask the owner to enable access for you.")
        return

    if len(context.args) < 1:
        await update.message.reply_text("Usage:\n/scrape <channel_link_or_username> [start_id] [end_id]")
        return

    source = context.args[0].strip()
    start_id = int(context.args[1]) if len(context.args) > 1 and context.args[1].isdigit() else None
    end_id = int(context.args[2]) if len(context.args) > 2 and context.args[2].isdigit() else None

    status = await update.message.reply_text("⏳ Scraping…")

    client = build_telethon_client()
    rows = []
    try:
        await client.connect()

        # Auto-join if invite link (t.me/+...)
        if "t.me/+" in source or "joinchat" in source:
            try:
                invite_hash = source.split("+", 1)[-1].split("/", 1)[0]
                await client(ImportChatInviteRequest(invite_hash))
            except RPCError:
                pass

        entity = await client.get_entity(source)
        channel_title = getattr(entity, "title", getattr(entity, "first_name", ""))
        channel_name = safe_filename(channel_title)

        # Telethon: min_id=> >, max_id => <
        min_id = (start_id - 1) if start_id else None
        max_id = (end_id + 1) if end_id else None

        count = 0
        async for msg in client.iter_messages(entity, min_id=min_id, max_id=max_id):
            post_link = build_tme_link(entity, msg.id)
            file_name = file_size = file_mime = None
            if msg.file:
                file_name = best_file_name(msg)
                file_size = getattr(msg.file, "size", None)
                file_mime = getattr(msg.file, "mime_type", None)

            rows.append({
                "channel_title": channel_title,
                "channel_id": getattr(entity, "id", None),
                "is_private": "Yes" if not getattr(entity, "username", None) else "No",
                "message_id": msg.id,
                "message_date": msg.date.isoformat() if msg.date else None,
                "post_link": post_link,
                "file_name": file_name,
                "file_size_bytes": file_size,
                "file_mime": file_mime,
                "text_snippet": (msg.text[:200] if msg.text else None)
            })
            count += 1
            if count % 500 == 0:
                try:
                    await status.edit_text(f"⏳ Scraping… {count} messages processed…")
                except:
                    pass

        if not rows:
            await status.edit_text("No messages found for the given range/source.")
            return

        df = pd.DataFrame(rows)

        # Excel to memory buffer (no disk issues)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="data")
        buf.seek(0)

        filename = f"{channel_name}.xlsx"
        await update.message.reply_document(buf, filename=filename, caption=f"✅ {len(df)} rows • {channel_title}")
        await status.delete()
    except FloodWaitError as fw:
        await status.edit_text(f"Rate limited. Try later. ({fw})")
    except Exception as e:
        await status.edit_text(f"Error: {e}")
    finally:
        await client.disconnect()

def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("scrape", scrape))
    app.run_polling()

if __name__ == "__main__":
    main()
