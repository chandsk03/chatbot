import asyncio
import logging
import time
from functools import wraps
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import pymongo
from pymongo.errors import ConnectionFailure, PyMongoError
from telegram.error import TelegramError, Forbidden
import aiohttp
from aiohttp import web

# Configuration
TOKEN = "7552161237:AAEI_Fi1NVRfkkWpWGjm58gIhEgV_07USUM"
ADMIN_ID = 7303763913
RATE_LIMIT_SECONDS = 1.0
WEBHOOK_URL = "https://chatbot-brown-tau.vercel.app/webhook"  # Replace with your public URL
WEBHOOK_PORT = 80  # Common port for HTTPS
BATCH_SIZE = 20  # Number of broadcast messages per batch

# MongoDB Setup with Retry
client = None
for attempt in range(3):
    try:
        client = pymongo.MongoClient("mongodb+srv://desiurl33:wW3wTqkERCvOn0sp@telegram-bots.5icgn.mongodb.net/?retryWrites=true&w=majority&appName=telegram-bots", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        logging.info("MongoDB connection successful")
        break
    except ConnectionFailure as e:
        print(f"MongoDB connection attempt {attempt + 1} failed: {e}")
        time.sleep(2 ** attempt)
else:
    raise Exception("MongoDB is not accessible after multiple attempts.")

db = client["telegram_chat_bot"]
users = db["users"]
queue = db["queue"]

# Create indexes for performance
try:
    users.create_index([("state", 1)])
    queue.create_index([("queued_at", 1)])
except PyMongoError as e:
    logging.error(f"Failed to create MongoDB indexes: {e}")

# Logging Setup
logging.basicConfig(
    filename='bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Helper Functions
def get_partner_id(user_id):
    """Retrieve the partner ID of a user if they are chatting."""
    try:
        user = users.find_one({"_id": user_id})
        return user.get("partner_id") if user and user["state"] == "chatting" else None
    except PyMongoError as e:
        logging.error(f"MongoDB error in get_partner_id for user {user_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error in get_partner_id for user {user_id}: {e}")
        return None

async def send_reply(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """Send a reply to the user."""
    try:
        if hasattr(update, 'message') and update.message:
            await update.message.reply_text(text)
        else:
            logging.error(f"Cannot send reply to user {update.effective_user.id}: No valid message object")
    except Forbidden:
        logging.info(f"User {update.effective_user.id} has blocked the bot or deleted chat")
    except TelegramError as e:
        logging.error(f"Telegram error sending reply to user {update.effective_user.id}: {e}")
    except Exception as e:
        logging.error(f"Error sending reply to user {update.effective_user.id}: {e}")

def rate_limit(func):
    """Decorator to enforce a rate limit on commands and messages."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        try:
            user = users.find_one({"_id": user_id})
            if user:
                current_time = time.time()
                last_time = user.get("last_action_time", 0)
                if current_time - last_time < RATE_LIMIT_SECONDS:
                    await send_reply(update, context, "Please wait before performing another action.")
                    return
                users.update_one({"_id": user_id}, {"$set": {"last_action_time": current_time}})
        except PyMongoError as e:
            logging.error(f"MongoDB error in rate_limit for user {user_id}: {e}")
            await send_reply(update, context, "Database error. Please try again later.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

# Command Handlers
@rate_limit
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start command: Register user, reset state, and send welcome message."""
    user = update.effective_user
    user_id = user.id
    user_data = {
        "_id": user_id,
        "username": user.username,
        "first_name": user.first_name,
        "state": "idle",
        "partner_id": None,
        "last_action_time": 0
    }
    try:
        users.update_one({"_id": user_id}, {"$set": user_data}, upsert=True)
        welcome_msg = (
            f"Welcome, {user.first_name or 'User'}!\n"
            "Here are the available commands:\n"
            "/start - Start the bot\n"
            "/find - Find a stranger to chat with\n"
            "/stop - End the current chat\n"
            "/cancel - Cancel the search\n"
            "/next - Move to the next stranger"
        )
        if user_id == ADMIN_ID:
            welcome_msg += "\n/stats - View bot statistics (Admin only)"
        await send_reply(update, context, welcome_msg)
    except PyMongoError as e:
        logging.error(f"MongoDB error in start for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in start for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred. Please try again later.")

@rate_limit
async def find(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /find command: Pair user with a stranger or add to queue."""
    user_id = update.effective_user.id
    try:
        user = users.find_one({"_id": user_id})
        if not user:
            await send_reply(update, context, "Please use /start first.")
            return
        if user["state"] != "idle":
            await send_reply(update, context, "You are already searching or chatting. Use /stop or /cancel first.")
            return

        other = queue.find_one_and_delete({}, sort=[("queued_at", 1)])
        if other:
            other_id = other["_id"]
            partner = users.find_one({"_id": other_id, "state": "searching"})
            if not partner:
                await send_reply(update, context, "No valid partners available. Please try again.")
                return
            users.update_one({"_id": user_id}, {"$set": {"state": "chatting", "partner_id": other_id}})
            users.update_one({"_id": other_id}, {"$set": {"state": "chatting", "partner_id": user_id}})
            await context.bot.send_message(user_id, "You are now chatting with a stranger!")
            try:
                await context.bot.send_message(other_id, "You are now chatting with a stranger!")
            except Forbidden:
                logging.info(f"Partner {other_id} has blocked the bot or deleted chat")
                users.update_many(
                    {"_id": {"$in": [user_id, other_id]}},
                    {"$set": {"state": "idle", "partner_id": None}}
                )
                await send_reply(update, context, "Your partner is unavailable. Use /find to try again.")
            except TelegramError as e:
                logging.error(f"Failed to notify partner {other_id}: {e}")
                users.update_many(
                    {"_id": {"$in": [user_id, other_id]}},
                    {"$set": {"state": "idle", "partner_id": None}}
                )
                await send_reply(update, context, "Your partner is unavailable. Use /find to try again.")
        else:
            queue.insert_one({"_id": user_id, "queued_at": time.time()})
            users.update_one({"_id": user_id}, {"$set": {"state": "searching", "partner_id": None}})
            await send_reply(update, context, "Searching for a stranger...")
    except PyMongoError as e:
        logging.error(f"MongoDB error in find for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in find for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred. Please try again later.")

@rate_limit
async def stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stop command: End the current chat."""
    user_id = update.effective_user.id
    try:
        user = users.find_one({"_id": user_id})
        if not user or user["state"] != "chatting" or not user["partner_id"]:
            await send_reply(update, context, "You are not in a chat.")
            return

        partner_id = user["partner_id"]
        users.update_many(
            {"_id": {"$in": [user_id, partner_id]}},
            {"$set": {"state": "idle", "partner_id": None}}
        )
        await context.bot.send_message(user_id, "You have stopped chatting.")
        try:
            await context.bot.send_message(partner_id, "The stranger has stopped chatting.")
        except Forbidden:
            logging.info(f"Partner {partner_id} has blocked the bot or deleted chat")
        except TelegramError as e:
            logging.error(f"Failed to notify partner {partner_id}: {e}")
    except PyMongoError as e:
        logging.error(f"MongoDB error in stop for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in stop for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred. Please try again later.")

@rate_limit
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /cancel command: Cancel the search process."""
    user_id = update.effective_user.id
    try:
        user = users.find_one({"_id": user_id})
        if not user or user["state"] != "searching":
            await send_reply(update, context, "You are not searching.")
            return

        queue.delete_one({"_id": user_id})
        users.update_one({"_id": user_id}, {"$set": {"state": "idle", "partner_id": None}})
        await send_reply(update, context, "Search cancelled.")
    except PyMongoError as e:
        logging.error(f"MongoDB error in cancel for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in cancel for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred. Please try again later.")

@rate_limit
async def next_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /next command: End current chat and find a new partner."""
    user_id = update.effective_user.id
    try:
        user = users.find_one({"_id": user_id})
        if not user or user["state"] != "chatting":
            await send_reply(update, context, "You are not in a chat. Use /find to start.")
            return

        await stop(update, context)
        await find(update, context)
    except PyMongoError as e:
        logging.error(f"MongoDB error in next_command for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in next_command for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred. Please try again later.")

@rate_limit
async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /broadcast command: Send a message to all users in batches (admin only)."""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await send_reply(update, context, "You are not authorized to use this command.")
        return

    try:
        message = update.message.text.split(" ", 1)[1] if len(update.message.text.split(" ", 1)) > 1 else None
        if not message:
            await send_reply(update, context, "Usage: /broadcast <message>")
            return

        all_users = list(users.find())  # Convert cursor to list for batching
        total_users = len(all_users)
        count = 0

        async def send_batch(batch):
            tasks = []
            for user in batch:
                tasks.append(
                    asyncio.create_task(
                        context.bot.send_message(user["_id"], message)
                    )
                )
            for task in tasks:
                try:
                    await task
                    nonlocal count
                    count += 1
                except Forbidden:
                    logging.info(f"User {user['_id']} has blocked the bot or deleted chat")
                except TelegramError as e:
                    logging.error(f"Failed to send broadcast to {user['_id']}: {e}")

        # Process users in batches
        for i in range(0, total_users, BATCH_SIZE):
            batch = all_users[i:i + BATCH_SIZE]
            await send_batch(batch)
            if i + BATCH_SIZE < total_users:
                await asyncio.sleep(1)  # Rate limit between batches

        await send_reply(update, context, f"Broadcast sent to {count} users.")
    except PyMongoError as e:
        logging.error(f"MongoDB error in broadcast for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in broadcast for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred during broadcast.")

@rate_limit
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /stats command: Show bot statistics (admin only)."""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID:
        await send_reply(update, context, "You are not authorized to use this command.")
        return

    try:
        total_users = users.count_documents({})
        chatting_users = users.count_documents({"state": "chatting"})
        searching_users = users.count_documents({"state": "searching"})
        online_users = chatting_users + searching_users

        stats_msg = (
            "📊 Bot Statistics:\n"
            f"Total Users: {total_users}\n"
            f"Online Users: {online_users}\n"
            f"Chatting Users: {chatting_users}\n"
            f"Searching Users: {searching_users}"
        )
        await send_reply(update, context, stats_msg)
    except PyMongoError as e:
        logging.error(f"MongoDB error in stats for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Please try again later.")
    except Exception as e:
        logging.error(f"Error in stats for user {user_id}: {e}")
        await send_reply(update, context, "An error occurred while fetching statistics.")

# Message Handler
@rate_limit
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle user messages and forward them to the chat partner."""
    user_id = update.message.from_user.id
    try:
        user = users.find_one({"_id": user_id})
        if not user:
            await send_reply(update, context, "Please use /start first.")
            return
        if user["state"] != "chatting" or not user["partner_id"]:
            await send_reply(update, context, "You are not in a chat. Use /find to start.")
            return

        partner_id = user["partner_id"]
        partner = users.find_one({"_id": partner_id})
        if not partner or partner["state"] != "chatting":
            await send_reply(update, context, "Your partner is no longer available. Use /find to start a new chat.")
            users.update_one({"_id": user_id}, {"$set": {"state": "idle", "partner_id": None}})
            return

        if update.message.text:
            await context.bot.send_message(partner_id, update.message.text)
        elif update.message.photo:
            await context.bot.send_photo(partner_id, update.message.photo[-1].file_id)
        elif update.message.video:
            await context.bot.send_video(partner_id, update.message.video.file_id)
        logging.info(f"Message forwarded from {user_id} to {partner_id}")
    except Forbidden:
        logging.info(f"Partner {partner_id} has blocked the bot or deleted chat")
        users.update_one({"_id": user_id}, {"$set": {"state": "idle", "partner_id": None}})
        await send_reply(update, context, "Your partner is unavailable. Use /find to start a new chat.")
    except PyMongoError as e:
        logging.error(f"MongoDB error in handle_message for user {user_id}: {e}")
        await send_reply(update, context, "Database error. Your chat has been ended. Please try again.")
        try:
            users.update_one({"_id": user_id}, {"$set": {"state": "idle", "partner_id": None}})
        except PyMongoError as db_e:
            logging.error(f"Failed to reset user {user_id} state after MongoDB error: {db_e}")
    except TelegramError as e:
        logging.error(f"Telegram error in handle_message for user {user_id}: {e}")
        try:
            users.update_many(
                {"_id": {"$in": [user_id, partner_id]}},
                {"$set": {"state": "idle", "partner_id": None}}
            )
        except PyMongoError as db_e:
            logging.error(f"MongoDB error resetting states after Telegram error for user {user_id}: {db_e}")
        await send_reply(update, context, "Failed to send message. Your chat has been ended. Use /find to start a new one.")
    except Exception as e:
        logging.error(f"Unexpected error in handle_message for user {user_id}: {e}")
        await send_reply(update, context, "Error sending message. Please try again.")

# Webhook Handler
async def webhook_handler(request):
    """Handle incoming webhook updates."""
    app = request.app['telegram_app']
    update = Update.de_json(await request.json(), app.bot)
    await app.process_update(update)
    return web.Response(status=200)

async def setup_webhook(application):
    """Set up the webhook with Telegram."""
    await application.bot.set_webhook(url=WEBHOOK_URL)
    logging.info(f"Webhook set to {WEBHOOK_URL}")

# Bot Setup and Main Function
async def main() -> None:
    """Run the bot with webhook."""
    logging.info("Initializing Telegram bot application...")
    try:
        application = Application.builder().token(TOKEN).build()
        logging.info("Telegram bot application initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize Telegram bot application: {e}")
        raise

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("find", find))
    application.add_handler(CommandHandler("stop", stop))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("next", next_command))
    application.add_handler(CommandHandler("broadcast", broadcast))
    application.add_handler(CommandHandler("stats", stats))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.PHOTO, handle_message))
    application.add_handler(MessageHandler(filters.VIDEO, handle_message))

    # Set up aiohttp server
    app = web.Application()
    app['telegram_app'] = application
    app.router.add_post('/webhook', webhook_handler)

    # Initialize application and set webhook
    await application.initialize()
    await setup_webhook(application)

    # Start the web server
    logging.info(f"Starting webhook server on port {WEBHOOK_PORT}...")
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', WEBHOOK_PORT)
    await site.start()

    # Keep the bot running
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logging.info("Shutting down webhook server...")
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
