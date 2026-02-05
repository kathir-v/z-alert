import os
import json
import time
import threading
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
import random

import zulip
from fastapi import FastAPI

# ============================================================
#  DEBUG LOGGING FLAG
# ============================================================

DEBUG_LOG = False   # Set to False to silence detailed logs


def log(msg):
    if DEBUG_LOG:
        print(msg)


# ============================================================
#  CONFIG LOADING (ENV first, fallback to config_localonly.json)
# ============================================================

def load_zulip_config():
    bot_email = os.environ.get("ZULIP_EMAIL")
    bot_api_key = os.environ.get("ZULIP_API_KEY")
    site = os.environ.get("ZULIP_SITE")

    source_login_email = os.environ.get("SOURCE_USER_EMAIL")
    source_api_key = os.environ.get("SOURCE_USER_API_KEY")

    target_login_email = os.environ.get("TARGET_USER_EMAIL")
    target_api_key = os.environ.get("TARGET_USER_API_KEY")

    if (
        bot_email and bot_api_key and site and
        source_login_email and source_api_key and
        target_login_email and target_api_key
    ):
        log("Loaded Zulip config from environment variables.")
        return (
            bot_email, bot_api_key, site,
            source_login_email, source_api_key,
            target_login_email, target_api_key
        )

    try:
        with open("config_localonly.json", "r") as f:
            cfg = json.load(f)
            log("Loaded Zulip config from config_localonly.json.")
            return (
                cfg["ZULIP_EMAIL"],
                cfg["ZULIP_API_KEY"],
                cfg["ZULIP_SITE"],
                cfg["SOURCE_USER_EMAIL"],
                cfg["SOURCE_USER_API_KEY"],
                cfg["TARGET_USER_EMAIL"],
                cfg["TARGET_USER_API_KEY"],
            )
    except Exception as e:
        print("Error loading Zulip config:", e)
        raise


(
    BOT_LOGIN_EMAIL,
    BOT_API_KEY,
    ZULIP_SITE,
    SOURCE_USER_LOGIN_EMAIL,
    SOURCE_USER_API_KEY,
    TARGET_USER_LOGIN_EMAIL,
    TARGET_USER_API_KEY,
) = load_zulip_config()

# ============================================================
#  CLIENTS
# ============================================================

# BOT client
bot_client = zulip.Client(
    email=BOT_LOGIN_EMAIL,
    api_key=BOT_API_KEY,
    site=ZULIP_SITE
)

# SOURCE USER client
source_client = zulip.Client(
    email=SOURCE_USER_LOGIN_EMAIL,
    api_key=SOURCE_USER_API_KEY,
    site=ZULIP_SITE
)

# TARGET USER client
target_client = zulip.Client(
    email=TARGET_USER_LOGIN_EMAIL,
    api_key=TARGET_USER_API_KEY,
    site=ZULIP_SITE
)


def get_internal_email(zclient, login_email_label):
    """
    Try to fetch the internal Zulip email via /users/me (get_profile).
    If it fails, fall back to the login email passed in.
    """
    try:
        profile = zclient.get_profile()
        if profile.get("result") == "success":
            return profile["email"]
        else:
            log(f"[WARN] get_profile failed for {login_email_label}: {profile}")
            return login_email_label
    except Exception as e:
        log(f"[WARN] Exception in get_internal_email for {login_email_label}: {e}")
        return login_email_label


# Internal Zulip emails (used for messaging, sender checks, etc.)
BOT_ZULIP_EMAIL = get_internal_email(bot_client, BOT_LOGIN_EMAIL)
SOURCE_USER_ZULIP_EMAIL = get_internal_email(source_client, SOURCE_USER_LOGIN_EMAIL)
TARGET_USER_ZULIP_EMAIL = get_internal_email(target_client, TARGET_USER_LOGIN_EMAIL)

# ============================================================
#  CONSTANTS
# ============================================================

# If you need a numeric user ID for presence, set it here:
TARGET_USER_ID = 1003298  # keep as-is if already correct

TARGET_STREAM = "spring"
TARGET_TOPIC = "Txt"

STATE_FILE = "presence.json"
MESSAGES_FILE = "messages.txt"

# ============================================================
#  CLEANUP CONSTANTS
# ============================================================

BOT_SENDER = BOT_ZULIP_EMAIL
DM_DELETE_OLDER_THAN = timedelta(hours=48)
STREAM_DELETE_OLDER_THAN = timedelta(days=3)
EXCLUDED_STREAM = "spring"
EXCLUDED_TOPIC = "Txt"

# ============================================================
#  STATE MANAGEMENT
# ============================================================

def load_previous_state():
    if not os.path.exists(STATE_FILE):
        log("No previous presence state found.")
        return {"last_status": None}
    with open(STATE_FILE, "r") as f:
        state = json.load(f)
        log(f"Loaded previous presence state: {state}")
        return state


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)
    log(f"Saved presence state: {state}")


# ============================================================
#  PRESENCE CHECKING
# ============================================================

def get_user_presence(user_id):
    result = bot_client.call_endpoint(
        url=f"/users/{user_id}/presence",
        method="GET",
    )
    if result["result"] != "success":
        print("Error fetching presence:", result)
        return None

    presence = result["presence"]
    aggregated = presence.get("aggregated", {})
    status = aggregated.get("status")
    log(f"Presence for {user_id}: {status}")
    return status


def send_presence_notification():
    content = "IsNowActive"
    bot_client.send_message({
        "type": "private",
        "to": [SOURCE_USER_ZULIP_EMAIL],
        "content": content,
    })
    print("Notification:", content)


# ============================================================
#  HEARTBEAT LOOP
# ============================================================

def send_heartbeat_loop():
    last_sent_hour = None
    allowed_hours = {7, 10, 13, 16, 19, 22}

    while True:
        now_utc = datetime.now(timezone.utc)
        now_jst = now_utc + timedelta(hours=9)

        hour = now_jst.hour
        minute = now_jst.minute

        if hour in allowed_hours and minute == 0:
            if last_sent_hour != hour:
                content = (
                    f"Railway Heartbeat:\n"
                    f"Current time (JST): {now_jst.strftime('%Y-%m-%d %H:%M:%S')}"
                )
                try:
                    bot_client.send_message({
                        "type": "private",
                        "to": [SOURCE_USER_ZULIP_EMAIL],
                        "content": content,
                    })
                    print(f"Heartbeat sent at hour {hour}.")
                    last_sent_hour = hour
                except Exception as e:
                    print("Error sending heartbeat:", e)

        time.sleep(60)


# ============================================================
#  ZULIP EVENT HANDLER
# ============================================================

def handle_event(event):
    if event["type"] != "message":
        return

    msg = event["message"]

    if msg.get("type") != "stream":
        return

    stream_info = bot_client.get_stream_id(TARGET_STREAM)
    if stream_info["result"] != "success":
        print("Stream not found:", TARGET_STREAM)
        return

    if (
        msg.get("stream_id") == stream_info["stream_id"]
        and msg.get("subject") == TARGET_TOPIC
        and msg.get("sender_email") == TARGET_USER_ZULIP_EMAIL
    ):
        try:
            bot_client.send_message({
                "type": "private",
                "to": [SOURCE_USER_ZULIP_EMAIL],
                "content": "Alert: Join Teams Meeting",
            })
            print("Notification: Message")
        except Exception as e:
            print("Error sending meeting alert:", e)


# ============================================================
#  PRESENCE MONITOR LOOP
# ============================================================

def presence_monitor_loop():
    state = load_previous_state()
    last_status = state.get("last_status")

    while True:
        current_status = get_user_presence(TARGET_USER_ID)

        if current_status == "active" and last_status != "active":
            send_presence_notification()

        state["last_status"] = current_status
        save_state(state)

        last_status = current_status
        time.sleep(60)


# ============================================================
#  MSG COUNT NOTIFICATION (for the TARGET USER in the last 15m)
# ============================================================

def get_messages_last_15_minutes(stream, topic):
    try:
        result = target_client.get_messages({
            "anchor": "newest",
            "num_before": 200,
            "num_after": 0,
            "narrow": [
                {"operator": "stream", "operand": stream},
                {"operator": "topic", "operand": topic},
            ],
        })
    except Exception as e:
        log(f"[ERROR] Failed to fetch messages: {e}")
        return None

    if result.get("result") != "success":
        log(f"[ERROR] Zulip returned failure: {result}")
        return None

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc.timestamp() - (15 * 60)

    recent_msgs = [
        m for m in result.get("messages", [])
        if m["timestamp"] >= cutoff
    ]

    count = len(recent_msgs)
    log(f"[TARGET_USER] Messages in last 15 minutes for {stream}/{topic}: {count}")
    return count


def notify_recent_message_count():
    count = get_messages_last_15_minutes(TARGET_STREAM, TARGET_TOPIC)

    if count is None:
        log("Skipping recent message notification due to API failure.")
        return

    if count > 0:
        try:
            bot_client.send_message({
                "type": "private",
                "to": [TARGET_USER_ZULIP_EMAIL],
                "content": f"{count} Incident(s).",
            })
            log(f"Recent message notification sent: {count}")
        except Exception as e:
            log(f"[ERROR] Failed to send recent message notification: {e}")


# ============================================================
#  MUTE TARGET TOPIC FOR SOURCE + TARGET USERS
# ============================================================

def mute_target_topic():
    """
    Mutes TARGET_TOPIC under TARGET_STREAM for:
      - SOURCE_USER (source_client)
      - TARGET_USER (target_client)
    Only mutes if currently unmuted.
    Logs successful mute actions.
    """

    stream_info = bot_client.get_stream_id(TARGET_STREAM)
    if stream_info["result"] != "success":
        log(f"[MUTE] Stream not found: {TARGET_STREAM}")
        return

    stream_id = stream_info["stream_id"]

    def process_user(zclient, label):
        result = zclient.call_endpoint(
            url="/user_topics",
            method="GET"
        )

        if result.get("result") != "success":
            log(f"[MUTE] Failed to fetch user_topics for {label}: {result}")
            return

        muted_list = result.get("user_topics", [])

        already_muted = any(
            t["stream_id"] == stream_id and
            t["topic"] == TARGET_TOPIC and
            t["visibility_policy"] == 1
            for t in muted_list
        )

        if already_muted:
            log(f"[MUTE] {label}: Topic already muted.")
            return

        mute_result = zclient.call_endpoint(
            url="/user_topics",
            method="POST",
            request={
                "stream_id": stream_id,
                "topic": TARGET_TOPIC,
                "mute": True
            }
        )

        if mute_result.get("result") == "success":
            print(f"[MUTE] {label}: Muted {TARGET_STREAM}/{TARGET_TOPIC}")
        else:
            print(f"[MUTE] {label}: Failed to mute topic: {mute_result}")

    process_user(source_client, "SOURCE_USER")
    process_user(target_client, "TARGET_USER")


# ============================================================
#  RANDOM MESSAGE BROADCASTER
# ============================================================

def load_random_messages():
    if not os.path.exists(MESSAGES_FILE):
        log("messages.txt not found.")
        return []
    with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
        msgs = [line.strip() for line in f if line.strip()]
        log(f"Loaded {len(msgs)} messages from messages.txt")
        return msgs


def get_subscribed_streams():
    result = bot_client.get_subscriptions()
    if result["result"] != "success":
        print("Error fetching subscriptions:", result)
        return []
    subs = result["subscriptions"]
    log(f"Subscribed streams: {[s['name'] for s in subs]}")
    return subs


def get_topics_for_stream(stream_id):
    result = bot_client.get_stream_topics(stream_id)
    if result["result"] != "success":
        print("Error fetching topics:", result)
        return []
    topics = [t["name"] for t in result["topics"]]
    log(f"Topics for stream {stream_id}: {topics}")
    return topics


def broadcast_random_messages():
    messages = load_random_messages()
    if not messages:
        return

    chosen_msgs = random.sample(messages, min(3, len(messages)))
    log(f"Chosen messages: {chosen_msgs}")

    subs = get_subscribed_streams()
    if not subs:
        return

    chosen_streams = random.sample(subs, min(3, len(subs)))
    log(f"Chosen streams: {[s['name'] for s in chosen_streams]}")

    for idx, stream in enumerate(chosen_streams):
        topics = get_topics_for_stream(stream["stream_id"])
        if not topics:
            log(f"No topics in stream {stream['name']}, skipping.")
            continue

        topic = random.choice(topics)
        msg = chosen_msgs[idx]

        # Skip sending to TARGET_STREAM/TARGET_TOPIC
        if stream["name"] == TARGET_STREAM and topic == TARGET_TOPIC:
            log(f"Skipping random broadcast to {TARGET_STREAM}/{TARGET_TOPIC}")
            continue

        try:
            bot_client.send_message({
                "type": "stream",
                "to": stream["name"],
                "subject": topic,
                "content": msg,
            })
            log(f"Sent message[{idx}] to {stream['name']} / {topic}")
        except Exception as e:
            print("Error sending random message:", e)


# ============================================================
#  CLEANUP HELPERS
# ============================================================

def fetch_bot_messages(anchor):
    try:
        result = bot_client.get_messages({
            "anchor": anchor,
            "num_before": 200,
            "num_after": 0,
            "narrow": [
                {"operator": "sender", "operand": BOT_SENDER}
            ]
        })
        if result["result"] != "success":
            print("Error fetching messages:", result)
            return []
        return result["messages"]
    except Exception as e:
        print("Error in fetch_bot_messages:", e)
        return []


def delete_old_direct_messages(messages):
    now = datetime.now(timezone.utc)
    cutoff = now - DM_DELETE_OLDER_THAN
    deleted = 0

    for msg in messages:
        if msg["type"] != "private":
            continue

        ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)
        if ts < cutoff:
            time.sleep(0.15)
            msg_id = msg["id"]
            try:
                result = bot_client.call_endpoint(
                    url=f"/messages/{msg_id}",
                    method="DELETE"
                )
                if result["result"] == "success":
                    deleted += 1
            except Exception as e:
                print(f"Error deleting DM {msg_id}:", e)

    return deleted


def delete_old_stream_messages(messages):
    now = datetime.now(timezone.utc)
    cutoff = now - STREAM_DELETE_OLDER_THAN
    deleted = 0

    for msg in messages:
        if msg["type"] != "stream":
            continue

        stream_name = msg.get("display_recipient")
        topic = msg.get("subject")

        if stream_name == EXCLUDED_STREAM and topic == EXCLUDED_TOPIC:
            continue

        ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)
        if ts < cutoff:
            time.sleep(0.15)
            msg_id = msg["id"]
            try:
                result = bot_client.call_endpoint(
                    url=f"/messages/{msg_id}",
                    method="DELETE"
                )
                if result["result"] == "success":
                    deleted += 1
            except Exception as e:
                print(f"Error deleting stream message {msg_id}:", e)

    return deleted


def send_cleanup_summary(dm_deleted, stream_deleted):
    summary = (
        "🧹*Daily Cleanup Report*\n"
        f"DM removed: {dm_deleted}\n"
        f"Stream removed: {stream_deleted}"
    )
    try:
        bot_client.send_message({
            "type": "private",
            "to": [SOURCE_USER_ZULIP_EMAIL],
            "content": summary,
        })
        print("Cleanup summary sent.")
    except Exception as e:
        print("Failed to send summary DM:", e)


def run_delete_noti():
    print("Starting cleanup worker (API-triggered)...")

    anchor = "newest"
    total_dm_deleted = 0
    total_stream_deleted = 0

    while True:
        messages = fetch_bot_messages(anchor)
        if not messages:
            break

        print(f"Fetched {len(messages)} messages at anchor={anchor}")

        oldest_id = messages[0]["id"]

        dm_deleted = delete_old_direct_messages(messages)
        total_dm_deleted += dm_deleted

        stream_deleted = delete_old_stream_messages(messages)
        total_stream_deleted += stream_deleted

        anchor = oldest_id

        if len(messages) < 200:
            break

        print("Waiting 60 seconds before next batch...")
        time.sleep(60)

    print("Cleanup complete.")
    send_cleanup_summary(total_dm_deleted, total_stream_deleted)

    return {
        "dm_deleted": total_dm_deleted,
        "stream_deleted": total_stream_deleted
    }


# ============================================================
#  15-MINUTE CLOCK-ALIGNED LOOP
# ============================================================

def check_recent_messages_loop():
    while True:
        now_utc = datetime.now(timezone.utc)
        now_jst = now_utc + timedelta(hours=9)

        hour = now_jst.hour
        minute = now_jst.minute

        if minute in {0, 15, 30, 45}:
            log(f"15-minute trigger at {now_jst.strftime('%H:%M')} JST")

            if 6 <= hour <= 23:
                notify_recent_message_count()
                mute_target_topic()
                if minute in {30}:
                    broadcast_random_messages()

            time.sleep(60)

        time.sleep(1)


# ============================================================
#  FASTAPI LIFESPAN (STARTUP THREADS)
# ============================================================

@asynccontextmanager
async def lifespan(app):
    print("Starting background threads...")

    threading.Thread(target=send_heartbeat_loop, daemon=True).start()
    threading.Thread(target=presence_monitor_loop, daemon=True).start()
    threading.Thread(
        target=lambda: bot_client.call_on_each_event(
            handle_event,
            event_types=["message"]
        ),
        daemon=True
    ).start()
    threading.Thread(target=check_recent_messages_loop, daemon=True).start()

    yield


# ============================================================
#  FASTAPI APP
# ============================================================

app = FastAPI(lifespan=lifespan)


@app.get("/ping")
def ping():
    return {"status": "alive"}


@app.get("/deletenoti8221")
def delete_noti_endpoint():
    result = run_delete_noti()
    return {
        "status": "cleanup completed",
        "dm_deleted": result["dm_deleted"],
        "stream_deleted": result["stream_deleted"]
    }