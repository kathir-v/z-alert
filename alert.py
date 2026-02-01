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

DEBUG_LOG = True   # Set to False to silence detailed logs


def log(msg):
    if DEBUG_LOG:
        print(msg)


# ============================================================
#  CONFIG LOADING (ENV first, fallback to config_localonly.json)
# ============================================================

def load_zulip_config():
    email = os.environ.get("ZULIP_EMAIL")
    api_key = os.environ.get("ZULIP_API_KEY")
    site = os.environ.get("ZULIP_SITE")
    TARGET_USER_EMAIL = os.environ.get("TARGET_USER_EMAIL")
    TARGET_USER_API_KEY = os.environ.get("TARGET_USER_API_KEY")

    if email and api_key and site and TARGET_USER_EMAIL and TARGET_USER_API_KEY:
        log("Loaded Zulip config from environment variables.")
        return email, api_key, site, TARGET_USER_EMAIL, TARGET_USER_API_KEY

    try:
        with open("config_localonly.json", "r") as f:
            cfg = json.load(f)
            log("Loaded Zulip config from config_localonly.json.")
            return (
                cfg["ZULIP_EMAIL"],
                cfg["ZULIP_API_KEY"],
                cfg["ZULIP_SITE"],
                cfg["TARGET_USER_EMAIL"],
                cfg["TARGET_USER_API_KEY"]
            )
    except Exception as e:
        print("Error loading Zulip config:", e)
        raise


ZULIP_EMAIL, ZULIP_API_KEY, ZULIP_SITE, TARGET_USER_EMAIL, TARGET_USER_API_KEY = load_zulip_config()

client = zulip.Client(
    email=ZULIP_EMAIL,
    api_key=ZULIP_API_KEY,
    site=ZULIP_SITE
)

target_client = zulip.Client(
    email=TARGET_USER_EMAIL,
    api_key=TARGET_USER_API_KEY,
    site=ZULIP_SITE
)

# ============================================================
#  CONSTANTS
# ============================================================

TARGET_USER_ID = 1003298
NOTIFY_USER = "user1003296@spfr.zulipchat.com"

TARGET_STREAM = "spring"
TARGET_TOPIC = "Txt"
TARGET_USER_EMAIL = "user1003298@spfr.zulipchat.com"

STATE_FILE = "presence.json"
MESSAGES_FILE = "messages.txt"


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
    result = client.call_endpoint(
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
    client.send_message({
        "type": "private",
        "to": [NOTIFY_USER],
        "content": content,
    })
    log("Presence notification sent.")


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
                    client.send_message({
                        "type": "private",
                        "to": [NOTIFY_USER],
                        "content": content,
                    })
                    log(f"Heartbeat sent at hour {hour}.")
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

    stream_info = client.get_stream_id(TARGET_STREAM)
    if stream_info["result"] != "success":
        print("Stream not found:", TARGET_STREAM)
        return

    if (
        msg.get("stream_id") == stream_info["stream_id"]
        and msg.get("subject") == TARGET_TOPIC
        and msg.get("sender_email") == TARGET_USER_EMAIL
    ):
        try:
            client.send_message({
                "type": "private",
                "to": [NOTIFY_USER],
                "content": "Alert: Join Teams Meeting",
            })
            log("Meeting alert sent.")
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
#  UNREAD COUNT NOTIFICATION (for the TARGET USER)
# ============================================================

def get_unread_count(stream, topic):
    try:
        result = target_client.get_messages({
            "anchor": "newest",
            "num_before": 0,
            "num_after": 500,
            "narrow": [
                {"operator": "stream", "operand": stream},
                {"operator": "topic", "operand": topic},
                {"operator": "is", "operand": "unread"},
            ],
        })
    except Exception as e:
        log(f"[ERROR] Unread count fetch failed due to network/API error: {e}")
        return None  # Signal failure to caller

    if result.get("result") != "success":
        log(f"[ERROR] Zulip returned failure: {result}")
        return None

    unread = len(result.get("messages", []))
    log(f"[TARGET_USER] Unread count for {stream}/{topic}: {unread}")
    return unread


def notify_unread_count():
    unread = get_unread_count(TARGET_STREAM, TARGET_TOPIC)

    if unread is None:
        log("Skipping unread notification due to API failure.")
        return

    if unread > 0:
        try:
            client.send_message({
                "type": "private",
                "to": [TARGET_USER_EMAIL],
                "content": f"{unread} online order(s) received.",
            })

            client.send_message({
                "type": "private",
                "to": [NOTIFY_USER],
                "content": f"{unread} online order(s) received.",
            })

            log(f"Unread notification sent to both users: {unread}")
        except Exception as e:
            log(f"[ERROR] Failed to send unread notification: {e}")


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
    result = client.get_subscriptions()
    if result["result"] != "success":
        print("Error fetching subscriptions:", result)
        return []
    subs = result["subscriptions"]
    log(f"Subscribed streams: {[s['name'] for s in subs]}")
    return subs


def get_topics_for_stream(stream_id):
    result = client.get_stream_topics(stream_id)
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

        try:
            client.send_message({
                "type": "stream",
                "to": stream["name"],
                "subject": topic,
                "content": msg,
            })
            log(f"Sent message[{idx}] to {stream['name']} / {topic}")
        except Exception as e:
            print("Error sending random message:", e)


# ============================================================
#  15-MINUTE CLOCK-ALIGNED LOOP
# ============================================================

def unread_msg_count():
    while True:
        now_utc = datetime.now(timezone.utc)
        now_jst = now_utc + timedelta(hours=9)

        hour = now_jst.hour
        minute = now_jst.minute

        # if True:
        if minute in {0, 15, 30, 45}:
            log(f"15-minute trigger at {now_jst.strftime('%H:%M')} JST")

            if 6 <= hour <= 23:
                notify_unread_count()
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
        target=lambda: client.call_on_each_event(handle_event, event_types=["message"]),
        daemon=True
    ).start()
    threading.Thread(target=unread_msg_count, daemon=True).start()

    yield


# ============================================================
#  FASTAPI APP
# ============================================================

app = FastAPI(lifespan=lifespan)


@app.get("/ping")
def ping():
    return {"status": "alive"}