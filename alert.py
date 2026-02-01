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
#  CONFIG LOADING (ENV first, fallback to config_localonly.json)
# ============================================================

def load_zulip_config():
    email = os.environ.get("ZULIP_EMAIL")
    api_key = os.environ.get("ZULIP_API_KEY")
    site = os.environ.get("ZULIP_SITE")

    if email and api_key and site:
        return email, api_key, site

    # Fallback to local config file
    try:
        with open("config_localonly.json", "r") as f:
            cfg = json.load(f)
            return (
                cfg["ZULIP_EMAIL"],
                cfg["ZULIP_API_KEY"],
                cfg["ZULIP_SITE"]
            )
    except Exception as e:
        print("Error loading Zulip config:", e)
        raise


ZULIP_EMAIL, ZULIP_API_KEY, ZULIP_SITE = load_zulip_config()

client = zulip.Client(
    email=ZULIP_EMAIL,
    api_key=ZULIP_API_KEY,
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
        return {"last_status": None}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


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
    return aggregated.get("status")  # "active", "idle", "offline"


def send_presence_notification():
    content = "IsNowActive"
    client.send_message({
        "type": "private",
        "to": [NOTIFY_USER],
        "content": content,
    })
    print("Notification:", content)


# ============================================================
#  HEARTBEAT LOOP
# ============================================================

def send_heartbeat_loop():
    last_sent_hour = None
    allowed_hours = {7, 10, 13, 16, 19, 22}  # JST hours

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
                    result = client.send_message({
                        "type": "private",
                        "to": [NOTIFY_USER],
                        "content": content,
                    })
                    print("Heartbeat sent:", result)
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
#  UNREAD COUNT NOTIFICATION
# ============================================================

def get_unread_count(stream, topic):
    result = client.get_messages({
        "anchor": "newest",
        "num_before": 0,
        "num_after": 500,
        "narrow": [
            {"operator": "stream", "operand": stream},
            {"operator": "topic", "operand": topic},
            {"operator": "is", "operand": "unread"},
        ],
    })

    if result["result"] != "success":
        print("Error fetching unread messages:", result)
        return 0

    return len(result.get("messages", []))


def notify_unread_count():
    unread = get_unread_count(TARGET_STREAM, TARGET_TOPIC)

    if unread > 0:
        try:
            # Send to the target user
            client.send_message({
                "type": "private",
                "to": [TARGET_USER_EMAIL],
                "content": f"{unread} incident(s).",
            })

            # Send to your own ID as well
            client.send_message({
                "type": "private",
                "to": [NOTIFY_USER],
                "content": f"{unread} incident(s).",
            })

        except Exception as e:
            print("Error sending unread count notification:", e)


# ============================================================
#  RANDOM MESSAGE BROADCASTER
# ============================================================

def load_random_messages():
    if not os.path.exists(MESSAGES_FILE):
        return []
    with open(MESSAGES_FILE, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def get_subscribed_streams():
    result = client.get_subscriptions()
    if result["result"] != "success":
        print("Error fetching subscriptions:", result)
        return []
    return result["subscriptions"]


def get_topics_for_stream(stream_id):
    result = client.get_stream_topics(stream_id)
    if result["result"] != "success":
        print("Error fetching topics:", result)
        return []
    return [t["name"] for t in result["topics"]]


def broadcast_random_messages():
    # Load messages from file
    messages = load_random_messages()
    if not messages:
        return

    # Pick 3 random messages
    chosen_msgs = random.sample(messages, min(3, len(messages)))

    # Get subscribed streams
    subs = get_subscribed_streams()
    if not subs:
        return

    # Pick 3 random streams
    chosen_streams = random.sample(subs, min(3, len(subs)))

    # For each stream, pick a random topic and send the corresponding message
    for idx, stream in enumerate(chosen_streams):
        topics = get_topics_for_stream(stream["stream_id"])
        if not topics:
            continue  # Skip if no topics exist

        topic = random.choice(topics)
        msg = chosen_msgs[idx]  # message 0 → stream 0, message 1 → stream 1, etc.

        try:
            client.send_message({
                "type": "stream",
                "to": stream["name"],
                "subject": topic,
                "content": msg,
            })
            # print(f"Sent message[{idx}] to {stream['name']} / {topic}")
        except Exception as e:
            print("Error sending random message:", e)


# ============================================================
#  15-MINUTE LOOP (UNREAD COUNT + RANDOM BROADCAST)
# ============================================================

def unread_msg_count():
    while True:
        now_utc = datetime.now(timezone.utc)
        now_jst = now_utc + timedelta(hours=9)

        hour = now_jst.hour
        minute = now_jst.minute

        # Run exactly at :00, :15, :30, :45
        if 6 <= hour <= 23 and minute in {0, 15, 30, 45}:
            notify_unread_count()
            broadcast_random_messages()

            # Prevent double‑triggering within the same minute
            time.sleep(60)

        # Sleep until the next minute boundary
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

    yield  # App is now ready


# ============================================================
#  FASTAPI APP
# ============================================================

app = FastAPI(lifespan=lifespan)


@app.get("/ping")
def ping():
    return {"status": "alive"}