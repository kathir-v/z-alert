import zulip
import os
import json
import threading
import time
from datetime import datetime, timedelta, timezone

# -----------------------------
# Load Zulip client
# -----------------------------
try:
    client = zulip.Client(
        email=os.environ["ZULIP_EMAIL"],
        api_key=os.environ["ZULIP_API_KEY"],
        site=os.environ["ZULIP_SITE"]
)
except Exception as e:
    print("Error loading client:", e)
    raise

# -----------------------------
# Constants
# -----------------------------
TARGET_USER_ID = 1003298
NOTIFY_USER = "user1003296@spfr.zulipchat.com"

TARGET_STREAM = "spring"
TARGET_TOPIC = "Txt"
TARGET_USER_EMAIL = "user1003298@spfr.zulipchat.com"

STATE_FILE = "presence.json"

# -----------------------------
# Presence State Management
# -----------------------------
def load_previous_state():
    if not os.path.exists(STATE_FILE):
        return {"last_status": None}
    with open(STATE_FILE, "r") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# -----------------------------
# Presence Checking
# -----------------------------
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

# -----------------------------
# Heartbeat Thread
# -----------------------------
def send_heartbeat_loop():
    last_sent_hour = None

    while True:
        now_utc = datetime.now(timezone.utc)
        now_jst = now_utc + timedelta(hours=9)

        hour = now_jst.hour
        minute = now_jst.minute

        if 6 <= hour < 22 and minute == 0:
            if last_sent_hour != hour:
                content = (
                    f"Heartbeat: Bot running on Railway.\n"
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

# -----------------------------
# Zulip Message Event Handler
# -----------------------------
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
            result = client.send_message({
                "type": "private",
                "to": [NOTIFY_USER],
                "content": "Alert: Join Teams Meeting",
            })
            print("Notification: Message")
        except Exception as e:
            print("Error sending meeting alert:", e)

# -----------------------------
# Presence Polling Loop
# -----------------------------
def presence_monitor_loop():
    state = load_previous_state()
    last_status = state.get("last_status")

    while True:
        current_status = get_user_presence(TARGET_USER_ID)
        # print("Current status:", current_status)

        if current_status == "active" and last_status != "active":
            send_presence_notification()

        state["last_status"] = current_status
        save_state(state)

        last_status = current_status
        time.sleep(60)

# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Notify startup
    client.send_message({
        "type": "private",
        "to": [NOTIFY_USER],
        "content": "Alert: Started"
    })

    # Start heartbeat thread
    threading.Thread(target=send_heartbeat_loop, daemon=True).start()

    # Start presence monitor thread
    threading.Thread(target=presence_monitor_loop, daemon=True).start()

    # Start Zulip event listener (blocking)

    client.call_on_each_event(handle_event, event_types=["message"])


