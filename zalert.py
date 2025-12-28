import zulip
import os
import json
from datetime import datetime, timedelta, timezone

# -----------------------------
# Load Zulip client
# -----------------------------
try:
    client = zulip.Client(
        email=os.environ.get("ZULIP_EMAIL"),
        api_key=os.environ.get("ZULIP_API_KEY"),
        site=os.environ.get("ZULIP_SITE")
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
    try:
        if not os.path.exists(STATE_FILE):
            return {"last_status": None}
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        print("Error loading state:", e)
        return {"last_status": None}

def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("Error saving state:", e)

# -----------------------------
# Presence Checking
# -----------------------------
def get_user_presence(user_id):
    try:
        result = client.call_endpoint(
            url=f"/users/{user_id}/presence",
            method="GET",
        )
        if result["result"] != "success":
            print("Error fetching presence:", result)
            return None

        presence = result["presence"]
        aggregated = presence.get("aggregated", {})
        return aggregated.get("status")
    except Exception as e:
        print("Error in get_user_presence:", e)
        return None

# -----------------------------
# Count messages in last 60 seconds
# -----------------------------
def count_recent_messages():
    try:
        now = datetime.now(timezone.utc)
        one_min_ago = now - timedelta(seconds=60)

        narrow = [
            {"operator": "stream", "operand": TARGET_STREAM},
            {"operator": "topic", "operand": TARGET_TOPIC},
            {"operator": "sender", "operand": TARGET_USER_EMAIL},
        ]

        result = client.get_messages({
            "anchor": "newest",
            "num_before": 50,
            "num_after": 0,
            "narrow": narrow
        })

        if result["result"] != "success":
            print("Error fetching messages:", result)
            return 0

        count = 0
        for msg in result["messages"]:
            ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)
            if ts >= one_min_ago:
                count += 1

        return count

    except Exception as e:
        print("Error in count_recent_messages:", e)
        return 0

# -----------------------------
# Main (runs once per minute)
# -----------------------------
if __name__ == "__main__":
    try:
        # Load previous presence
        state = load_previous_state()
        last_status = state.get("last_status")

        # Check presence
        current_status = get_user_presence(TARGET_USER_ID)

        # Send IsNowActive alert
        if current_status == "active" and last_status != "active":
            try:
                client.send_message({
                    "type": "private",
                    "to": [NOTIFY_USER],
                    "content": "IsNowActive",
                })
                print("Notification sent: IsNowActive")
            except Exception as e:
                print("Error sending IsNowActive:", e)

        # Save new presence state
        state["last_status"] = current_status
        save_state(state)

        # Count messages in last 60 seconds
        count = count_recent_messages()

        # Send NewM/{count}
        try:
            client.send_message({
                "type": "private",
                "to": [NOTIFY_USER],
                "content": f"NewM/{count}",
            })
            print(f"Notification sent: NewM/{count}")
        except Exception as e:
            print("Error sending NewM count:", e)

    except Exception as e:
        print("Fatal error in main:", e)
