import os
import json
from datetime import datetime, timedelta, timezone

import zulip

# ============================================================
#  CONFIG LOADING (same pattern as your main script)
# ============================================================

def load_zulip_config():
    email = os.environ.get("ZULIP_EMAIL")
    api_key = os.environ.get("ZULIP_API_KEY")
    site = os.environ.get("ZULIP_SITE")

    if email and api_key and site:
        return email, api_key, site

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

# Delete only messages sent BY this bot
BOT_SENDER = ZULIP_EMAIL

# Delete messages older than 24 hours
DELETE_OLDER_THAN = timedelta(hours=24)


# ============================================================
#  FETCH BOT-SENT DIRECT MESSAGES
# ============================================================

def fetch_bot_direct_messages():
    """
    Fetches recent direct messages sent by the bot.
    Zulip requires narrowing by 'sender' and 'pm-with'.
    Since you said the bot sends DMs only to you, we can
    simply narrow by sender=BOT_SENDER and type=private.
    """
    try:
        result = client.get_messages({
            "anchor": "newest",
            "num_before": 200,
            "num_after": 0,
            "narrow": [
                {"operator": "sender", "operand": BOT_SENDER},
                {"operator": "is", "operand": "private"},
            ]
        })

        if result["result"] != "success":
            print("Error fetching messages:", result)
            return []

        return result["messages"]

    except Exception as e:
        print("Error in fetch_bot_direct_messages:", e)
        return []


# ============================================================
#  DELETE OLD MESSAGES
# ============================================================

def delete_old_messages():
    now = datetime.now(timezone.utc)
    cutoff = now - DELETE_OLDER_THAN

    messages = fetch_bot_direct_messages()
    print(f"Fetched {len(messages)} direct messages sent by bot.")

    deleted_count = 0

    for msg in messages:
        ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)

        if ts < cutoff:
            msg_id = msg["id"]
            try:
                result = client.call_endpoint(
                    url=f"/messages/{msg_id}",
                    method="DELETE"
                )
                if result["result"] == "success":
                    # print(f"Deleted message {msg_id} from {ts.isoformat()}")
                    deleted_count += 1
                else:
                    print(f"Failed to delete message {msg_id}: {result}")
            except Exception as e:
                print(f"Error deleting message {msg_id}:", e)

    print(f"Total deleted: {deleted_count}")


# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    print("Starting daily cleanup worker...")
    delete_old_messages()
    print("Cleanup complete.")