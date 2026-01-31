import os
import json
import time
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
#  FETCH BOT-SENT DIRECT MESSAGES (PAGINATED)
# ============================================================

def fetch_bot_direct_messages(anchor):
    """
    Fetches up to 200 direct messages sent by the bot,
    anchored at the given message ID or 'newest'.
    """
    try:
        result = client.get_messages({
            "anchor": anchor,
            "num_before": 200,     # back to: fetch older messages
            "num_after": 0,        # back to: look backwards from anchor
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
#  DELETE OLD MESSAGES (WITH PAGINATION)
# ============================================================

def delete_old_messages():
    now = datetime.now(timezone.utc)
    cutoff = now - DELETE_OLDER_THAN

    # Start from the newest messages
    anchor = "newest"
    total_deleted = 0

    while True:
        messages = fetch_bot_direct_messages(anchor)

        if not messages:
            break

        print(f"Fetched {len(messages)} messages at anchor={anchor}")

        # Oldest message ID in this batch (for pagination)
        oldest_id = messages[0]["id"]

        batch_deleted = 0

        for msg in messages:
            ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)

            if ts < cutoff:
                # Introduce a delay to avoid hitting zulips rate limit
                time.sleep(0.15)

                msg_id = msg["id"]
                try:
                    result = client.call_endpoint(
                        url=f"/messages/{msg_id}",
                        method="DELETE"
                    )
                    if result["result"] == "success":
                        batch_deleted += 1
                    else:
                        print(f"Failed to delete message {msg_id}: {result}")
                except Exception as e:
                    print(f"Error deleting message {msg_id}:", e)

        total_deleted += batch_deleted

        print(f"Deleted in this batch: {batch_deleted}")

        # Pagination: move anchor to the oldest message ID
        anchor = oldest_id

        # If fewer than 200 messages returned, we reached the end
        if len(messages) < 200:
            break

        # Delay the fetch of next batch of 200 records by 10s
        time.sleep(10)

    print(f"Total deleted: {total_deleted}")


# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    print("Starting daily cleanup worker...")
    delete_old_messages()
    print("Cleanup complete.")