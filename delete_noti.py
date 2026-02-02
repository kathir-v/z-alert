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

# Delete only messages sent BY this bot ("noti")
BOT_SENDER = ZULIP_EMAIL

# DM deletion threshold: 48 hours
DM_DELETE_OLDER_THAN = timedelta(hours=48)

# Stream deletion threshold: 3 days
STREAM_DELETE_OLDER_THAN = timedelta(days=3)

# Stream/topic to EXCLUDE from deletion
EXCLUDED_STREAM = "spring"
EXCLUDED_TOPIC = "Txt"


# ============================================================
#  FETCH ALL BOT-SENT MESSAGES (PAGINATED)
#  (Correct narrow: sender only)
# ============================================================

def fetch_bot_messages(anchor):
    """
    Fetches up to 200 messages sent by the bot (DM + stream),
    anchored at the given message ID or 'newest'.
    """
    try:
        result = client.get_messages({
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


# ============================================================
#  DELETE OLD DIRECT MESSAGES (48 HOURS)
# ============================================================

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
                result = client.call_endpoint(
                    url=f"/messages/{msg_id}",
                    method="DELETE"
                )
                if result["result"] == "success":
                    deleted += 1
                else:
                    print(f"Failed to delete DM {msg_id}: {result}")
            except Exception as e:
                print(f"Error deleting DM {msg_id}:", e)

    return deleted


# ============================================================
#  DELETE OLD STREAM MESSAGES (3 DAYS)
#  (Skip spring/Txt)
# ============================================================

def delete_old_stream_messages(messages):
    now = datetime.now(timezone.utc)
    cutoff = now - STREAM_DELETE_OLDER_THAN

    deleted = 0

    for msg in messages:
        if msg["type"] != "stream":
            continue

        stream_name = msg.get("display_recipient")
        topic = msg.get("subject")

        # Skip excluded stream/topic
        if stream_name == EXCLUDED_STREAM and topic == EXCLUDED_TOPIC:
            continue

        ts = datetime.fromtimestamp(msg["timestamp"], timezone.utc)

        if ts < cutoff:
            time.sleep(0.15)
            msg_id = msg["id"]

            try:
                result = client.call_endpoint(
                    url=f"/messages/{msg_id}",
                    method="DELETE"
                )
                if result["result"] == "success":
                    deleted += 1
                else:
                    print(f"Failed to delete stream message {msg_id}: {result}")
            except Exception as e:
                print(f"Error deleting stream message {msg_id}:", e)

    return deleted


# ============================================================
#  MAIN CLEANUP LOOP (Unified Pagination)
# ============================================================

def run_cleanup():
    print("Starting cleanup worker...")

    anchor = "newest"
    total_dm_deleted = 0
    total_stream_deleted = 0

    while True:
        messages = fetch_bot_messages(anchor)

        if not messages:
            break

        print(f"\nFetched {len(messages)} messages at anchor={anchor}")

        oldest_id = messages[0]["id"]

        # Delete DMs
        dm_deleted = delete_old_direct_messages(messages)
        total_dm_deleted += dm_deleted
        print(f"Deleted {dm_deleted} DIRECT messages in this batch")

        # Delete stream messages
        stream_deleted = delete_old_stream_messages(messages)
        total_stream_deleted += stream_deleted
        print(f"Deleted {stream_deleted} STREAM messages in this batch")

        anchor = oldest_id

        if len(messages) < 200:
            break

        print("Waiting 60 seconds before next batch...")
        time.sleep(60)

    print("\n=== CLEANUP SUMMARY ===")
    print(f"Total DIRECT messages deleted: {total_dm_deleted}")
    print(f"Total STREAM messages deleted: {total_stream_deleted}")
    print("Cleanup complete.")


# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    run_cleanup()