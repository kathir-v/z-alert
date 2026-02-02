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

# DM deletion threshold: 24 hours
DM_DELETE_OLDER_THAN = timedelta(hours=24)

# Stream deletion threshold: 3 days
STREAM_DELETE_OLDER_THAN = timedelta(days=3)

# Stream/topic to EXCLUDE from deletion
EXCLUDED_STREAM = "spring"
EXCLUDED_TOPIC = "Txt"


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
            "num_before": 200,
            "num_after": 0,
            "narrow": [
                {"operator": "sender", "operand": BOT_SENDER},
                {"operator": "is", "operand": "private"},
            ]
        })

        if result["result"] != "success":
            print("Error fetching DM messages:", result)
            return []

        return result["messages"]

    except Exception as e:
        print("Error in fetch_bot_direct_messages:", e)
        return []


# ============================================================
#  DELETE OLD DIRECT MESSAGES (24 HOURS)
# ============================================================

def delete_old_direct_messages():
    now = datetime.now(timezone.utc)
    cutoff = now - DM_DELETE_OLDER_THAN

    anchor = "newest"
    total_deleted = 0

    print("\n=== Deleting old DIRECT MESSAGES from noti (older than 24 hours) ===")

    while True:
        messages = fetch_bot_direct_messages(anchor)

        if not messages:
            break

        print(f"Fetched {len(messages)} DM messages at anchor={anchor}")

        oldest_id = messages[0]["id"]
        batch_deleted = 0

        for msg in messages:
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
                        batch_deleted += 1
                    else:
                        print(f"Failed to delete DM {msg_id}: {result}")
                except Exception as e:
                    print(f"Error deleting DM {msg_id}:", e)

        total_deleted += batch_deleted
        print(f"Deleted in this DM batch: {batch_deleted}")

        anchor = oldest_id

        if len(messages) < 200:
            break

        print("Waiting 60 seconds before next DM batch...")
        time.sleep(60)

    print(f"Total DIRECT MESSAGES deleted: {total_deleted}")


# ============================================================
#  FETCH ALL BOT-SENT STREAM MESSAGES (PAGINATED)
#  (Optimized: fetch ALL streams at once, not per-stream)
# ============================================================

def fetch_bot_stream_messages(anchor):
    """
    Fetches up to 200 stream messages sent by the bot,
    anchored at the given message ID or 'newest'.
    """
    try:
        result = client.get_messages({
            "anchor": anchor,
            "num_before": 200,
            "num_after": 0,
            "narrow": [
                {"operator": "sender", "operand": BOT_SENDER},
                {"operator": "is", "operand": "stream"},
            ]
        })

        if result["result"] != "success":
            print("Error fetching stream messages:", result)
            return []

        return result["messages"]

    except Exception as e:
        print("Error in fetch_bot_stream_messages:", e)
        return []


# ============================================================
#  DELETE OLD STREAM MESSAGES (3 DAYS)
#  (Optimized: single unified fetch, skip spring/Txt)
# ============================================================

def delete_old_stream_messages():
    now = datetime.now(timezone.utc)
    cutoff = now - STREAM_DELETE_OLDER_THAN

    print("\n=== Deleting old STREAM MESSAGES from noti (older than 3 days) ===")
    print(f"Excluding stream/topic: {EXCLUDED_STREAM}/{EXCLUDED_TOPIC}")

    anchor = "newest"
    total_deleted = 0

    while True:
        messages = fetch_bot_stream_messages(anchor)

        if not messages:
            break

        print(f"Fetched {len(messages)} stream messages at anchor={anchor}")

        oldest_id = messages[0]["id"]
        batch_deleted = 0

        for msg in messages:
            stream_name = msg.get("display_recipient")
            topic = msg.get("subject")

            # Skip the excluded stream/topic
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
                        batch_deleted += 1
                    else:
                        print(f"Failed to delete message {msg_id}: {result}")
                except Exception as e:
                    print(f"Error deleting message {msg_id}:", e)

        total_deleted += batch_deleted
        print(f"Deleted in this stream batch: {batch_deleted}")

        anchor = oldest_id

        if len(messages) < 200:
            break

        print("Waiting 60 seconds before next stream batch...")
        time.sleep(60)

    print(f"\nTotal STREAM MESSAGES deleted: {total_deleted}")


# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    print("Starting cleanup worker...")

    delete_old_direct_messages()
    delete_old_stream_messages()

    print("Cleanup complete.")