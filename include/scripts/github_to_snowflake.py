import os
import json
import logging
from datetime import datetime, timezone

import requests
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com/events"
REQUEST_TIMEOUT_SECONDS = 30


def _load_private_key():
    """Load the RSA private key for Snowflake key-pair auth from a PEM file.

    Handles optional passphrase decryption and formats the key into the raw 
    DER/PKCS8 bytes required by the Snowflake Python connector.
    """

    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    passphrase = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None

    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )

    return p_key.private_bytes(
        encoding=Encoding.DER,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    )


def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        # Disable bulk array-binding 
        session_parameters={"CLIENT_STAGE_ARRAY_BINDING_THRESHOLD": 0},
    )


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.exceptions.RequestException,)),
    reraise=True,
)
def fetch_github_events():
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "developer-metrics-platform",
        "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
    }
    response = requests.get(GITHUB_API_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)

    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining is not None and int(remaining) < 5:
        reset = response.headers.get("X-RateLimit-Reset")
        logger.warning("GitHub rate limit nearly exhausted (remaining=%s, reset=%s)", remaining, reset)

    response.raise_for_status()
    return response.json()


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze_github_events (
            event_id STRING PRIMARY KEY,
            ingested_at TIMESTAMP_NTZ,
            raw_payload VARIANT
        )
        """
    )


def merge_events(cursor, events, ingested_at):
    """Idempotent load to prevent duplicates from overlapping API windows.
   
    Staging payloads as strings because Snowflake executemany() breaks 
    with PARSE_JSON in VALUES lists. Parsing happens during the MERGE instead.
    """
    if not events:
        logger.info("No events returned by GitHub API; nothing to load.")
        return 0

    rows = [(e.get("id"), ingested_at, json.dumps(e)) for e in events if e.get("id")]
    skipped = len(events) - len(rows)
    if skipped:
        logger.warning("Skipped %d event(s) with no id", skipped)

    if not rows:
        return 0

    cursor.execute(
        """
        CREATE OR REPLACE TEMPORARY TABLE stg_github_events (
            event_id STRING,
            ingested_at TIMESTAMP_NTZ,
            raw_payload_str STRING
        )
        """
    )
    cursor.executemany(
        "INSERT INTO stg_github_events (event_id, ingested_at, raw_payload_str) VALUES (%s, %s, %s)",
        rows,
    )
    cursor.execute(
        """
        MERGE INTO bronze_github_events AS tgt
        USING (
            SELECT event_id, ingested_at, PARSE_JSON(raw_payload_str) AS raw_payload
            FROM stg_github_events
        ) AS src
        ON tgt.event_id = src.event_id
        WHEN NOT MATCHED THEN
            INSERT (event_id, ingested_at, raw_payload)
            VALUES (src.event_id, src.ingested_at, src.raw_payload)
        """
    )
    return cursor.rowcount or 0


def extract_and_load():
    events = fetch_github_events()
    ingested_at = datetime.now(timezone.utc)

    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        try:
            ensure_table(cursor)
            inserted = merge_events(cursor, events, ingested_at)
            conn.commit()
            logger.info(
                "Fetched %d event(s) from GitHub, inserted %d new row(s) into bronze_github_events.",
                len(events),
                inserted,
            )
        except Exception:
            conn.rollback()
            logger.exception("Load failed; transaction rolled back.")
            raise
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    extract_and_load()