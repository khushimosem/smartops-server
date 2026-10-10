
import os
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL)


@app.route("/log", methods=["POST"])
def log_event():
    data = request.get_json(silent=True) or {}

    table_id = data.get("tableId")
    event = data.get("event")
    icon = data.get("icon")
    time_str = data.get("time")

    if table_id is None or not event:
        return jsonify({"error": "Invalid data"}), 400

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO logs (table_id, event, icon, event_time)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (table_id, event, icon, time_str)
                )

        return jsonify({"status": "success"}), 200

    except Exception:
        app.logger.exception("Failed to save event to PostgreSQL")
        return jsonify({"error": "Failed to save event"}), 500


@app.route("/data", methods=["GET"])
def get_data():
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (table_id)
                        table_id, event, icon, event_time, created_at
                    FROM logs
                    ORDER BY table_id, created_at DESC, id DESC
                    """
                )
                rows = cur.fetchall()

                cur.execute(
                    "SELECT COUNT(*) AS total FROM logs WHERE event = %s",
                    ("Customer_Called",)
                )
                total_calls = cur.fetchone()["total"]

        live_status = []
        open_calls = 0

        for row in rows:
            created_at = row["created_at"]
            minutes_ago = int(
                (datetime.now(created_at.tzinfo) - created_at).total_seconds() / 60
            ) if created_at.tzinfo else int(
                (datetime.now() - created_at).total_seconds() / 60
            )

            live_status.append({
                "table_id": row["table_id"],
                "status": row["event"],
                "minutes_ago": max(0, minutes_ago)
            })

            if row["event"] != "Table_Closed":
                open_calls += 1

        return jsonify({
            "live_status": live_status,
            "analytics": {
                "total": total_calls,
                "open": open_calls
            }
        })

    except Exception:
        app.logger.exception("Failed to read events from PostgreSQL")
        return jsonify({"error": "Failed to read events"}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))
