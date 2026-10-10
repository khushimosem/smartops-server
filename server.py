
import os
import csv
import io
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL)


def get_history_filters():
    """Build safe filters for history and CSV export."""
    conditions = []
    params = []

    table_id = request.args.get("table_id")
    event = request.args.get("event")

    if table_id:
        try:
            table_id = int(table_id)
        except ValueError:
            raise ValueError("table_id must be a number")

        conditions.append("table_id = %s")
        params.append(table_id)

    if event:
        conditions.append("event = %s")
        params.append(event)

    where_clause = (
        " WHERE " + " AND ".join(conditions)
        if conditions else ""
    )

    return where_clause, params


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
                    INSERT INTO logs
                        (table_id, event, icon, event_time)
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
                    """
                    SELECT COUNT(*) AS total
                    FROM logs
                    WHERE event = %s
                    """,
                    ("Customer_Called",)
                )
                total_calls = cur.fetchone()["total"]

        live_status = []
        open_calls = 0

        for row in rows:
            created_at = row["created_at"]

            if created_at.tzinfo:
                minutes_ago = int(
                    (
                        datetime.now(created_at.tzinfo) - created_at
                    ).total_seconds() / 60
                )
            else:
                minutes_ago = int(
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
        }), 200

    except Exception:
        app.logger.exception("Failed to read events from PostgreSQL")
        return jsonify({"error": "Failed to read events"}), 500


@app.route("/history", methods=["GET"])
def get_history():
    """
    Retrieve stored event records.
    Optional filters: table_id, event.
    Pagination: limit and offset.
    """
    try:
        where_clause, params = get_history_filters()

        try:
            limit = int(request.args.get("limit", 500))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return jsonify({
                "error": "limit and offset must be integers"
            }), 400

        if limit < 1 or limit > 5000 or offset < 0:
            return jsonify({
                "error": "limit must be 1-5000 and offset must be 0 or greater"
            }), 400

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT COUNT(*) AS total FROM logs" + where_clause,
                    params
                )
                total = cur.fetchone()["total"]

                cur.execute(
                    """
                    SELECT id, created_at, table_id, event,
                           icon, event_time
                    FROM logs
                    """
                    + where_clause
                    + " ORDER BY created_at DESC, id DESC LIMIT %s OFFSET %s",
                    params + [limit, offset]
                )
                rows = cur.fetchall()

        history = []
        for row in rows:
            history.append({
                "id": row["id"],
                "created_at": (
                    row["created_at"].isoformat()
                    if row["created_at"] else None
                ),
                "table_id": row["table_id"],
                "event": row["event"],
                "icon": row["icon"],
                "event_time": row["event_time"]
            })

        return jsonify({
            "total": total,
            "limit": limit,
            "offset": offset,
            "count": len(history),
            "history": history
        }), 200

    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("Failed to retrieve event history")
        return jsonify({"error": "Failed to retrieve event history"}), 500


@app.route("/history.csv", methods=["GET"])
def export_history_csv():
    """Export all matching history records to a CSV file."""
    try:
        where_clause, params = get_history_filters()

        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, created_at, table_id, event,
                           icon, event_time
                    FROM logs
                    """
                    + where_clause
                    + " ORDER BY created_at DESC, id DESC",
                    params
                )
                rows = cur.fetchall()

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow([
            "ID", "Created At", "Table ID",
            "Event", "Icon", "Event Time"
        ])

        for row in rows:
            writer.writerow([
                row["id"],
                row["created_at"].isoformat()
                if row["created_at"] else "",
                row["table_id"],
                row["event"],
                row["icon"] or "",
                row["event_time"] or ""
            ])

        return Response(
            "\ufeff" + output.getvalue(),
            mimetype="text/csv; charset=utf-8",
            headers={
                "Content-Disposition":
                    'attachment; filename="restaurant_event_history.csv"'
            }
        )

    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception:
        app.logger.exception("Failed to export event history")
        return jsonify({"error": "Failed to export event history"}), 500


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000"))
    )
