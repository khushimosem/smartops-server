76
import os
import csv
import io
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify, Response

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")


# ==========================================
# DATABASE CONNECTION
# ==========================================

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg2.connect(DATABASE_URL)


# ==========================================
# HISTORY FILTERS
# ==========================================

def get_history_filters():
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


# ==========================================
# RECEIVE ESP8266 EVENTS
# ==========================================

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


# ==========================================
# DASHBOARD AND MONITOR DATA
# ==========================================

@app.route("/data", methods=["GET"])
def get_data():
    try:
        with get_db_connection() as conn:
            with conn.cursor(
                cursor_factory=RealDictCursor
            ) as cur:

                # 1. Latest event for each table.
                # Keep the latest status across all dates.
                cur.execute("""
                    SELECT DISTINCT ON (table_id)
                        table_id, event, icon, event_time, created_at
                    FROM logs
                    ORDER BY table_id, created_at DESC, id DESC
                """)
                rows = cur.fetchall()

                # 2. Today's customer calls in IST.
                # created_at is assumed to contain UTC timestamps
                # stored without a timezone.
                cur.execute("""
                    SELECT COUNT(*) AS total
                    FROM logs
                    WHERE event LIKE 'Customer_Called%'
                      AND created_at >= (
                          (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
                          ::date::timestamp - INTERVAL '5 hours 30 minutes'
                      )
                      AND created_at < (
                          ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
                           ::date + 1)::timestamp
                          - INTERVAL '5 hours 30 minutes'
                      )
                """)
                total_calls = cur.fetchone()["total"]

                # 3. Today's events for response/delivery averages.
                cur.execute("""
                    SELECT table_id, event, created_at
                    FROM logs
                    WHERE created_at >= (
                          (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
                          ::date::timestamp - INTERVAL '5 hours 30 minutes'
                      )
                      AND created_at < (
                          ((CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Kolkata')
                           ::date + 1)::timestamp
                          - INTERVAL '5 hours 30 minutes'
                      )
                    ORDER BY table_id, created_at ASC, id ASC
                """)
                events = cur.fetchall()

        # 4. Current table status and age of latest event.
        live_status = []
        open_calls = 0

        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)

        for row in rows:
            created_at = row["created_at"]

            if created_at.tzinfo is not None:
                created_at = created_at.astimezone(
                    timezone.utc
                ).replace(tzinfo=None)

            minutes_ago = int(
                (now_utc - created_at).total_seconds() / 60
            )

            live_status.append({
                "table_id": row["table_id"],
                "status": row["event"],
                "minutes_ago": max(0, minutes_ago)
            })

            if not str(row["event"]).startswith("Table_Closed"):
                open_calls += 1

        # 5. Calculate today's average response/delivery times.
        response_times = []
        delivery_times = []
        pending_call = {}
        pending_response = {}

        for item in events:
            table_id = item["table_id"]
            event = item["event"]
            timestamp = item["created_at"]

            if event.startswith("Customer_Called"):
                pending_call[table_id] = timestamp

            elif event.startswith("Waiter_Responded"):
                call_time = pending_call.pop(table_id, None)

                if call_time is not None:
                    seconds = (
                        timestamp - call_time
                    ).total_seconds()

                    if seconds >= 0:
                        response_times.append(seconds)

                pending_response[table_id] = timestamp

            elif event.startswith("Food_Delivered"):
                response_time = pending_response.pop(
                    table_id, None
                )

                if response_time is not None:
                    seconds = (
                        timestamp - response_time
                    ).total_seconds()

                    if seconds >= 0:
                        delivery_times.append(seconds)

            elif event.startswith("Table_Closed"):
                pending_call.pop(table_id, None)
                pending_response.pop(table_id, None)

        avg_response = (
            round(sum(response_times) / len(response_times), 1)
            if response_times else None
        )

        avg_delivery = (
            round(sum(delivery_times) / len(delivery_times), 1)
            if delivery_times else None
        )

        return jsonify({
            "live_status": live_status,
            "analytics": {
                "total": total_calls,
                "open": open_calls,
                "avg_response_seconds": avg_response,
                "avg_delivery_seconds": avg_delivery
            }
        }), 200

    except Exception:
        app.logger.exception("Failed to read events from PostgreSQL")
        return jsonify({"error": "Failed to read events"}), 500


# ==========================================
# HISTORICAL EVENT LOGS
# ==========================================

@app.route("/history", methods=["GET"])
def get_history():
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
            with conn.cursor(
                cursor_factory=RealDictCursor
            ) as cur:

                cur.execute(
                    "SELECT COUNT(*) AS total FROM logs" + where_clause,
                    params
                )
                total = cur.fetchone()["total"]

                cur.execute(
                    """
                    SELECT
                        id,
                        created_at AT TIME ZONE 'UTC'
                            AT TIME ZONE 'Asia/Kolkata' AS created_at,
                        table_id,
                        event,
                        icon,
                        event_time
                    FROM logs
                    """
                    + where_clause
                    + """
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s OFFSET %s
                    """,
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


# ==========================================
# EXPORT HISTORICAL LOGS TO CSV
# ==========================================

@app.route("/history.csv", methods=["GET"])
def export_history_csv():
    try:
        where_clause, params = get_history_filters()

        with get_db_connection() as conn:
            with conn.cursor(
                cursor_factory=RealDictCursor
            ) as cur:
                cur.execute(
                    """
                    SELECT
                        id,
                        created_at AT TIME ZONE 'UTC'
                            AT TIME ZONE 'Asia/Kolkata' AS created_at,
                        table_id,
                        event,
                        icon,
                        event_time
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
                row["created_at"].strftime("%d-%m-%Y, %I:%M:%S %p")
                if row["created_at"] else "",
                row["table_id"],
                row["event"],
                row["icon"] or "",
                datetime.strptime(
                    str(row["event_time"]), "%H:%M:%S"
                ).strftime("%I:%M:%S %p") if row["event_time"] else ""
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


# ==========================================
# START SERVER
# ==========================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000"))
    )
