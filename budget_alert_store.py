"""
Danh sách CID theo dõi cảnh báo ngân sách (< 4 ngày) + lịch sử check/alert.
PostgreSQL (DATABASE_URL).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import psycopg


def init_budget_alert_watch_table(database_url: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS budget_alert_watch (
      customer_id TEXT NOT NULL PRIMARY KEY,
      label TEXT NOT NULL DEFAULT '',
      mcc_id TEXT NOT NULL DEFAULT '',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_check_at TEXT NOT NULL DEFAULT '',
      last_alert_at TEXT NOT NULL DEFAULT '',
      last_status TEXT NOT NULL DEFAULT '',
      last_error TEXT NOT NULL DEFAULT '',
      last_total_daily_micros BIGINT NOT NULL DEFAULT 0,
      last_remaining_micros BIGINT,
      last_days_remaining DOUBLE PRECISION
    );
    """
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def _row_to_dict(row: tuple) -> dict:
    return {
        "customer_id": row[0],
        "label": row[1] or "",
        "mcc_id": row[2] or "",
        "active": bool(row[3]),
        "created_at": row[4] or "",
        "updated_at": row[5] or "",
        "last_check_at": row[6] or "",
        "last_alert_at": row[7] or "",
        "last_status": row[8] or "",
        "last_error": row[9] or "",
        "last_total_daily_micros": int(row[10] or 0),
        "last_remaining_micros": int(row[11]) if row[11] is not None else None,
        "last_days_remaining": float(row[12]) if row[12] is not None else None,
    }


def list_watch(database_url: str, *, active_only: bool = False) -> List[dict]:
    sql = """
        SELECT customer_id, label, mcc_id, active, created_at, updated_at,
               last_check_at, last_alert_at, last_status, last_error,
               last_total_daily_micros, last_remaining_micros, last_days_remaining
        FROM budget_alert_watch
    """
    if active_only:
        sql += " WHERE active = TRUE"
    sql += " ORDER BY updated_at DESC"
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [_row_to_dict(row) for row in cur.fetchall()]


def get_watch(database_url: str, customer_id: str) -> Optional[dict]:
    cid = (customer_id or "").strip().replace("-", "")
    if not cid:
        return None
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT customer_id, label, mcc_id, active, created_at, updated_at,
                       last_check_at, last_alert_at, last_status, last_error,
                       last_total_daily_micros, last_remaining_micros, last_days_remaining
                FROM budget_alert_watch WHERE customer_id = %s LIMIT 1
                """,
                (cid,),
            )
            row = cur.fetchone()
            return _row_to_dict(row) if row else None


def upsert_watch(
    database_url: str,
    *,
    customer_id: str,
    mcc_id: str,
    label: str = "",
    active: bool = True,
) -> None:
    cid = (customer_id or "").strip().replace("-", "")
    mid = (mcc_id or "").strip().replace("-", "")
    if len(cid) != 10 or not cid.isdigit():
        raise ValueError("customer_id phải đúng 10 chữ số.")
    if len(mid) != 10 or not mid.isdigit():
        raise ValueError("mcc_id phải đúng 10 chữ số.")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO budget_alert_watch (
                  customer_id, label, mcc_id, active, created_at, updated_at
                ) VALUES (%(cid)s, %(label)s, %(mid)s, %(active)s, %(now)s, %(now)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                  mcc_id = EXCLUDED.mcc_id,
                  label = EXCLUDED.label,
                  active = EXCLUDED.active,
                  updated_at = EXCLUDED.updated_at
                """,
                {
                    "cid": cid,
                    "label": (label or "").strip(),
                    "mid": mid,
                    "active": active,
                    "now": now,
                },
            )


def update_watch_check_result(
    database_url: str,
    *,
    customer_id: str,
    last_status: str,
    last_error: str = "",
    last_total_daily_micros: int = 0,
    last_remaining_micros: Optional[int] = None,
    last_days_remaining: Optional[float] = None,
    last_alert_at: Optional[str] = None,
) -> None:
    cid = (customer_id or "").strip().replace("-", "")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            if last_alert_at is not None:
                cur.execute(
                    """
                    UPDATE budget_alert_watch SET
                      last_check_at = %(check_at)s,
                      last_alert_at = %(alert_at)s,
                      last_status = %(status)s,
                      last_error = %(err)s,
                      last_total_daily_micros = %(daily)s,
                      last_remaining_micros = %(rem)s,
                      last_days_remaining = %(days)s,
                      updated_at = %(check_at)s
                    WHERE customer_id = %(cid)s
                    """,
                    {
                        "cid": cid,
                        "check_at": now,
                        "alert_at": last_alert_at,
                        "status": last_status,
                        "err": last_error or "",
                        "daily": last_total_daily_micros,
                        "rem": last_remaining_micros,
                        "days": last_days_remaining,
                    },
                )
            else:
                cur.execute(
                    """
                    UPDATE budget_alert_watch SET
                      last_check_at = %(check_at)s,
                      last_status = %(status)s,
                      last_error = %(err)s,
                      last_total_daily_micros = %(daily)s,
                      last_remaining_micros = %(rem)s,
                      last_days_remaining = %(days)s,
                      updated_at = %(check_at)s
                    WHERE customer_id = %(cid)s
                    """,
                    {
                        "cid": cid,
                        "check_at": now,
                        "status": last_status,
                        "err": last_error or "",
                        "daily": last_total_daily_micros,
                        "rem": last_remaining_micros,
                        "days": last_days_remaining,
                    },
                )


def set_watch_active(database_url: str, customer_id: str, active: bool) -> bool:
    cid = (customer_id or "").strip().replace("-", "")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE budget_alert_watch SET active = %s, updated_at = %s WHERE customer_id = %s",
                (active, now, cid),
            )
            return cur.rowcount > 0


def delete_watch(database_url: str, customer_id: str) -> bool:
    cid = (customer_id or "").strip().replace("-", "")
    if not cid:
        return False
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM budget_alert_watch WHERE customer_id = %s", (cid,))
            return cur.rowcount > 0
