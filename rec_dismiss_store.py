"""
CID bật tự động bỏ qua đề xuất Google Ads (quét hàng ngày trên Railway).
PostgreSQL (DATABASE_URL).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import psycopg


def init_recommendation_auto_dismiss_table(database_url: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS recommendation_auto_dismiss (
      customer_id TEXT NOT NULL PRIMARY KEY,
      label TEXT NOT NULL DEFAULT '',
      mcc_id TEXT NOT NULL DEFAULT '',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      last_run_at TEXT NOT NULL DEFAULT '',
      last_run_date TEXT NOT NULL DEFAULT '',
      last_status TEXT NOT NULL DEFAULT '',
      last_error TEXT NOT NULL DEFAULT '',
      last_dismissed_count INTEGER NOT NULL DEFAULT 0
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
        "last_run_at": row[6] or "",
        "last_run_date": row[7] or "",
        "last_status": row[8] or "",
        "last_error": row[9] or "",
        "last_dismissed_count": int(row[10] or 0),
    }


def list_auto_dismiss(database_url: str, *, active_only: bool = False) -> List[dict]:
    sql = """
        SELECT customer_id, label, mcc_id, active, created_at, updated_at,
               last_run_at, last_run_date, last_status, last_error, last_dismissed_count
        FROM recommendation_auto_dismiss
    """
    if active_only:
        sql += " WHERE active = TRUE"
    sql += " ORDER BY updated_at DESC"
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            return [_row_to_dict(row) for row in cur.fetchall()]


def get_auto_dismiss(database_url: str, customer_id: str) -> Optional[dict]:
    cid = (customer_id or "").strip().replace("-", "")
    if not cid:
        return None
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT customer_id, label, mcc_id, active, created_at, updated_at,
                       last_run_at, last_run_date, last_status, last_error, last_dismissed_count
                FROM recommendation_auto_dismiss WHERE customer_id = %s LIMIT 1
                """,
                (cid,),
            )
            row = cur.fetchone()
            return _row_to_dict(row) if row else None


def upsert_auto_dismiss(
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
    if mid and (len(mid) != 10 or not mid.isdigit()):
        raise ValueError("mcc_id phải đúng 10 chữ số.")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO recommendation_auto_dismiss (
                  customer_id, label, mcc_id, active, created_at, updated_at
                ) VALUES (%(cid)s, %(label)s, %(mid)s, %(active)s, %(now)s, %(now)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                  mcc_id = CASE
                    WHEN TRIM(COALESCE(EXCLUDED.mcc_id, '')) <> '' THEN EXCLUDED.mcc_id
                    ELSE recommendation_auto_dismiss.mcc_id
                  END,
                  label = CASE
                    WHEN TRIM(COALESCE(EXCLUDED.label, '')) <> '' THEN EXCLUDED.label
                    WHEN TRIM(COALESCE(recommendation_auto_dismiss.label, '')) <> ''
                      THEN recommendation_auto_dismiss.label
                    ELSE EXCLUDED.label
                  END,
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


def update_auto_dismiss_run(
    database_url: str,
    *,
    customer_id: str,
    last_status: str,
    last_run_date: str = "",
    last_error: str = "",
    last_dismissed_count: int = 0,
) -> None:
    cid = (customer_id or "").strip().replace("-", "")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE recommendation_auto_dismiss SET
                  last_run_at = %(run_at)s,
                  last_run_date = %(run_date)s,
                  last_status = %(status)s,
                  last_error = %(err)s,
                  last_dismissed_count = %(count)s,
                  updated_at = %(run_at)s
                WHERE customer_id = %(cid)s
                """,
                {
                    "cid": cid,
                    "run_at": now,
                    "run_date": last_run_date or "",
                    "status": last_status,
                    "err": last_error or "",
                    "count": int(last_dismissed_count or 0),
                },
            )


def set_auto_dismiss_active(database_url: str, customer_id: str, active: bool) -> bool:
    cid = (customer_id or "").strip().replace("-", "")
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE recommendation_auto_dismiss SET active = %s, updated_at = %s WHERE customer_id = %s",
                (active, now, cid),
            )
            return cur.rowcount > 0


def delete_auto_dismiss(database_url: str, customer_id: str) -> bool:
    cid = (customer_id or "").strip().replace("-", "")
    if not cid:
        return False
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM recommendation_auto_dismiss WHERE customer_id = %s", (cid,))
            return cur.rowcount > 0
