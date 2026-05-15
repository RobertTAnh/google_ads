"""
Lưu map CID (tài khoản con) → MCC để MCP/Claude gọi API không cần truyền mcc_id.
Dùng PostgreSQL (cùng DATABASE_URL với report_projects).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

import psycopg


def init_customer_mcc_map_table(database_url: str) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS customer_mcc_map (
      customer_id TEXT NOT NULL PRIMARY KEY,
      mcc_id TEXT NOT NULL,
      label TEXT NOT NULL DEFAULT '',
      active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    """
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)


def lookup_mcc_for_customer(database_url: str, customer_id: str) -> Optional[str]:
    """Trả mcc_id (10 số) nếu có bản ghi active; ngược lại None."""
    cid = (customer_id or "").strip().replace("-", "")
    if len(cid) != 10 or not cid.isdigit():
        return None
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT mcc_id FROM customer_mcc_map WHERE customer_id = %s AND active = TRUE LIMIT 1",
                (cid,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return str(row[0] or "").strip()


def list_mappings(database_url: str) -> List[dict]:
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT customer_id, mcc_id, label, active, created_at, updated_at
                FROM customer_mcc_map
                ORDER BY updated_at DESC
                """
            )
            out: List[dict] = []
            for row in cur.fetchall():
                out.append(
                    {
                        "customer_id": row[0],
                        "mcc_id": row[1],
                        "label": row[2] or "",
                        "active": bool(row[3]),
                        "created_at": row[4] or "",
                        "updated_at": row[5] or "",
                    }
                )
            return out


def upsert_mapping(database_url: str, *, customer_id: str, mcc_id: str, label: str = "", active: bool = True) -> None:
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
                INSERT INTO customer_mcc_map (customer_id, mcc_id, label, active, created_at, updated_at)
                VALUES (%(cid)s, %(mid)s, %(label)s, %(active)s, %(now)s, %(now)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                  mcc_id = EXCLUDED.mcc_id,
                  label = EXCLUDED.label,
                  active = EXCLUDED.active,
                  updated_at = EXCLUDED.updated_at
                """,
                {"cid": cid, "mid": mid, "label": (label or "").strip(), "active": active, "now": now},
            )


def upsert_mapping_sync(
    database_url: str,
    *,
    customer_id: str,
    mcc_id: str,
    suggested_label: str = "",
    active: bool = True,
) -> None:
    """
    Upsert từ job đồng bộ API: luôn cập nhật mcc_id, active, updated_at.
    Giữ label do người dùng nhập trên web nếu đã có (không ghi đè bằng suggested_label).
    """
    cid = (customer_id or "").strip().replace("-", "")
    mid = (mcc_id or "").strip().replace("-", "")
    if len(cid) != 10 or not cid.isdigit():
        raise ValueError("customer_id phải đúng 10 chữ số.")
    if len(mid) != 10 or not mid.isdigit():
        raise ValueError("mcc_id phải đúng 10 chữ số.")
    lab = (suggested_label or "").strip()
    now = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO customer_mcc_map (customer_id, mcc_id, label, active, created_at, updated_at)
                VALUES (%(cid)s, %(mid)s, %(lab)s, %(active)s, %(now)s, %(now)s)
                ON CONFLICT (customer_id) DO UPDATE SET
                  mcc_id = EXCLUDED.mcc_id,
                  active = EXCLUDED.active,
                  updated_at = EXCLUDED.updated_at,
                  label = CASE
                    WHEN TRIM(COALESCE(customer_mcc_map.label, '')) <> ''
                    THEN customer_mcc_map.label
                    ELSE EXCLUDED.label
                  END
                """,
                {"cid": cid, "mid": mid, "lab": lab, "active": active, "now": now},
            )


def delete_mappings_for_mcc_except_customer_ids(
    database_url: str,
    *,
    mcc_id: str,
    keep_customer_ids: List[str],
) -> int:
    """
    Với một MCC: xóa khỏi map mọi CID có mcc_id trùng MCC đó mà không nằm trong keep_customer_ids
    (snapshot quét API — thường chỉ các CID đang bật). keep rỗng → xóa hết map của MCC đó.
    """
    mid = (mcc_id or "").strip().replace("-", "")
    if len(mid) != 10 or not mid.isdigit():
        raise ValueError("mcc_id phải đúng 10 chữ số.")
    ids: List[str] = []
    for x in keep_customer_ids:
        c = (x or "").strip().replace("-", "")
        if len(c) == 10 and c.isdigit():
            ids.append(c)
    ids = sorted(set(ids))
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            if not ids:
                cur.execute("DELETE FROM customer_mcc_map WHERE mcc_id = %s", (mid,))
            else:
                placeholders = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"DELETE FROM customer_mcc_map WHERE mcc_id = %s AND customer_id NOT IN ({placeholders})",
                    [mid, *ids],
                )
            return int(cur.rowcount or 0)


def delete_mapping(database_url: str, customer_id: str) -> bool:
    cid = (customer_id or "").strip().replace("-", "")
    if not cid:
        return False
    with psycopg.connect(database_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM customer_mcc_map WHERE customer_id = %s", (cid,))
            return cur.rowcount > 0
