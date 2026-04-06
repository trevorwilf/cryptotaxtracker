"""
CryptoTaxTracker Data Remediation Script
Run in Jupyter notebook with DATABASE_URL or individual DB env vars set.

Usage:
  DRY_RUN = True   → report only, no changes
  DRY_RUN = False  → apply all fixes
"""
import os
import json

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    raise

DB_URL = os.environ.get("POSTGRES_URL", os.environ.get("DATABASE_URL",
    "postgresql://hbot:PASSWORD@127.0.0.1:5432/hummingbot_api"))

DRY_RUN = True  # Set to False to actually modify data

conn = psycopg2.connect(DB_URL)
cur = conn.cursor(cursor_factory=RealDictCursor)

print("=" * 60)
print("CRYPTOTAXTRACKER DATA REMEDIATION")
print(f"Mode: {'DRY RUN' if DRY_RUN else '*** LIVE ***'}")
print("=" * 60)

# --- 1. Report zero-amount NonKYC deposits ---
cur.execute("""
    SELECT id, exchange_id, asset, amount, tx_hash,
           raw_data::jsonb->>'quantity' as raw_qty,
           raw_data::jsonb->>'transactionid' as raw_txid,
           raw_data::jsonb->>'address' as raw_addr
    FROM tax.deposits
    WHERE exchange = 'nonkyc' AND amount = 0
      AND raw_data IS NOT NULL
""")
bad_deposits = cur.fetchall()
print(f"\n[1] Zero-amount NonKYC deposits: {len(bad_deposits)}")
for d in bad_deposits:
    print(f"  id={d['id']}, eid={str(d['exchange_id'])[:25]}, "
          f"raw_qty={d['raw_qty']}, raw_txid={str(d['raw_txid'] or 'NONE')[:20]}")

# --- 2. Report zero-amount NonKYC withdrawals ---
cur.execute("""
    SELECT id, exchange_id, asset, amount, tx_hash,
           raw_data::jsonb->>'quantity' as raw_qty,
           raw_data::jsonb->>'transactionid' as raw_txid,
           raw_data::jsonb->>'address' as raw_addr
    FROM tax.withdrawals
    WHERE exchange = 'nonkyc' AND amount = 0
      AND raw_data IS NOT NULL
""")
bad_wds = cur.fetchall()
print(f"\n[2] Zero-amount NonKYC withdrawals: {len(bad_wds)}")
for w in bad_wds:
    print(f"  id={w['id']}, eid={str(w['exchange_id'])[:25]}, "
          f"raw_qty={w['raw_qty']}, raw_txid={str(w['raw_txid'] or 'NONE')[:20]}")

# --- 3. Report duplicate NonKYC deposits ---
cur.execute("""
    SELECT d1.id as api_id, d2.id as csv_id, d1.exchange_id as api_eid,
           d2.exchange_id as csv_eid, d1.asset, d2.amount,
           d1.raw_data::jsonb->>'transactionid' as api_txid,
           d2.tx_hash as csv_txid
    FROM tax.deposits d1
    JOIN tax.deposits d2 ON d1.exchange = d2.exchange
        AND d1.asset = d2.asset
        AND d1.id < d2.id
        AND d1.raw_data::jsonb->>'transactionid' = d2.exchange_id
    WHERE d1.exchange = 'nonkyc'
""")
dup_deps = cur.fetchall()
print(f"\n[3] Duplicate NonKYC deposit pairs: {len(dup_deps)}")
for dd in dup_deps:
    print(f"  API row {dd['api_id']} <-> CSV row {dd['csv_id']}, "
          f"asset={dd['asset']}, amount={dd['amount']}")

# --- 4. Report junk pool rows ---
cur.execute("""
    SELECT COUNT(*) as cnt FROM tax.pool_activity
    WHERE exchange = 'nonkyc'
      AND (pool_name IS NULL OR pool_name = '')
      AND amount_in = 0 AND amount_out = 0
""")
junk_pools = cur.fetchone()['cnt']
print(f"\n[4] Junk NonKYC pool rows: {junk_pools}")

# --- 5. Report negative holding_days ---
try:
    cur.execute("SELECT COUNT(*) as cnt FROM tax.disposals_v4 WHERE holding_days < 0")
    neg_hd = cur.fetchone()['cnt']
    print(f"\n[5] Negative holding_days in disposals_v4: {neg_hd}")
except Exception:
    print("\n[5] disposals_v4 table not found (may not have run compute yet)")

print("\n" + "=" * 60)
if DRY_RUN:
    print("DRY RUN — no changes made. Set DRY_RUN = False to remediate.")
else:
    print("APPLYING FIXES...")

    # Fix 1: Repair zero-amount deposits from raw_data
    for d in bad_deposits:
        if d['raw_qty'] and d['raw_qty'] != '0':
            cur.execute("""
                UPDATE tax.deposits SET
                    amount = %s::numeric,
                    tx_hash = COALESCE(NULLIF(%s, ''), tx_hash),
                    address = COALESCE(NULLIF(%s, ''), address)
                WHERE id = %s
            """, (d['raw_qty'], d['raw_txid'] or '', d['raw_addr'] or '', d['id']))
            print(f"  Repaired deposit {d['id']}: amount={d['raw_qty']}")

    # Fix 2: Repair zero-amount withdrawals from raw_data
    for w in bad_wds:
        if w['raw_qty'] and w['raw_qty'] != '0':
            cur.execute("""
                UPDATE tax.withdrawals SET
                    amount = %s::numeric,
                    tx_hash = COALESCE(NULLIF(%s, ''), tx_hash),
                    address = COALESCE(NULLIF(%s, ''), address)
                WHERE id = %s
            """, (w['raw_qty'], w['raw_txid'] or '', w['raw_addr'] or '', w['id']))
            print(f"  Repaired withdrawal {w['id']}: amount={w['raw_qty']}")

    # Fix 3: Merge duplicate deposits (keep CSV row with better data, delete API row)
    for dd in dup_deps:
        cur.execute("DELETE FROM tax.deposits WHERE id = %s", (dd['api_id'],))
        print(f"  Deleted duplicate API deposit {dd['api_id']} (kept CSV row {dd['csv_id']})")

    # Fix 4: Delete junk pool rows
    cur.execute("""
        DELETE FROM tax.pool_activity
        WHERE exchange = 'nonkyc'
          AND (pool_name IS NULL OR pool_name = '')
          AND amount_in = 0 AND amount_out = 0
    """)
    print(f"  Deleted {cur.rowcount} junk pool rows")

    conn.commit()
    print("\nAll fixes committed.")

cur.close()
conn.close()
print("\nDone.")
