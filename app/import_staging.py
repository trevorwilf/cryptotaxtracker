"""
Import Staging — 3-step staged import process.

Step 1: Parse file, detect format, compare rows against DB → return staged preview
Step 2: User reviews and marks each row (IMPORT / SKIP / LINK_TRANSFER)
Step 3: Only approved rows are committed to the database

All staged data lives in server memory with a 1-hour TTL.
"""
import csv
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from sqlalchemy import text

logger = logging.getLogger("tax-collector.import-staging")

D = Decimal

# ── In-memory stage storage ──────────────────────────────────────────────

_staged_imports: dict[str, dict] = {}
STAGE_TTL_SECONDS = 3600  # 1 hour


def _create_stage_id() -> str:
    return f"stg_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def _cleanup_expired_stages():
    now = time.time()
    expired = [k for k, v in _staged_imports.items() if now - v["created_at"] > STAGE_TTL_SECONDS]
    for k in expired:
        del _staged_imports[k]


def get_staged(stage_id: str) -> dict | None:
    _cleanup_expired_stages()
    return _staged_imports.get(stage_id)


def remove_staged(stage_id: str):
    _staged_imports.pop(stage_id, None)


# ── File parsing ─────────────────────────────────────────────────────────

def _file_hash(filepath: str) -> str:
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_decimal(val) -> str:
    try:
        return str(D(str(val or "0")))
    except (InvalidOperation, ValueError):
        return "0"


def _parse_ts(val) -> datetime | None:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=timezone.utc) if val.tzinfo is None else val
    s = str(val).strip()
    for fmt in ("%m/%d/%Y, %I:%M:%S %p", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            pass
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        pass
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val / 1000, tz=timezone.utc)
    try:
        return datetime.fromtimestamp(float(s) / 1000, tz=timezone.utc)
    except (ValueError, TypeError):
        return None


def _normalize_tx_hash(tx_hash: str) -> str:
    """Strip MEXC :N output index suffix for comparison."""
    if not tx_hash:
        return ""
    if ":" in tx_hash:
        base, suffix = tx_hash.rsplit(":", 1)
        if suffix.isdigit():
            return base.lower().strip()
    return tx_hash.lower().strip()


# ── Header fingerprints (from csv_importer) ──────────────────────────────

MEXC_DEPOSIT_XLSX_HEADERS = ["UID", "Status", "Time", "Crypto", "Network",
                              "Deposit Amount", "TxID", "Progress"]
MEXC_WITHDRAWAL_XLSX_HEADERS = ["UID", "Status", "Time", "Crypto", "Network",
                                 "Request Amount", "Withdrawal Address", "memo",
                                 "TxID", "Trading Fee", "Settlement Amount",
                                 "Withdrawal Descriptions"]
NONKYC_DEPOSIT_CSV_HEADERS = ["Type", "Time", "Ticker", "Amount", "ValueUsd",
                               "Confirmations", "TransactionId", "Address",
                               "isPosted", "isReversed", "isSecurityConfirm"]
NONKYC_WITHDRAWAL_CSV_HEADERS = ["Type", "Time", "Ticker", "Amount", "ValueUsd",
                                  "Address", "TransactionId", "Status"]


def parse_file(filepath: str) -> dict:
    """Parse a file without writing to DB. Returns file_info + parsed rows."""
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)
    file_size = os.path.getsize(filepath)
    sha = _file_hash(filepath)

    if ext in (".xlsx", ".xls"):
        return _parse_xlsx(filepath, filename, file_size, sha)
    elif ext in (".csv", ".tsv"):
        return _parse_csv(filepath, filename, file_size, sha)
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _parse_xlsx(filepath, filename, file_size, sha):
    import openpyxl
    wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not all_rows:
        raise ValueError("Empty file")

    headers = [str(c or "").strip() for c in all_rows[0]]

    if headers == MEXC_DEPOSIT_XLSX_HEADERS:
        return _parse_mexc_deposit_xlsx(all_rows[1:], headers, filename, file_size, sha)
    elif headers == MEXC_WITHDRAWAL_XLSX_HEADERS:
        return _parse_mexc_withdrawal_xlsx(all_rows[1:], headers, filename, file_size, sha)
    else:
        raise ValueError(f"Unknown XLSX format: headers={headers}")


def _parse_csv(filepath, filename, file_size, sha):
    with open(filepath, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = [h.strip() for h in next(reader, [])]
        data_rows = list(reader)

    if headers == NONKYC_DEPOSIT_CSV_HEADERS:
        return _parse_nonkyc_deposit_csv(data_rows, headers, filename, file_size, sha)
    elif headers == NONKYC_WITHDRAWAL_CSV_HEADERS:
        return _parse_nonkyc_withdrawal_csv(data_rows, headers, filename, file_size, sha)
    else:
        raise ValueError(f"Unknown CSV format: headers={headers}")


def _file_info(filename, file_size, sha, exchange, data_type, parser, headers):
    return {
        "filename": filename, "size_bytes": file_size, "sha256": sha,
        "detected_exchange": exchange, "detected_type": data_type,
        "parser": parser, "headers": headers,
    }


def _parse_mexc_deposit_xlsx(data_rows, headers, filename, file_size, sha):
    rows = []
    for i, row in enumerate(data_rows):
        if not row or all(c is None for c in row):
            continue
        uid, status, time_val, crypto, network, amount, txid, progress = row[:8]
        txid_str = str(txid or "")
        eid = txid_str if txid_str else f"mexc_dep_{uid}_{i+1}"
        tx_hash = txid_str.split(":")[0] if ":" in txid_str else txid_str
        ts = _parse_ts(time_val)
        rows.append({
            "row_num": i + 1,
            "parsed": {
                "exchange": "mexc", "exchange_id": eid, "asset": str(crypto or ""),
                "amount": _safe_decimal(amount), "tx_hash": tx_hash,
                "full_txid": txid_str, "address": None,
                "confirmed_at": ts.isoformat() if ts else None,
                "network": str(network or ""), "status": str(status or ""),
                "fee": None, "fee_asset": None,
            },
            "raw": dict(zip(headers, [str(c) if c is not None else "" for c in row[:8]])),
        })
    return {
        "file_info": _file_info(filename, file_size, sha, "mexc", "deposits",
                                "mexc_deposit_xlsx", headers),
        "rows": rows,
    }


def _parse_mexc_withdrawal_xlsx(data_rows, headers, filename, file_size, sha):
    rows = []
    for i, row in enumerate(data_rows):
        if not row or all(c is None for c in row):
            continue
        (uid, status, time_val, crypto, network, req_amount,
         address, memo, txid, trading_fee, settlement, desc) = row[:12]
        txid_str = str(txid or "")
        eid = txid_str if txid_str else f"mexc_wd_{uid}_{i+1}"
        ts = _parse_ts(time_val)
        rows.append({
            "row_num": i + 1,
            "parsed": {
                "exchange": "mexc", "exchange_id": eid, "asset": str(crypto or ""),
                "amount": _safe_decimal(req_amount), "tx_hash": txid_str,
                "address": str(address or ""),
                "confirmed_at": ts.isoformat() if ts else None,
                "network": str(network or ""), "status": str(status or ""),
                "fee": _safe_decimal(trading_fee), "fee_asset": str(crypto or ""),
            },
            "raw": dict(zip(headers, [str(c) if c is not None else "" for c in row[:12]])),
        })
    return {
        "file_info": _file_info(filename, file_size, sha, "mexc", "withdrawals",
                                "mexc_withdrawal_xlsx", headers),
        "rows": rows,
    }


def _parse_nonkyc_deposit_csv(data_rows, headers, filename, file_size, sha):
    rows = []
    for i, row in enumerate(data_rows):
        if not row or len(row) < len(headers):
            continue
        d = dict(zip(headers, row))
        txid = str(d.get("TransactionId", "")).strip()
        eid = f"csv-{txid}" if txid else f"csv-nok-dep-{i}"
        ts = _parse_ts(d.get("Time"))
        is_posted = d.get("isPosted", "").lower() == "true"
        rows.append({
            "row_num": i + 1,
            "parsed": {
                "exchange": "nonkyc", "exchange_id": eid, "asset": d.get("Ticker", ""),
                "amount": _safe_decimal(d.get("Amount")), "amount_usd": _safe_decimal(d.get("ValueUsd")),
                "external_tx_id": txid, "tx_hash": txid, "address": d.get("Address", ""),
                "confirmed_at": ts.isoformat() if ts else None,
                "network": None, "status": "posted" if is_posted else "pending",
                "fee": None, "fee_asset": None,
            },
            "raw": d,
        })
    return {
        "file_info": _file_info(filename, file_size, sha, "nonkyc", "deposits",
                                "nonkyc_deposit_csv", headers),
        "rows": rows,
    }


def _parse_nonkyc_withdrawal_csv(data_rows, headers, filename, file_size, sha):
    rows = []
    for i, row in enumerate(data_rows):
        if not row or len(row) < len(headers):
            continue
        d = dict(zip(headers, row))
        txid = str(d.get("TransactionId", "")).strip()
        eid = f"csv-{txid}" if txid else f"csv-nok-wd-{i}"
        ts = _parse_ts(d.get("Time"))
        rows.append({
            "row_num": i + 1,
            "parsed": {
                "exchange": "nonkyc", "exchange_id": eid, "asset": d.get("Ticker", ""),
                "amount": _safe_decimal(d.get("Amount")), "amount_usd": _safe_decimal(d.get("ValueUsd")),
                "external_tx_id": txid, "tx_hash": txid, "address": d.get("Address", ""),
                "confirmed_at": ts.isoformat() if ts else None,
                "network": None, "status": d.get("Status", ""),
                "fee": None, "fee_asset": None,
            },
            "raw": d,
        })
    return {
        "file_info": _file_info(filename, file_size, sha, "nonkyc", "withdrawals",
                                "nonkyc_withdrawal_csv", headers),
        "rows": rows,
    }


# ── Match analysis ─��─────────────────────────────────────────────────────

async def analyze_matches(session, parsed_data: dict) -> dict:
    """Compare every parsed row against existing DB records.

    Categories:
      MATCH    — exact duplicate already in DB
      TRANSFER — likely self-transfer matching another exchange
      NEW      — not found anywhere
      CONFLICT — partial match with discrepancies
    """
    exchange = parsed_data["file_info"]["detected_exchange"]
    data_type = parsed_data["file_info"]["detected_type"]
    table = "deposits" if data_type == "deposits" else "withdrawals"

    summary = {"total_rows": len(parsed_data["rows"]),
               "matches": 0, "transfers": 0, "new": 0, "conflicts": 0}

    for row in parsed_data["rows"]:
        p = row["parsed"]
        eid = p.get("exchange_id", "")
        tx_hash = p.get("tx_hash", "")
        full_txid = p.get("full_txid", tx_hash)

        # 1. Exact duplicate check by exchange_id
        match = await _check_exact_duplicate(session, table, exchange, eid, p)
        if match:
            row["status"] = "MATCH"
            row["match"] = match
            row["decision"] = "SKIP"
            row["decision_reason"] = "Exact duplicate already in database"
            row["transfer_candidates"] = []
            summary["matches"] += 1
            continue

        # 2. TX hash duplicate on same exchange
        if tx_hash:
            match = await _check_tx_hash_same_exchange(session, table, exchange, tx_hash, full_txid, p)
            if match:
                row["status"] = "MATCH"
                row["match"] = match
                row["decision"] = "SKIP"
                row["decision_reason"] = "TX hash already in database for this exchange"
                row["transfer_candidates"] = []
                summary["matches"] += 1
                continue

        # 3. TX hash cross-exchange + amount/timing → TRANSFER candidates
        transfer_candidates = []
        if tx_hash:
            tc = await _check_tx_hash_cross_exchange(session, exchange, tx_hash, full_txid, p)
            transfer_candidates.extend(tc)

        # 4. Amount + timing heuristic
        tc_amt = await _check_amount_timing(session, exchange, p, data_type)
        # Deduplicate by id
        seen_ids = {c["other_record"]["id"] for c in transfer_candidates}
        for c in tc_amt:
            if c["other_record"]["id"] not in seen_ids:
                transfer_candidates.append(c)

        if transfer_candidates:
            row["status"] = "TRANSFER"
            row["match"] = None
            row["transfer_candidates"] = transfer_candidates
            row["decision"] = None
            row["decision_reason"] = None
            summary["transfers"] += 1
        else:
            row["status"] = "NEW"
            row["match"] = None
            row["transfer_candidates"] = []
            row["decision"] = None
            row["decision_reason"] = None
            summary["new"] += 1

    parsed_data["summary"] = summary
    return parsed_data


async def _check_exact_duplicate(session, table, exchange, eid, parsed):
    if not eid:
        return None
    r = await session.execute(text(f"""
        SELECT id, exchange_id, asset, amount::text, confirmed_at
        FROM tax.{table}
        WHERE exchange = :ex AND exchange_id = :eid
        LIMIT 1
    """), {"ex": exchange, "eid": eid})
    row = r.fetchone()
    if not row:
        return None

    existing = dict(zip(r.keys(), row))
    differences = {}
    if parsed.get("amount") and existing["amount"] and parsed["amount"] != existing["amount"]:
        differences["amount"] = {"existing": existing["amount"], "imported": parsed["amount"]}

    return {
        "type": "exact_duplicate" if not differences else "partial_match",
        "existing_id": existing["id"],
        "existing_table": f"tax.{table}",
        "existing_exchange": exchange,
        "existing_exchange_id": existing["exchange_id"],
        "existing_amount": existing["amount"],
        "existing_confirmed_at": existing["confirmed_at"].isoformat() if existing["confirmed_at"] else None,
        "differences": differences,
    }


async def _check_tx_hash_same_exchange(session, table, exchange, tx_hash, full_txid, parsed):
    norm = _normalize_tx_hash(tx_hash)
    norm_full = _normalize_tx_hash(full_txid) if full_txid != tx_hash else norm
    r = await session.execute(text(f"""
        SELECT id, exchange_id, asset, amount::text, tx_hash, confirmed_at
        FROM tax.{table}
        WHERE exchange = :ex AND tx_hash IS NOT NULL AND tx_hash != ''
        LIMIT 500
    """), {"ex": exchange})
    for row in r.fetchall():
        existing = dict(zip(r.keys(), row))
        existing_norm = _normalize_tx_hash(existing["tx_hash"] or "")
        if existing_norm and (existing_norm == norm or existing_norm == norm_full):
            differences = {}
            if parsed.get("amount") and existing["amount"] and parsed["amount"] != existing["amount"]:
                differences["amount"] = {"existing": existing["amount"], "imported": parsed["amount"]}
            return {
                "type": "tx_hash_match",
                "existing_id": existing["id"],
                "existing_table": f"tax.{table}",
                "existing_exchange": exchange,
                "existing_exchange_id": existing["exchange_id"],
                "existing_amount": existing["amount"],
                "existing_confirmed_at": existing["confirmed_at"].isoformat() if existing["confirmed_at"] else None,
                "differences": differences,
            }
    return None


async def _check_tx_hash_cross_exchange(session, exchange, tx_hash, full_txid, parsed):
    """Check for matching tx_hash on OTHER exchanges in both deposits and withdrawals."""
    candidates = []
    norm = _normalize_tx_hash(tx_hash)
    norm_full = _normalize_tx_hash(full_txid) if full_txid != tx_hash else norm

    for other_table in ("deposits", "withdrawals"):
        r = await session.execute(text(f"""
            SELECT id, exchange, asset, amount::text, tx_hash, address, confirmed_at
            FROM tax.{other_table}
            WHERE exchange != :ex AND tx_hash IS NOT NULL AND tx_hash != ''
            LIMIT 1000
        """), {"ex": exchange})
        for row in r.fetchall():
            existing = dict(zip(r.keys(), row))
            existing_norm = _normalize_tx_hash(existing["tx_hash"] or "")
            if existing_norm and (existing_norm == norm or existing_norm == norm_full):
                direction = (f"This {exchange.upper()} {parsed.get('asset', '?')} may match "
                             f"a {existing['exchange'].upper()} {other_table.rstrip('s')}")
                candidates.append({
                    "confidence": "high",
                    "evidence": ["tx_hash_cross_exchange_match"],
                    "direction": direction,
                    "other_record": {
                        "table": f"tax.{other_table}",
                        "id": existing["id"],
                        "exchange": existing["exchange"],
                        "asset": existing["asset"],
                        "amount": existing["amount"],
                        "tx_hash": existing["tx_hash"],
                        "address": existing.get("address"),
                        "confirmed_at": existing["confirmed_at"].isoformat() if existing["confirmed_at"] else None,
                    },
                })
    return candidates


async def _check_amount_timing(session, exchange, parsed, data_type):
    """Look for amount+timing matches on other exchanges."""
    candidates = []
    asset = parsed.get("asset", "")
    amount = parsed.get("amount", "0")
    confirmed_at_str = parsed.get("confirmed_at")
    if not asset or not confirmed_at_str:
        return candidates

    try:
        ts = datetime.fromisoformat(confirmed_at_str)
    except (ValueError, TypeError):
        return candidates

    window = timedelta(hours=72)
    # For a deposit, look for withdrawals on other exchanges BEFORE this time
    # For a withdrawal, look for deposits on other exchanges AFTER this time
    if data_type == "deposits":
        other_table = "withdrawals"
        earliest = ts - window
        latest = ts
    else:
        other_table = "deposits"
        earliest = ts
        latest = ts + window

    r = await session.execute(text(f"""
        SELECT id, exchange, asset, amount::text, fee::text, tx_hash, address, confirmed_at
        FROM tax.{other_table}
        WHERE exchange != :ex AND asset = :asset
          AND confirmed_at BETWEEN :earliest AND :latest
          AND amount > 0
        ORDER BY confirmed_at
        LIMIT 50
    """), {"ex": exchange, "asset": asset, "earliest": earliest, "latest": latest})

    try:
        parsed_amount = D(amount)
    except (InvalidOperation, ValueError):
        return candidates

    for row in r.fetchall():
        existing = dict(zip(r.keys(), row))
        try:
            ex_amount = D(existing["amount"])
            ex_fee = D(existing.get("fee") or "0")
            ex_net = ex_amount - ex_fee

            # Check within 10% tolerance
            if ex_net > 0:
                diff_pct = abs(parsed_amount - ex_net) / ex_net
            elif ex_amount > 0:
                diff_pct = abs(parsed_amount - ex_amount) / ex_amount
            else:
                continue

            if diff_pct <= D("0.1"):
                evidence = []
                if diff_pct <= D("0.001"):
                    evidence.append("amount_exact_match")
                else:
                    evidence.append(f"amount_within_{float(diff_pct*100):.1f}%")

                hours_diff = abs((ts - existing["confirmed_at"]).total_seconds()) / 3600
                if hours_diff <= 24:
                    evidence.append("timing_within_24h")
                else:
                    evidence.append(f"timing_within_{hours_diff:.0f}h")

                direction = (f"This {exchange.upper()} {asset} may match "
                             f"a {existing['exchange'].upper()} {other_table.rstrip('s')}")
                candidates.append({
                    "confidence": "medium",
                    "evidence": evidence,
                    "direction": direction,
                    "other_record": {
                        "table": f"tax.{other_table}",
                        "id": existing["id"],
                        "exchange": existing["exchange"],
                        "asset": existing["asset"],
                        "amount": existing["amount"],
                        "tx_hash": existing.get("tx_hash"),
                        "address": existing.get("address"),
                        "confirmed_at": existing["confirmed_at"].isoformat() if existing["confirmed_at"] else None,
                    },
                })
        except (InvalidOperation, ValueError, TypeError):
            continue

    return candidates


# ── Commit logic ─────────────────────────────────────────────────────────

async def commit_staged(session, stage_id: str, decisions: list[dict]) -> dict:
    """Write approved rows to the database."""
    stage = get_staged(stage_id)
    if not stage:
        raise ValueError(f"Stage {stage_id} not found or expired")

    parsed_data = stage["parsed_data"]
    exchange = parsed_data["file_info"]["detected_exchange"]
    data_type = parsed_data["file_info"]["detected_type"]
    table = "deposits" if data_type == "deposits" else "withdrawals"
    filename = parsed_data["file_info"]["filename"]
    sha = parsed_data["file_info"]["sha256"]

    # Build decision map
    decision_map = {d["row_num"]: d for d in decisions}

    results = {"imported": 0, "skipped": 0, "updated": 0,
               "linked_transfers": 0, "errors": 0}

    for row in parsed_data["rows"]:
        dec = decision_map.get(row["row_num"], {})
        action = dec.get("action", "SKIP")

        if action == "SKIP":
            results["skipped"] += 1
            continue

        p = row["parsed"]
        try:
            if action in ("IMPORT", "LINK_TRANSFER"):
                if data_type == "deposits":
                    await session.execute(text("""
                        INSERT INTO tax.deposits
                            (exchange, exchange_id, asset, amount, amount_usd,
                             tx_hash, address, network, status, confirmed_at,
                             raw_data, source_type, source_file)
                        VALUES (:ex, :eid, :asset, :amount, :amount_usd,
                                :txh, :addr, :net, :status, :ts,
                                :raw, :src_type, :sf)
                        ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                            amount_usd = COALESCE(EXCLUDED.amount_usd, tax.deposits.amount_usd),
                            source_type = COALESCE(EXCLUDED.source_type, tax.deposits.source_type),
                            source_file = COALESCE(EXCLUDED.source_file, tax.deposits.source_file)
                    """), {
                        "ex": exchange, "eid": p["exchange_id"],
                        "asset": p["asset"], "amount": p["amount"],
                        "amount_usd": p.get("amount_usd"),
                        "txh": p.get("tx_hash", ""), "addr": p.get("address", ""),
                        "net": p.get("network", ""), "status": p.get("status", ""),
                        "ts": _parse_ts(p.get("confirmed_at")),
                        "raw": json.dumps(row.get("raw", {})),
                        "src_type": "xlsx" if ".xlsx" in filename.lower() else "csv",
                        "sf": filename,
                    })
                else:  # withdrawals
                    await session.execute(text("""
                        INSERT INTO tax.withdrawals
                            (exchange, exchange_id, asset, amount, amount_usd,
                             fee, fee_asset, tx_hash, address, network, status,
                             confirmed_at, raw_data, source_type, source_file)
                        VALUES (:ex, :eid, :asset, :amount, :amount_usd,
                                :fee, :fee_asset, :txh, :addr, :net, :status,
                                :ts, :raw, :src_type, :sf)
                        ON CONFLICT (exchange, exchange_id) DO UPDATE SET
                            amount_usd = COALESCE(EXCLUDED.amount_usd, tax.withdrawals.amount_usd),
                            fee = COALESCE(EXCLUDED.fee, tax.withdrawals.fee),
                            fee_asset = COALESCE(EXCLUDED.fee_asset, tax.withdrawals.fee_asset),
                            source_type = COALESCE(EXCLUDED.source_type, tax.withdrawals.source_type),
                            source_file = COALESCE(EXCLUDED.source_file, tax.withdrawals.source_file)
                    """), {
                        "ex": exchange, "eid": p["exchange_id"],
                        "asset": p["asset"], "amount": p["amount"],
                        "amount_usd": p.get("amount_usd"),
                        "fee": p.get("fee"), "fee_asset": p.get("fee_asset"),
                        "txh": p.get("tx_hash", ""), "addr": p.get("address", ""),
                        "net": p.get("network", ""), "status": p.get("status", ""),
                        "ts": _parse_ts(p.get("confirmed_at")),
                        "raw": json.dumps(row.get("raw", {})),
                        "src_type": "xlsx" if ".xlsx" in filename.lower() else "csv",
                        "sf": filename,
                    })

                results["imported"] += 1

                if action == "LINK_TRANSFER" and dec.get("link_transfer_id"):
                    results["linked_transfers"] += 1

            elif action == "IMPORT_UPDATE":
                # Update existing record with new data
                match = row.get("match", {})
                existing_id = match.get("existing_id")
                if existing_id:
                    updates = []
                    params = {"id": existing_id}
                    if p.get("amount_usd"):
                        updates.append("amount_usd = :amount_usd")
                        params["amount_usd"] = p["amount_usd"]
                    if p.get("status"):
                        updates.append("status = :status")
                        params["status"] = p["status"]
                    if updates:
                        await session.execute(text(f"""
                            UPDATE tax.{table} SET {', '.join(updates)} WHERE id = :id
                        """), params)
                    results["updated"] += 1
                else:
                    results["errors"] += 1

        except Exception as e:
            logger.warning(f"Commit error on row {row['row_num']}: {e}")
            results["errors"] += 1

    # Record import metadata
    await session.execute(text("""
        INSERT INTO tax.csv_imports
            (exchange, data_type, filename, file_hash, row_count,
             imported_count, duplicate_count, error_count)
        VALUES (:ex, :dt, :fn, :fh, :rc, :ic, :dc, :ec)
    """), {
        "ex": exchange, "dt": data_type, "fn": filename, "fh": sha,
        "rc": len(parsed_data["rows"]),
        "ic": results["imported"], "dc": results["skipped"], "ec": results["errors"],
    })

    # Mark stage as committed
    remove_staged(stage_id)

    return results
