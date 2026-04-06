"""
Database Diagnostic Export — produces everything needed for remote debugging.

Output files:
  _schema.sql          — pg_dump --schema-only of the tax schema (with fallback)
  _manifest.txt        — table inventory with row counts and file sizes
  _data_quality.txt    — NULL counts, distinct values, date ranges per table
  _integrity.txt       — foreign key orphan checks
  _sequences.txt       — current sequence values vs max IDs
  _storage.txt         — table sizes, index sizes, dead tuples
  {table_name}.csv     — full table data per table
"""
import csv
import logging
import os
import subprocess
from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import text

logger = logging.getLogger("tax-collector.db-export")

FK_CHECKS = [
    ("normalized_events", "source_trade_id", "trades", "id", "Events → Trades"),
    ("normalized_events", "source_deposit_id", "deposits", "id", "Events → Deposits"),
    ("normalized_events", "source_withdrawal_id", "withdrawals", "id", "Events → Withdrawals"),
    ("normalized_events", "source_pool_id", "pool_activity", "id", "Events → Pool Activity"),
    ("normalized_events", "paired_event_id", "normalized_events", "id", "Events → Paired Event"),
    ("normalized_events", "valuation_id", "valuation_log", "id", "Events → Valuation"),
    ("lots_v4", "source_event_id", "normalized_events", "id", "Lots → Source Event"),
    ("lots_v4", "parent_lot_id", "lots_v4", "id", "Lots → Parent Lot"),
    ("lots_v4", "transfer_carryover_id", "transfer_carryover", "id", "Lots → Transfer Carryover"),
    ("disposals_v4", "disposal_event_id", "normalized_events", "id", "Disposals → Event"),
    ("disposals_v4", "lot_id", "lots_v4", "id", "Disposals → Lot"),
    ("disposals_v4", "source_trade_id", "trades", "id", "Disposals → Trade"),
    ("transfer_carryover", "source_lot_id", "lots_v4", "id", "Carryover → Source Lot"),
    ("transfer_carryover", "dest_lot_id", "lots_v4", "id", "Carryover → Dest Lot"),
    ("transfer_carryover", "withdrawal_id", "withdrawals", "id", "Carryover → Withdrawal"),
    ("transfer_carryover", "deposit_id", "deposits", "id", "Carryover → Deposit"),
    ("income_events_v4", "source_event_id", "normalized_events", "id", "Income → Event"),
    ("income_events_v4", "source_deposit_id", "deposits", "id", "Income → Deposit"),
    ("income_events_v4", "lot_id", "lots_v4", "id", "Income → Lot"),
    ("income_events_v4", "valuation_id", "valuation_log", "id", "Income → Valuation"),
    ("form_8949_v4", "disposal_id", "disposals_v4", "id", "8949 → Disposal"),
    ("exceptions", "source_trade_id", "trades", "id", "Exceptions → Trade"),
    ("exceptions", "source_deposit_id", "deposits", "id", "Exceptions → Deposit"),
    ("exceptions", "source_withdrawal_id", "withdrawals", "id", "Exceptions → Withdrawal"),
    ("exceptions", "source_event_id", "normalized_events", "id", "Exceptions → Event"),
    ("exceptions", "lot_id", "lots_v4", "id", "Exceptions → Lot"),
]


class DatabaseExporter:
    """Produces a complete diagnostic package for remote debugging."""

    def __init__(self, export_dir: str, database_url: str):
        self.export_dir = export_dir
        self.database_url = database_url
        self.export_time = datetime.now(timezone.utc).isoformat()
        self.table_names: list[str] = []
        self.table_info: dict[str, dict] = {}  # table -> {row_count, columns, file_size}
        self.total_rows = 0
        self.errors: list[dict] = []

    async def export_all(self, session) -> dict:
        """Run the full diagnostic export."""
        # Create/clean directory
        os.makedirs(self.export_dir, exist_ok=True)
        for f in os.listdir(self.export_dir):
            fp = os.path.join(self.export_dir, f)
            if os.path.isfile(fp):
                os.remove(fp)

        # Discover tables
        r = await session.execute(text("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'tax' ORDER BY table_name
        """))
        self.table_names = [row[0] for row in r.fetchall()]

        logger.info(f"[export] Starting diagnostic export: {len(self.table_names)} tables")

        # Run each export step
        await self._export_schema(session)
        await self._export_table_data(session)
        await self._export_data_quality(session)
        await self._export_integrity(session)
        await self._export_sequences(session)
        await self._export_storage(session)
        self._write_manifest()

        logger.info(f"[export] Complete: {len(self.table_names)} tables, {self.total_rows} rows")

        return {
            "export_dir": self.export_dir,
            "export_time": self.export_time,
            "tables_exported": len(self.table_info),
            "total_rows": self.total_rows,
            "errors": self.errors,
            "files": sorted(os.listdir(self.export_dir)),
        }

    # ── Schema DDL ─────────────────────────────────────────────────────

    async def _export_schema(self, session):
        """Try pg_dump first, fall back to information_schema reconstruction."""
        schema_path = os.path.join(self.export_dir, "_schema.sql")
        try:
            self._export_schema_pgdump(schema_path)
            logger.info("[export] Schema exported via pg_dump")
        except Exception as e:
            logger.info(f"[export] pg_dump unavailable ({e}), using fallback")
            await self._export_schema_fallback(session, schema_path)

    def _export_schema_pgdump(self, schema_path: str):
        """Run pg_dump --schema-only for the tax schema."""
        # Convert asyncpg URL to plain postgresql URL
        url = self.database_url.replace("+asyncpg", "")
        parsed = urlparse(url)
        env = os.environ.copy()
        env["PGPASSWORD"] = parsed.password or ""

        host = parsed.hostname or "localhost"
        port = str(parsed.port or 5432)
        user = parsed.username or "postgres"
        dbname = parsed.path.lstrip("/")

        result = subprocess.run(
            ["pg_dump", "--schema-only", "--schema=tax", "--no-owner", "--no-privileges",
             "-h", host, "-p", port, "-U", user, dbname],
            capture_output=True, text=True, timeout=30, env=env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"pg_dump failed: {result.stderr}")

        with open(schema_path, "w", encoding="utf-8") as f:
            f.write(f"-- Exported via pg_dump at {self.export_time}\n")
            f.write(result.stdout)

    async def _export_schema_fallback(self, session, schema_path: str):
        """Reconstruct DDL from information_schema when pg_dump unavailable."""
        lines = [f"-- Schema reconstructed from information_schema at {self.export_time}",
                 "-- (pg_dump not available — this is an approximation)", ""]

        for table_name in self.table_names:
            # Columns
            r = await session.execute(text("""
                SELECT column_name, data_type, character_maximum_length,
                       numeric_precision, numeric_scale,
                       is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'tax' AND table_name = :tbl
                ORDER BY ordinal_position
            """), {"tbl": table_name})
            cols = [dict(zip(r.keys(), row)) for row in r.fetchall()]

            lines.append(f"CREATE TABLE IF NOT EXISTS tax.{table_name} (")
            col_defs = []
            for c in cols:
                dtype = c["data_type"]
                if dtype == "character varying" and c["character_maximum_length"]:
                    dtype = f"VARCHAR({c['character_maximum_length']})"
                elif dtype == "numeric" and c["numeric_precision"]:
                    dtype = f"NUMERIC({c['numeric_precision']},{c['numeric_scale'] or 0})"
                nullable = "" if c["is_nullable"] == "YES" else " NOT NULL"
                default = f" DEFAULT {c['column_default']}" if c["column_default"] else ""
                col_defs.append(f"    {c['column_name']:30s} {dtype}{nullable}{default}")
            lines.append(",\n".join(col_defs))
            lines.append(");")
            lines.append("")

            # Constraints
            try:
                r = await session.execute(text("""
                    SELECT tc.constraint_name, tc.constraint_type,
                           kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = 'tax' AND tc.table_name = :tbl
                    ORDER BY tc.constraint_type, tc.constraint_name
                """), {"tbl": table_name})
                constraints = [dict(zip(r.keys(), row)) for row in r.fetchall()]
                for c in constraints:
                    lines.append(f"-- {c['constraint_type']}: {c['constraint_name']} ({c['column_name']})")
            except Exception:
                pass

            # Indexes
            try:
                r = await session.execute(text("""
                    SELECT indexname, indexdef FROM pg_indexes
                    WHERE schemaname = 'tax' AND tablename = :tbl
                """), {"tbl": table_name})
                for row in r.fetchall():
                    lines.append(f"{row[1]};")
            except Exception:
                pass

            lines.append("")

        with open(schema_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ── Table Data CSVs ────────────────────────────────────────────────

    async def _export_table_data(self, session):
        """Export each table to CSV."""
        for table_name in self.table_names:
            try:
                # Get columns
                r = await session.execute(text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'tax' AND table_name = :tbl
                    ORDER BY ordinal_position
                """), {"tbl": table_name})
                col_names = [row[0] for row in r.fetchall()]

                # Row count
                cr = await session.execute(text(f"SELECT COUNT(*) FROM tax.{table_name}"))
                row_count = cr.scalar() or 0

                # Cast columns
                cast_cols = ", ".join(
                    f"{c}::text" if c != "raw_data" else f"LEFT({c}::text, 500) AS {c}"
                    for c in col_names
                )
                order = " ORDER BY id" if "id" in col_names else ""
                dr = await session.execute(text(f"SELECT {cast_cols} FROM tax.{table_name}{order}"))
                rows = dr.fetchall()

                # Write CSV
                csv_path = os.path.join(self.export_dir, f"{table_name}.csv")
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(col_names)
                    for row in rows:
                        writer.writerow(list(row))

                file_size = os.path.getsize(csv_path)
                self.table_info[table_name] = {
                    "row_count": row_count, "columns": col_names, "file_size": file_size}
                self.total_rows += row_count
                logger.info(f"[export] tax.{table_name}: {row_count} rows, {file_size/1024:.1f} KB")

            except Exception as e:
                self.errors.append({"table": table_name, "error": str(e)})
                logger.error(f"[export] Failed: tax.{table_name}: {e}")

    # ── Data Quality ───────────────────────────────────────────────────

    async def _export_data_quality(self, session):
        """Analyze NULL counts, distinct values, date ranges per column."""
        lines = [f"CryptoTaxTracker — Data Quality Report",
                 f"Generated: {self.export_time}", ""]

        for table_name in self.table_names:
            info = self.table_info.get(table_name)
            if not info:
                continue
            col_names = info["columns"]
            row_count = info["row_count"]

            lines.append(f"{'='*70}")
            lines.append(f"TABLE: tax.{table_name}  ({row_count} rows)")
            lines.append(f"{'='*70}")

            if row_count == 0:
                lines.append("  (empty table)")
                lines.append("")
                continue

            # Per-exchange breakdown if exchange column exists
            if "exchange" in col_names:
                try:
                    r = await session.execute(text(
                        f"SELECT exchange, COUNT(*) FROM tax.{table_name} GROUP BY exchange ORDER BY exchange"))
                    lines.append("  Per-exchange breakdown:")
                    for row in r.fetchall():
                        lines.append(f"    {row[0] or 'NULL':20s} {row[1]:>8d} rows")
                    lines.append("")
                except Exception:
                    pass

            for col in col_names:
                if col == "raw_data":
                    continue
                try:
                    # NULL count
                    r = await session.execute(text(
                        f"SELECT COUNT(*) FILTER (WHERE {col} IS NULL), COUNT(*) FROM tax.{table_name}"))
                    null_row = r.fetchone()
                    null_count = null_row[0] or 0
                    total = null_row[1] or 0
                    null_pct = (null_count / total * 100) if total > 0 else 0

                    # Distinct count
                    r = await session.execute(text(
                        f"SELECT COUNT(DISTINCT {col}::text) FROM tax.{table_name}"))
                    distinct = r.scalar() or 0

                    line = f"  {col:30s}  nulls={null_count:>5d} ({null_pct:5.1f}%)  distinct={distinct:>6d}"
                    lines.append(line)

                    # Sample values
                    r = await session.execute(text(
                        f"SELECT DISTINCT LEFT({col}::text, 40) FROM tax.{table_name} "
                        f"WHERE {col} IS NOT NULL LIMIT 5"))
                    samples = [row[0] for row in r.fetchall()]
                    if samples:
                        lines.append(f"    samples: {', '.join(repr(s) for s in samples)}")

                    # Date ranges for timestamp columns
                    r_type = await session.execute(text("""
                        SELECT data_type FROM information_schema.columns
                        WHERE table_schema = 'tax' AND table_name = :tbl AND column_name = :col
                    """), {"tbl": table_name, "col": col})
                    dtype_row = r_type.fetchone()
                    if dtype_row and "timestamp" in (dtype_row[0] or ""):
                        r = await session.execute(text(
                            f"SELECT MIN({col})::text, MAX({col})::text FROM tax.{table_name}"))
                        dr = r.fetchone()
                        if dr and dr[0]:
                            lines.append(f"    date range: {dr[0]} to {dr[1]}")

                except Exception as e:
                    lines.append(f"  {col:30s}  ERROR: {e}")

            lines.append("")

        path = os.path.join(self.export_dir, "_data_quality.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ── Referential Integrity ──────────────────────────────────────────

    async def _export_integrity(self, session):
        """Check for orphaned foreign key references."""
        lines = [f"CryptoTaxTracker — Referential Integrity Check",
                 f"Generated: {self.export_time}", ""]

        existing_tables = set(self.table_names)
        ok_count = 0
        warn_count = 0

        for child_tbl, child_col, parent_tbl, parent_col, desc in FK_CHECKS:
            if child_tbl not in existing_tables or parent_tbl not in existing_tables:
                lines.append(f"  ⏭ {desc:40s} SKIPPED (table not found)")
                continue

            # Check if child column exists
            try:
                r = await session.execute(text("""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'tax' AND table_name = :tbl AND column_name = :col
                """), {"tbl": child_tbl, "col": child_col})
                if not r.fetchone():
                    lines.append(f"  ⏭ {desc:40s} SKIPPED (column {child_col} not found)")
                    continue
            except Exception:
                lines.append(f"  ⏭ {desc:40s} SKIPPED (error checking column)")
                continue

            try:
                r = await session.execute(text(f"""
                    SELECT COUNT(*) FROM tax.{child_tbl} c
                    WHERE c.{child_col} IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM tax.{parent_tbl} p WHERE p.{parent_col} = c.{child_col}
                      )
                """))
                orphan_count = r.scalar() or 0

                if orphan_count == 0:
                    lines.append(f"  ✓ {desc:40s} OK")
                    ok_count += 1
                else:
                    # Get sample orphaned values
                    r = await session.execute(text(f"""
                        SELECT DISTINCT c.{child_col}::text FROM tax.{child_tbl} c
                        WHERE c.{child_col} IS NOT NULL
                          AND NOT EXISTS (
                              SELECT 1 FROM tax.{parent_tbl} p WHERE p.{parent_col} = c.{child_col}
                          )
                        LIMIT 5
                    """))
                    samples = [row[0] for row in r.fetchall()]
                    lines.append(f"  ⚠️ {desc:40s} {orphan_count} orphans (samples: {samples})")
                    warn_count += 1

            except Exception as e:
                lines.append(f"  ❌ {desc:40s} ERROR: {e}")

        lines.append("")
        lines.append(f"Summary: {ok_count} OK, {warn_count} warnings, "
                     f"{len(FK_CHECKS) - ok_count - warn_count} skipped/errors")

        path = os.path.join(self.export_dir, "_integrity.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ── Sequences ──────────────────────────────────────────────────────

    async def _export_sequences(self, session):
        """Check sequence values vs actual max IDs."""
        lines = [f"CryptoTaxTracker — Sequence States",
                 f"Generated: {self.export_time}", "",
                 f"{'Table':30s} {'Sequence Last':>15s} {'Max ID':>10s} {'Status':>10s}",
                 "-" * 70]

        for table_name in self.table_names:
            info = self.table_info.get(table_name)
            if not info or "id" not in info["columns"]:
                continue

            try:
                # Get the default for the id column to find the sequence name
                r = await session.execute(text("""
                    SELECT column_default FROM information_schema.columns
                    WHERE table_schema = 'tax' AND table_name = :tbl AND column_name = 'id'
                """), {"tbl": table_name})
                default_row = r.fetchone()
                if not default_row or not default_row[0] or "nextval" not in str(default_row[0]):
                    continue

                # Extract sequence name
                default_str = str(default_row[0])
                seq_name = default_str.split("'")[1] if "'" in default_str else None
                if not seq_name:
                    continue

                # Get sequence last_value
                try:
                    r = await session.execute(text(f"SELECT last_value FROM {seq_name}"))
                    seq_val = r.scalar()
                except Exception:
                    seq_val = "?"

                # Get max ID
                r = await session.execute(text(f"SELECT MAX(id) FROM tax.{table_name}"))
                max_id = r.scalar() or 0

                status = "OK" if seq_val == "?" or int(seq_val) >= int(max_id) else "BEHIND"
                lines.append(f"  tax.{table_name:26s} {str(seq_val):>15s} {str(max_id):>10s} {status:>10s}")

            except Exception as e:
                lines.append(f"  tax.{table_name:26s} ERROR: {e}")

        path = os.path.join(self.export_dir, "_sequences.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    # ── Storage Statistics ─────────────────────────────────────────────

    async def _export_storage(self, session):
        """Query table and index sizes from pg_stat."""
        lines = [f"CryptoTaxTracker — Storage Statistics",
                 f"Generated: {self.export_time}", "",
                 f"{'Table':30s} {'Rows':>10s} {'Dead':>8s} {'Table Size':>12s} {'Index Size':>12s} {'Total':>12s}",
                 "-" * 90]

        total_size = 0
        for table_name in self.table_names:
            try:
                r = await session.execute(text(f"""
                    SELECT
                        s.n_live_tup,
                        s.n_dead_tup,
                        pg_table_size('tax.{table_name}') AS table_bytes,
                        pg_indexes_size('tax.{table_name}') AS index_bytes,
                        pg_total_relation_size('tax.{table_name}') AS total_bytes
                    FROM pg_stat_user_tables s
                    WHERE s.schemaname = 'tax' AND s.relname = :tbl
                """), {"tbl": table_name})
                row = r.fetchone()
                if row:
                    live, dead, tbl_b, idx_b, tot_b = row
                    total_size += tot_b or 0
                    lines.append(
                        f"  tax.{table_name:26s} {live or 0:>10d} {dead or 0:>8d} "
                        f"{self._fmt_size(tbl_b):>12s} {self._fmt_size(idx_b):>12s} "
                        f"{self._fmt_size(tot_b):>12s}")
                else:
                    lines.append(f"  tax.{table_name:26s} (no stats)")
            except Exception as e:
                lines.append(f"  tax.{table_name:26s} ERROR: {e}")

        lines.append("-" * 90)
        lines.append(f"  {'TOTAL':30s} {'':>10s} {'':>8s} {'':>12s} {'':>12s} {self._fmt_size(total_size):>12s}")

        path = os.path.join(self.export_dir, "_storage.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

    @staticmethod
    def _fmt_size(b):
        if b is None:
            return "—"
        if b < 1024:
            return f"{b} B"
        if b < 1024 * 1024:
            return f"{b/1024:.1f} KB"
        return f"{b/1024/1024:.1f} MB"

    # ── Manifest ───────────────────────────────────────────────────────

    def _write_manifest(self):
        """Summary of the entire export package."""
        lines = [f"CryptoTaxTracker — Database Diagnostic Export",
                 f"Exported at: {self.export_time}", "",
                 f"Tables found: {len(self.table_names)}",
                 "=" * 60, ""]

        # Diagnostic files
        lines.append("DIAGNOSTIC FILES:")
        diag_files = ["_schema.sql", "_data_quality.txt", "_integrity.txt",
                      "_sequences.txt", "_storage.txt"]
        for df in diag_files:
            fp = os.path.join(self.export_dir, df)
            if os.path.exists(fp):
                sz = os.path.getsize(fp)
                lines.append(f"  {df:30s} {sz/1024:.1f} KB")
            else:
                lines.append(f"  {df:30s} (not created)")
        lines.append("")

        # Table CSVs
        lines.append("TABLE DATA:")
        for table_name in sorted(self.table_info.keys()):
            info = self.table_info[table_name]
            lines.append(f"  tax.{table_name:26s} {info['row_count']:>8d} rows  "
                         f"{info['file_size']/1024:>8.1f} KB  "
                         f"({len(info['columns'])} columns)")
        lines.append("")

        if self.errors:
            lines.append("ERRORS:")
            for e in self.errors:
                lines.append(f"  {e['table']}: {e['error']}")
            lines.append("")

        lines.append("=" * 60)
        lines.append(f"TOTALS: {len(self.table_info)} tables, {self.total_rows} rows")

        path = os.path.join(self.export_dir, "_manifest.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
