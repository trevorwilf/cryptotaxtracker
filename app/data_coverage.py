"""
Data Coverage Tracker — compute and store exchange data coverage.

Scans raw tables and CSV imports to determine coverage per exchange/data_type,
then populates tax.data_coverage for the export and filing-readiness checks.
"""
import logging
from datetime import datetime, timezone

from sqlalchemy import text

logger = logging.getLogger("tax-collector.data-coverage")


class DataCoverageTracker:
    """Compute and store exchange data coverage for each data type."""

    DATA_TYPES = {
        "trades": {"table": "tax.trades", "ts_col": "executed_at"},
        "deposits": {"table": "tax.deposits", "ts_col": "confirmed_at"},
        "withdrawals": {"table": "tax.withdrawals", "ts_col": "confirmed_at"},
    }

    async def compute_coverage(self, session, run_id: int, tax_year: int) -> list[dict]:
        """Scan raw tables and CSV imports to determine coverage per exchange/data_type."""
        year_start = datetime(tax_year, 1, 1, tzinfo=timezone.utc)
        year_end = datetime(tax_year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        results = []

        # Clear old coverage for this run
        if run_id:
            await session.execute(text("DELETE FROM tax.data_coverage WHERE run_id = :rid"),
                                  {"rid": run_id})

        for data_type, info in self.DATA_TYPES.items():
            table = info["table"]
            ts_col = info["ts_col"]

            # Get per-exchange coverage from API-sourced records
            r = await session.execute(text(f"""
                SELECT exchange,
                       MIN({ts_col}) AS earliest,
                       MAX({ts_col}) AS latest,
                       COUNT(*) AS cnt
                FROM {table}
                WHERE (source_type IS NULL OR source_type = 'api')
                  AND {ts_col} IS NOT NULL
                GROUP BY exchange
            """))
            api_rows = {row[0]: {"earliest": row[1], "latest": row[2], "cnt": row[3]}
                        for row in r.fetchall()}

            # Get per-exchange coverage from CSV/XLSX-sourced records
            r = await session.execute(text(f"""
                SELECT exchange,
                       MIN({ts_col}) AS earliest,
                       MAX({ts_col}) AS latest,
                       COUNT(*) AS cnt
                FROM {table}
                WHERE source_type IN ('csv', 'xlsx')
                  AND {ts_col} IS NOT NULL
                GROUP BY exchange
            """))
            csv_rows = {row[0]: {"earliest": row[1], "latest": row[2], "cnt": row[3]}
                        for row in r.fetchall()}

            # Get all exchanges that have data in this table
            all_exchanges = set(api_rows.keys()) | set(csv_rows.keys())

            for exchange in all_exchanges:
                api = api_rows.get(exchange, {})
                csv = csv_rows.get(exchange, {})

                api_earliest = api.get("earliest")
                api_latest = api.get("latest")
                csv_earliest = csv.get("earliest")
                csv_latest = csv.get("latest")

                # Determine overall earliest considering both sources
                overall_earliest = None
                if api_earliest and csv_earliest:
                    overall_earliest = min(api_earliest, csv_earliest)
                elif api_earliest:
                    overall_earliest = api_earliest
                elif csv_earliest:
                    overall_earliest = csv_earliest

                # Determine if there's a gap
                has_gap = overall_earliest is None or overall_earliest > year_start
                requires_csv = has_gap and (not csv_earliest or csv_earliest > year_start)
                csv_imported = csv_earliest is not None

                gap_desc = None
                if has_gap:
                    if overall_earliest:
                        gap_desc = f"No data before {overall_earliest.strftime('%Y-%m-%d')}; tax year starts {tax_year}-01-01"
                    else:
                        gap_desc = f"No {data_type} data found for {exchange}"

                # Upsert into data_coverage
                await session.execute(text("""
                    INSERT INTO tax.data_coverage
                        (exchange, data_type, api_earliest, api_latest,
                         csv_earliest, csv_latest, has_gap, gap_description,
                         requires_csv, csv_imported, run_id)
                    VALUES (:ex, :dt, :ae, :al, :ce, :cl, :hg, :gd, :rc, :ci, :rid)
                    ON CONFLICT ON CONSTRAINT data_coverage_pkey DO NOTHING
                """), {
                    "ex": exchange, "dt": data_type,
                    "ae": api_earliest, "al": api_latest,
                    "ce": csv_earliest, "cl": csv_latest,
                    "hg": has_gap, "gd": gap_desc,
                    "rc": requires_csv, "ci": csv_imported,
                    "rid": run_id,
                })

                entry = {
                    "exchange": exchange, "data_type": data_type,
                    "api_earliest": str(api_earliest) if api_earliest else None,
                    "api_latest": str(api_latest) if api_latest else None,
                    "csv_earliest": str(csv_earliest) if csv_earliest else None,
                    "csv_latest": str(csv_latest) if csv_latest else None,
                    "has_gap": has_gap, "requires_csv": requires_csv,
                    "csv_imported": csv_imported,
                }
                results.append(entry)

        logger.info(f"Data coverage computed: {len(results)} entries for tax year {tax_year}")
        return results
