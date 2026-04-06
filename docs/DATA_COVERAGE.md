# Data Coverage — Exchange API Retention Limits

## MEXC API Retention

| Endpoint | Retention | Description |
|----------|-----------|-------------|
| `GET /api/v3/myTrades` | **30 days** | Trade history |
| `GET /api/v3/allOrders` | **7 days** (default 24h) | Order history |
| `GET /api/v3/capital/deposit/hisrec` | **90 days** | Deposit history |
| `GET /api/v3/capital/withdraw/history` | **90 days** | Withdrawal history |
| `GET /api/v3/capital/transfer` | **180 days** | Universal transfer history |

### When CSV Import Is Required

If the tax year starts before the API retention window, you **must** import the
official MEXC CSV export to cover the gap. For example:

- Tax year 2024 starts 2024-01-01
- MEXC trade API only covers the last 30 days
- Gap: 2024-01-01 to ~30 days ago (most of the year)
- **Action:** Export trade history CSV from MEXC and import via `/v4/import-csv`

The system will create a **BLOCKING** exception if a coverage gap exists without
a corresponding CSV import.

### How to Export CSVs from MEXC

1. Log in to [mexc.com](https://www.mexc.com)
2. Go to **Orders > Spot Orders > Trade History**
3. Select date range and click **Export**
4. Save the CSV file
5. Import via the `/v4/import-csv` endpoint or dashboard

## NonKYC API Retention

NonKYC.io does not publicly document retention limits. In practice:

- Trade history: typically available for the full account lifetime
- Deposit/withdrawal history: typically available for the full account lifetime

If NonKYC retention becomes an issue, the same CSV import pipeline can be used.

## Checking Coverage

Use `GET /v4/data-coverage?year=2025` to see:
- What date ranges each exchange's API can cover
- Whether gaps exist
- Whether CSV imports have filled those gaps
- The "Data Coverage" tab in the XLSX export shows this information

## How Coverage Is Tracked

The `tax.data_coverage` table is populated by `DataCoverageTracker` during each
`/v4/compute-all` run. It scans the raw tables (`tax.trades`, `tax.deposits`,
`tax.withdrawals`) and computes:

1. **API-sourced ranges**: MIN/MAX timestamps where `source_type IS NULL OR source_type = 'api'`
2. **CSV-sourced ranges**: MIN/MAX timestamps where `source_type IN ('csv', 'xlsx')`
3. **Gap detection**: If the earliest record is after the tax year start (Jan 1)
4. **CSV coverage check**: Whether CSV/XLSX imports fill the gap

Each record gets `source_type` set to `'api'`, `'csv'`, or `'xlsx'` based on how it
was ingested. This enables the system to distinguish API-fetched data from imported data.

## Import Pipeline

The format-aware import subsystem (`/v4/import-file`) supports:
- MEXC deposit/withdrawal `.xlsx` exports (official format)
- NonKYC deposit/withdrawal `.csv` exports (official format)
- MEXC trade `.csv` (API format)

See `docs/EXCHANGE_EXPORT_FORMATS.md` for exact header fingerprints.
