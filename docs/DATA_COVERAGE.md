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
