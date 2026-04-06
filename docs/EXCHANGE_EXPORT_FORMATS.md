# Exchange Export Formats

Supported import formats with exact header fingerprints and parsing rules.

## MEXC Deposits (XLSX)

**File type**: `.xlsx`
**Header fingerprint**:
```
UID | Status | Time | Crypto | Network | Deposit Amount | TxID | Progress
```

**Column mapping**:
| Export Column | DB Column | Notes |
|---|---|---|
| TxID | `exchange_id`, `tx_hash` | Full value for exchange_id; strip `:N` suffix for tx_hash matching |
| Crypto | `asset` | e.g. "BTC", "USDT" |
| Deposit Amount | `amount` | |
| Network | `network` | e.g. "Bitcoin(BTC)", "Solana(SOL)" |
| Status | `status` | e.g. "Credited Successfully" |
| Time | `confirmed_at` | Format: `YYYY-MM-DD HH:MM:SS` (UTC) |

**Fallback exchange_id**: If TxID is empty, generates `mexc_dep_{UID}_{row}`.

## MEXC Withdrawals (XLSX)

**File type**: `.xlsx`
**Header fingerprint**:
```
UID | Status | Time | Crypto | Network | Request Amount | Withdrawal Address | memo | TxID | Trading Fee | Settlement Amount | Withdrawal Descriptions
```

**Column mapping**:
| Export Column | DB Column | Notes |
|---|---|---|
| TxID | `exchange_id`, `tx_hash` | |
| Crypto | `asset`, `fee_asset` | Fee is always in same asset |
| Request Amount | `amount` | Gross amount before fee |
| Trading Fee | `fee` | |
| Settlement Amount | (validated) | Should equal Request Amount - Trading Fee |
| Withdrawal Address | `address` | |
| Time | `confirmed_at` | Format: `YYYY-MM-DD HH:MM:SS` (UTC) |

**Validation**: Warning logged if `Settlement Amount != Request Amount - Trading Fee`.

## NonKYC Deposits (CSV)

**File type**: `.csv`
**Header fingerprint**:
```
Type,Time,Ticker,Amount,ValueUsd,Confirmations,TransactionId,Address,isPosted,isReversed,isSecurityConfirm
```

**Column mapping**:
| Export Column | DB Column | Notes |
|---|---|---|
| TransactionId | `exchange_id`, `tx_hash` | |
| Ticker | `asset` | |
| Amount | `amount` | |
| ValueUsd | `amount_usd` | NonKYC provides FMV directly |
| Address | `address` | |
| Time | `confirmed_at` | Format: `M/D/YYYY, h:mm:ss AM/PM` |
| isPosted | `status` | `"true"` → "posted", else "pending" |

**Time parsing**: `datetime.strptime(val, "%m/%d/%Y, %I:%M:%S %p")` with UTC timezone.

## NonKYC Withdrawals (CSV)

**File type**: `.csv`
**Header fingerprint**:
```
Type,Time,Ticker,Amount,ValueUsd,Address,TransactionId,Status
```

**Column mapping**:
| Export Column | DB Column | Notes |
|---|---|---|
| TransactionId | `exchange_id`, `tx_hash` | |
| Ticker | `asset` | |
| Amount | `amount` | Net amount (fee already deducted) |
| ValueUsd | `amount_usd` | |
| Address | `address` | |
| Time | `confirmed_at` | Same format as deposits |
| Status | `status` | e.g. "Confirmed" |

**Note**: No fee column in CSV export. Fee is deducted before export. Record `fee = NULL` from CSV.

## MEXC Trades (CSV — API format)

**File type**: `.csv`
**Header fingerprint**:
```
symbol,orderId,id,price,qty,quoteQty,commission,commissionAsset,time,isBuyer
```

Standard MEXC API format. Used for trade history beyond the 30-day API retention window.

## Auto-Detection

The system auto-detects format using `detect_format(filepath)`:
1. Check file extension (`.xlsx` vs `.csv`)
2. Read first row (headers)
3. Match against known fingerprints
4. Return `(exchange, data_type, parser_name)` or raise `ValueError`

Use `POST /v4/import-file?filepath=...` for auto-detected import.
Use `POST /v4/import-file?filepath=...&exchange=mexc&data_type=deposits` to override detection.
