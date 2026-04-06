# Fix Verification Report
Generated: 2026-04-06

## Test results
- Total: 661  Passed: 661  Failed: 0

## Fixes confirmed
- [x] Group 1: Activity start endpoint added; run_id param on PnL endpoint (soft)
- [x] Group 2: NonKYC trade parser uses totalWithFee + alternateFeeAsset; base/quote split; upsert_trades updates fee_asset/base_asset/quote_asset/market on conflict
- [x] Group 3: NonKYC CSV identity uses csv- prefix; external_tx_id in parsed output
- [x] Group 4: Transfer matcher selects w.address and d.address; tx-hash checked before amount (overrides amount mismatch)
- [x] Group 5: Sync lock acquired via _sync_lock in run_sync; _run_sync_inner separated
- [x] Group 6: EXTERNAL_DEPOSIT/WITHDRAWAL emitted by flow classifier for unmatched items
- [x] Group 7: activity_start table + endpoint for phantom gap suppression
- [x] Group 8: Export button uses /export/v4-tax-report; year defaults dynamic via CURRENT_TAX_YEAR
- [x] Group 9: INVENTORY_SHORTFALL replaces OVERSOLD (backward-compat alias); descriptive message

## Post-deploy operator steps
1. Restart the tax-collector container to deploy changes
2. POST /v4/compute-all?year=2026  (clears 136 OVERSOLD exceptions)
3. POST /v4/activity-start  exchange=nonkyc  start_date=2026-02-04
   POST /v4/activity-start  exchange=mexc    start_date=2026-02-17
   POST /v4/activity-start  exchange=salvium start_date=2026-02-07
   (clears DATA_COVERAGE_GAP phantom exceptions)
4. The 1 UNSUPPORTED_TX_TYPE (SAL/USDT pool buy) needs manual tax decision
5. Verify P&L: mexc=+$8.81, nonkyc=-$8.33, net=+$0.48 (±USD valuation drift)
6. Verify zero disposals with holding_days < 0
