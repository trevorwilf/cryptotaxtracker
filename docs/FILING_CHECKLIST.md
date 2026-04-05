# Filing Checklist

Pre-filing checklist tied to the v4 exception system. All items must be resolved
before the system marks a tax year as **Filing Ready**.

## Blocking Conditions (Must Fix)

- [ ] **Zero blocking exceptions** — `GET /v4/exceptions?severity=BLOCKING&status=open`
- [ ] **All data coverage gaps filled** — MEXC CSV imports for date ranges beyond API retention
- [ ] **Zero missing-price exceptions** — All assets have historical FMV
- [ ] **Zero unsupported transaction types** — No futures, options, margin, etc.
- [ ] **All deposits resolved** — No unclassified deposits remain

## Accountant Decisions Required

These items require human judgment before filing:

1. **FIFO Confirmation** — The system uses FIFO (First In, First Out). Confirm this is the desired method.
2. **Wallet Definition** — Verify that each exchange account is treated as a separate wallet for FIFO purposes.
3. **Income Review** — All staking/airdrop income events have `review_status: confirmed` (check via `GET /v4/income`)
4. **Transfer Verification** — Review matched transfers to ensure no disposals were incorrectly classified as transfers
5. **Fee Treatment** — Confirm whether crypto-denominated fees should be treated as separate disposals

## XLSX Export Sheets

| Sheet | Purpose |
|-------|---------|
| **Summary** | Schedule D totals (ST/LT), income, fees, filing readiness |
| **Form 8949 (ST)** | Short-term capital gains/losses — Box B |
| **Form 8949 (LT)** | Long-term capital gains/losses — Box D |
| **Income Schedule** | Staking rewards, airdrops (ordinary income) |
| **Transfer Recon** | Matched cross-exchange transfers (non-taxable) |
| **Lot Inventory** | Current holdings with cost basis |
| **Exchange P&L Summary** | Realized gains/losses by exchange |
| **Funding Flows** | Classified deposit/withdrawal flows by exchange |
| **Exceptions** | Open issues that need review |
| **Data Coverage** | API retention gaps and CSV import status |
| **Valuation Audit** | Per-lookup price source audit trail |
| **Run Manifest** | Computation run metadata for reproducibility |

## Filing Readiness Flag

The system displays a prominent **FILING READINESS** indicator:

```
Filing Ready:   YES / NO
Blocking Issues: N
Warnings:       N
Coverage Gaps:  N
Missing Prices: N
Reason(s):      [list any blockers]
```

A run is filing-ready **ONLY** when:
- Zero blocking exceptions
- Zero unresolved deposits
- All requested tax-year date ranges covered (no retention gaps without CSV)
- Zero missing-price exceptions
- Zero unsupported transaction types detected
