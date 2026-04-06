# Accountant Handoff Checklist

Checklist for handing the CryptoTaxTracker report package to a CPA.

## Pre-Handoff Steps

1. **Run the full pipeline**: `POST /v4/compute-all?year=2026`
2. **Check filing readiness**: `GET /v4/filing-status?year=2026`
3. **Export the workbook**: `GET /export/v4-tax-report?year=2026`
4. **Review exceptions**: `GET /v4/exceptions?year=2026`

## Filing Readiness Gate

The system blocks filing when:
- Any **BLOCKING** exceptions are open
- Data coverage gaps exist without CSV imports
- Missing price valuations exist
- Unsupported transaction types detected

All blockers must be resolved before the export is considered filing-grade.

## XLSX Tabs to Review

| Tab | Accountant Action |
|-----|-------------------|
| **Summary** | Verify Schedule D totals, income summary, filing readiness flag |
| **Form 8949 (ST)** | Review short-term capital gains for Box B |
| **Form 8949 (LT)** | Review long-term capital gains for Box D |
| **Income Schedule** | Confirm staking/airdrop income at FMV |
| **Transfer Recon** | Verify self-transfers are correctly classified (non-taxable) |
| **Funding Flows** | Confirm external funding vs. internal transfers |
| **Exceptions** | Review and resolve any open issues |
| **Lot Inventory** | Verify remaining holdings and cost basis |
| **Exchange P&L Summary** | Per-exchange breakdown for reconciliation |
| **Data Coverage** | Confirm all date ranges are covered |
| **Valuation Audit** | Verify price sources per transaction |
| **Run Manifest** | Confirm computation metadata |

## CPA Decisions Required

1. **Basis Method Confirmation**: The system uses FIFO. Confirm this is the desired method.
2. **Wallet-Per-Exchange**: Each exchange is treated as a separate wallet for FIFO. Confirm this approach.
3. **Income Review**: All staking/airdrop events need `review_status: confirmed` before they create tax lots.
4. **Transfer Verification**: Review matched transfers — ensure no disposals were incorrectly classified as self-transfers.
5. **Fee Treatment**: Confirm whether crypto-denominated network fees should be treated as separate disposals (they currently are).
6. **Unclassified Flows**: All UNCLASSIFIED deposits/withdrawals must be manually classified before filing.

## Stop-Ship Conditions

Do NOT file if:
- Filing readiness flag is **NO**
- Any BLOCKING exceptions are unresolved
- Any UNCLASSIFIED flows remain
- Data coverage gaps exist without CSV imports
- Income events have `review_status: pending`

## How to Resolve Common Issues

| Issue | Resolution |
|-------|------------|
| MISSING_PRICE | Add manual price via `POST /v4/manual-price` or wait for CoinGecko data |
| DATA_COVERAGE_GAP | Import official MEXC CSV/XLSX exports via `POST /v4/import-file` |
| UNKNOWN_BASIS | Investigate why a disposal has no matching lot — may indicate missing trade data |
| OVERSOLD | More sold than purchased — usually indicates missing buy trades or imports |
| UNCLASSIFIED flow | Manually classify via wallet ownership claims or flow override |

## Interpreting the Exception Queue

- **BLOCKING**: Must fix before filing. Usually MISSING_PRICE, UNKNOWN_BASIS, or OVERSOLD.
- **WARNING**: Should review but won't prevent filing. Usually UNMATCHED_TRANSFER.
- **INFO**: Informational only. No action required.

## Required Manual Confirmations

Before signing off, the CPA should confirm:
- [ ] Basis method (FIFO) is acceptable
- [ ] All income events reviewed and confirmed
- [ ] All transfers verified as self-transfers
- [ ] Fee treatment is acceptable
- [ ] No unsupported transaction types (futures, margin, etc.)
- [ ] All data coverage gaps filled with official exports
