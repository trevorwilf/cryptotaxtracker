# Money Flow Definitions

## Why This Matters

The dashboard previously labeled raw deposit/withdrawal USD sums as "Deposits (USD)"
and "Withdrawals (USD)". These numbers are misleading because they include:
- Internal transfers between your own exchanges (not real funding)
- Income receipts (staking rewards, airdrops)
- FMV-valued crypto movements (not actual USD transfers)

The classified flow system separates these into categories the accountant can use directly.

## "Raw Deposit Ledger FMV" vs "External Deposit"

| Term | Meaning |
|------|---------|
| **Raw Deposit Ledger FMV** | Sum of all deposits valued at FMV — includes transfers, income, everything |
| **External Deposit** | Only deposits from outside the tracked exchange ecosystem (actual new money in) |

The accountant cares about **External Deposit** for funding analysis and capital flow reporting.

## Flow Classification Rules

Every deposit and withdrawal is classified into one of these categories:

### EXTERNAL_DEPOSIT
- **Definition:** Fiat or crypto received from outside the tracked ecosystem
- **Examples:** Bank wire to MEXC, crypto sent from a non-tracked wallet
- **Tax impact:** Not directly taxable (establishes cost basis if crypto)
- **Rule:** Deposit not matched as transfer and not classified as income

### EXTERNAL_WITHDRAWAL
- **Definition:** Fiat or crypto sent outside the tracked ecosystem
- **Examples:** Withdrawal to personal bank, crypto sent to cold storage
- **Tax impact:** Not directly taxable (disposal may have already occurred)
- **Rule:** Withdrawal not matched as a transfer to another tracked exchange

### INTERNAL_TRANSFER_IN
- **Definition:** Crypto received from another tracked exchange/wallet
- **Examples:** BTC transferred from MEXC to NonKYC
- **Tax impact:** Non-taxable; cost basis carries over from source
- **Rule:** Deposit matched as TRANSFER_IN by the transfer matcher (tx_hash or amount+timing)

### INTERNAL_TRANSFER_OUT
- **Definition:** Crypto sent to another tracked exchange/wallet
- **Examples:** BTC sent from MEXC to NonKYC
- **Tax impact:** Non-taxable (but network fee may be a taxable disposal)
- **Rule:** Withdrawal matched as TRANSFER_OUT by the transfer matcher

### INCOME_RECEIPT
- **Definition:** Staking reward, airdrop, referral bonus, pool reward
- **Examples:** SAL staking reward, promotional token airdrop
- **Tax impact:** Ordinary income at FMV when dominion gained
- **Rule:** Deposit classified as income by the income classifier

### UNCLASSIFIED
- **Definition:** Deposit or withdrawal that needs manual review
- **Examples:** Ambiguous deposits with no clear source
- **Tax impact:** Blocks filing until manually classified
- **Rule:** Does not fit any of the above categories with sufficient confidence

## Why Internal Transfers Are Excluded from Funding Totals

When you transfer BTC from MEXC to NonKYC:
- MEXC shows a withdrawal (money leaving)
- NonKYC shows a deposit (money arriving)
- Net effect: $0 (it's the same money moving between your accounts)

Including both would double-count your capital flows. The classified system
ensures the accountant only sees genuine external funding movements.

## API Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /v4/classify-flows` | Run the classification engine |
| `GET /v4/funding-by-exchange` | Get classified totals by exchange |
| The **Funding Flows** tab in XLSX export | Full breakdown for the accountant |
