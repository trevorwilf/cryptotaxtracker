# Wallet Ownership Guide

How to register wallet entities, accounts, addresses, and ownership claims.
This is required to correctly classify self-transfers as non-taxable.

## Why This Matters

Without address claims, the system cannot distinguish:
- **Self-transfer** (MEXC → NonKYC): Non-taxable, basis carries over
- **External withdrawal** (MEXC → someone else): May be a gift or payment

Unclaimed addresses result in **UNCLASSIFIED** flows that block filing.

## Concepts

### Wallet Entity
A person or business that owns wallets. Typically "taxpayer" for personal use.

### Wallet Account
An exchange account or hardware wallet owned by an entity.

### Wallet Address
A specific blockchain address associated with an account.

### Address Claim
An assertion that a specific address is self-owned, with confidence level.

## Workflow

### 1. Create an Entity

```
POST /v4/wallet/entities?entity_type=taxpayer&label=My Personal Wallets
```

### 2. Create Accounts

```
POST /v4/wallet/accounts?entity_id=1&account_type=exchange&exchange_name=mexc&label=MEXC Main Account
POST /v4/wallet/accounts?entity_id=1&account_type=exchange&exchange_name=nonkyc&label=NonKYC Account
POST /v4/wallet/accounts?entity_id=1&account_type=hardware_wallet&label=Ledger Nano X
```

### 3. Register Addresses

```
POST /v4/wallet/addresses?account_id=1&address=bc1qz9p0s296sf07tlcz20sz6n5suf2rk7fg83kanl&chain=bitcoin&label=BTC withdrawal address
POST /v4/wallet/addresses?account_id=3&address=SC11aHNa...NewhG&chain=salvium&label=SAL cold storage
```

### 4. Claim Ownership

```
POST /v4/wallet/claims?address_id=1&claim_type=self_owned&confidence=verified&evidence_summary=This is my personal BTC address on my Ledger
```

### 5. Auto-Discover

```
POST /v4/wallet/auto-discover
```

Scans all deposits and withdrawals for addresses that appear on multiple exchanges,
suggesting likely self-owned addresses.

### 6. Check an Address

```
GET /v4/wallet/addresses/check/bc1qz9p0s296sf07tlcz20sz6n5suf2rk7fg83kanl
```

Returns ownership status, entity, account, and confidence.

## Claim Types

| Type | Meaning |
|------|---------|
| `self_owned` | Address belongs to the taxpayer |
| `counterparty` | Known third-party (exchange hot wallet, merchant) |
| `exchange_hot_wallet` | Exchange's internal hot wallet address |
| `unknown` | Ownership not determined |

## Confidence Levels

| Level | When to Use |
|-------|-------------|
| `verified` | Manually confirmed by the taxpayer |
| `high` | Auto-discovered cross-exchange match |
| `medium` | Pattern-based inference |
| `low` | Guess based on limited evidence |
| `unverified` | Placeholder, needs review |

## Example: MEXC → NonKYC Transfer

1. You withdraw BTC from MEXC to address `bc1q...abc`
2. NonKYC shows a deposit from `bc1q...abc`
3. Register `bc1q...abc` under your NonKYC account
4. Claim it as `self_owned` with `verified` confidence
5. Next compute run: transfer matcher uses the claim to boost match confidence
6. Flow classifier marks the withdrawal as `INTERNAL_TRANSFER_OUT` and deposit as `INTERNAL_TRANSFER_IN`
7. No taxable event — basis carries over from MEXC to NonKYC
