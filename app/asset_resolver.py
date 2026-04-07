"""
Canonical asset resolution across exchanges.

MEXC withdrawal API joins coin+network with a hyphen (e.g., SAL-SALVIUM1).
MEXC coin config confirms zero coins natively have hyphens (out of 9,156).
So any hyphenated MEXC withdrawal asset can be split on hyphen to get the
canonical coin name.
"""
import logging
from sqlalchemy import text

logger = logging.getLogger("tax-collector.asset-resolver")

_alias_cache: dict[tuple, str] = {}


async def load_aliases(session):
    """Load alias cache from DB."""
    global _alias_cache
    try:
        r = await session.execute(text(
            "SELECT exchange, raw_asset, network, canonical_asset FROM tax.asset_aliases"))
        _alias_cache = {}
        for row in r.fetchall():
            ex, raw, net, canon = row
            _alias_cache[(ex, raw, net)] = canon
            _alias_cache[(None, raw, net)] = canon
    except Exception as e:
        logger.debug(f"Could not load asset aliases (table may not exist): {e}")


def resolve_canonical(raw_asset: str, exchange: str = None, network: str = None) -> str:
    """Resolve a raw exchange asset symbol to its canonical form."""
    if not raw_asset:
        return raw_asset

    upper = raw_asset.upper().strip()

    # 1. Check explicit alias cache
    for key in [(exchange, upper, network), (None, upper, network),
                (exchange, upper, None), (None, upper, None)]:
        if key in _alias_cache:
            return _alias_cache[key]

    # 2. MEXC-specific: withdrawal API joins coin-network with hyphen
    #    Since zero MEXC coins natively have hyphens, we can safely split
    if exchange == "mexc" and "-" in upper and network:
        parts = upper.split("-", 1)
        if parts[1] == network.upper():
            return parts[0]

    # 3. General fallback: if asset contains hyphen and network matches suffix
    if "-" in upper and network:
        parts = upper.split("-", 1)
        if parts[1] == network.upper():
            return parts[0]

    # 4. Even without network, try splitting MEXC hyphenated assets
    if exchange == "mexc" and "-" in upper:
        return upper.split("-", 1)[0]

    return upper


async def ensure_alias(session, exchange: str, raw_asset: str, network: str,
                       canonical: str, notes: str = None):
    """Insert or update an alias mapping."""
    await session.execute(text("""
        INSERT INTO tax.asset_aliases (exchange, raw_asset, network, canonical_asset, notes)
        VALUES (:ex, :raw, :net, :canon, :notes)
        ON CONFLICT ON CONSTRAINT uq_asset_alias
        DO UPDATE SET canonical_asset = :canon, notes = :notes
    """), {"ex": exchange, "raw": raw_asset.upper(), "net": network,
           "canon": canonical.upper(), "notes": notes})
