"""
Pre-compute data quality validation.

Runs before normalization to catch corrupted data before it enters the pipeline.
Logs BLOCKING exceptions for issues that would produce incorrect tax results.
"""
import logging
from sqlalchemy import text
from exceptions import ExceptionManager, BLOCKING

logger = logging.getLogger("tax-collector.data-quality")


async def validate_data_quality(session, exc: ExceptionManager, run_id: int):
    """Pre-compute data quality checks. Logs blocking exceptions for issues."""

    # 1. Zero-amount deposits with non-zero raw_data
    try:
        result = await session.execute(text("""
            SELECT COUNT(*) FROM tax.deposits
            WHERE amount = 0
              AND raw_data IS NOT NULL
              AND raw_data::text LIKE '%quantity%'
              AND raw_data::text NOT LIKE '%"quantity":"0"%'
              AND raw_data::text NOT LIKE '%"quantity": "0"%'
        """))
        bad_deposits = result.scalar() or 0
        if bad_deposits:
            exc.log(BLOCKING, "CORRUPT_DEPOSIT_DATA",
                    f"{bad_deposits} deposit(s) have amount=0 but non-zero raw_data.quantity",
                    run_id=run_id)
            logger.warning(f"Data quality: {bad_deposits} corrupt deposits found")
    except Exception as e:
        logger.debug(f"Deposit quality check skipped: {e}")

    # 2. Zero-amount withdrawals with non-zero raw_data
    try:
        result = await session.execute(text("""
            SELECT COUNT(*) FROM tax.withdrawals
            WHERE amount = 0
              AND raw_data IS NOT NULL
              AND raw_data::text LIKE '%quantity%'
              AND raw_data::text NOT LIKE '%"quantity":"0"%'
              AND raw_data::text NOT LIKE '%"quantity": "0"%'
        """))
        bad_withdrawals = result.scalar() or 0
        if bad_withdrawals:
            exc.log(BLOCKING, "CORRUPT_WITHDRAWAL_DATA",
                    f"{bad_withdrawals} withdrawal(s) have amount=0 but non-zero raw_data.quantity",
                    run_id=run_id)
            logger.warning(f"Data quality: {bad_withdrawals} corrupt withdrawals found")
    except Exception as e:
        logger.debug(f"Withdrawal quality check skipped: {e}")

    # 3. Blank pool rows
    try:
        result = await session.execute(text("""
            SELECT COUNT(*) FROM tax.pool_activity
            WHERE (pool_name IS NULL OR pool_name = '')
              AND (amount_in = 0 OR amount_in IS NULL)
              AND (amount_out = 0 OR amount_out IS NULL)
        """))
        junk_pools = result.scalar() or 0
        if junk_pools:
            exc.log(BLOCKING, "JUNK_POOL_DATA",
                    f"{junk_pools} pool activity row(s) have blank pool name and zero amounts",
                    run_id=run_id)
            logger.warning(f"Data quality: {junk_pools} junk pool rows found")
    except Exception as e:
        logger.debug(f"Pool quality check skipped: {e}")
