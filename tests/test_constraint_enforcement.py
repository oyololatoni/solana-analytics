#!/usr/bin/env python3
"""
Test database constraint enforcement directly.

Tests:
1. Composite unique constraint (tx_signature, event_type, wallet)
2. NOT NULL constraints
3. Index existence and performance
4. Foreign key constraints (if any)
"""
import pytest
import json
from datetime import datetime, timezone
from db import get_conn


def test_composite_unique_constraint():
    """Test that the composite unique constraint works correctly."""
    conn = get_conn()
    cur = conn.cursor()
    
    base_data = {
        "tx_signature": "constraint_test_001",
        "slot": 12345,
        "event_type": "swap",
        "wallet": "wallet_test",
        "token_mint": "TOKEN_TEST",
        "amount": 100,
        "block_time": datetime.now(timezone.utc),
        "program_id": "test_program",
        "metadata": json.dumps({"test": "data"}),
    }
    
    # Insert first record
    cur.execute(
        """
        INSERT INTO events (
            tx_signature, slot, event_type, wallet, token_mint,
            amount, block_time, program_id, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        tuple(base_data.values()),
    )
    conn.commit()
    
    # Try to insert exact duplicate (should fail)
    with pytest.raises(Exception):  # Should raise unique constraint violation
        cur.execute(
            """
            INSERT INTO events (
                tx_signature, slot, event_type, wallet, token_mint,
                amount, block_time, program_id, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            tuple(base_data.values()),
        )
        conn.commit()
    
    conn.rollback()
    
    # Same tx_signature but different wallet (should succeed)
    different_wallet = base_data.copy()
    different_wallet["wallet"] = "wallet_different"
    
    cur.execute(
        """
        INSERT INTO events (
            tx_signature, slot, event_type, wallet, token_mint,
            amount, block_time, program_id, metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        tuple(different_wallet.values()),
    )
    conn.commit()
    assert cur.rowcount == 1
    
    # Verify 2 records exist for this tx_signature
    cur.execute(
        "SELECT COUNT(*) FROM events WHERE tx_signature = %s",
        (base_data["tx_signature"],)
    )
    count = cur.fetchone()[0]
    assert count == 2
    
    cur.close()
    conn.close()


def test_not_null_constraints():
    """Test that required fields cannot be NULL."""
    conn = get_conn()
    cur = conn.cursor()
    
    # Try to insert with missing tx_signature (should fail)
    with pytest.raises(Exception):
        cur.execute(
            """
            INSERT INTO events (
                tx_signature, slot, event_type, wallet, token_mint,
                amount, block_time, program_id, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                None,  # tx_signature cannot be NULL
                99999,
                "swap",
                "wallet_x",
                "TOKEN_Y",
                50,
                datetime.now(timezone.utc),
                "prog",
                json.dumps({}),
            ),
        )
        conn.commit()
    
    conn.rollback()
    
    # Try to insert with missing wallet (should fail)
    with pytest.raises(Exception):
        cur.execute(
            """
            INSERT INTO events (
                tx_signature, slot, event_type, wallet, token_mint,
                amount, block_time, program_id, metadata
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                "test_sig_null",
                99999,
                "swap",
                None,  # wallet cannot be NULL
                "TOKEN_Y",
                50,
                datetime.now(timezone.utc),
                "prog",
                json.dumps({}),
            ),
        )
        conn.commit()
    
    conn.rollback()
    cur.close()
    conn.close()


def test_index_existence():
    """Verify that expected indexes exist."""
    conn = get_conn()
    cur = conn.cursor()
    
    # Check for composite unique index
    cur.execute(
        """
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'events' 
          AND indexname = 'idx_events_unique'
        """
    )
    result = cur.fetchone()
    assert result is not None, "Composite unique index 'idx_events_unique' not found"
    
    # Check for token_mint index
    cur.execute(
        """
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'events' 
          AND indexname = 'idx_events_token_mint'
        """
    )
    result = cur.fetchone()
    assert result is not None, "Index 'idx_events_token_mint' not found"
    
    # Check for block_time index
    cur.execute(
        """
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'events' 
          AND indexname = 'idx_events_block_time'
        """
    )
    result = cur.fetchone()
    assert result is not None, "Index 'idx_events_block_time' not found"
    
    cur.close()
    conn.close()


def test_webhook_replays_constraint():
    """Test webhook_replays table unique constraint."""
    conn = get_conn()
    cur = conn.cursor()
    
    test_hash = "test_hash_unique_123"
    
    # First insert
    cur.execute(
        "INSERT INTO webhook_replays (payload_hash) VALUES (%s)",
        (test_hash,)
    )
    conn.commit()
    assert cur.rowcount == 1
    
    # Duplicate insert (should fail)
    with pytest.raises(Exception):  # Should raise unique constraint violation
        cur.execute(
            "INSERT INTO webhook_replays (payload_hash) VALUES (%s)",
            (test_hash,)
        )
        conn.commit()
    
    conn.rollback()
    cur.close()
    conn.close()


def test_ingestion_stats_new_columns():
    """Verify new ignored reason columns exist and accept data."""
    conn = get_conn()
    cur = conn.cursor()
    
    # Insert with all new columns
    cur.execute(
        """
        INSERT INTO ingestion_stats (
            source,
            events_received,
            swaps_inserted,
            swaps_ignored,
            ignored_missing_fields,
            ignored_no_swap_event,
            ignored_no_tracked_tokens,
            ignored_constraint_violation,
            ignored_exception
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        ("test", 100, 80, 20, 5, 3, 7, 2, 3),
    )
    conn.commit()
    assert cur.rowcount == 1
    
    # Verify data
    cur.execute(
        """
        SELECT 
            ignored_missing_fields,
            ignored_no_swap_event,
            ignored_no_tracked_tokens,
            ignored_constraint_violation,
            ignored_exception
        FROM ingestion_stats
        WHERE source = 'test'
        ORDER BY created_at DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    assert row == (5, 3, 7, 2, 3)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
