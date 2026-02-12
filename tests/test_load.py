#!/usr/bin/env python3
"""
Load testing for the webhook endpoint.

Tests:
1. Concurrent webhook requests
2. Large payload handling
3. Burst traffic scenarios
4. Rate limiting behavior
5. Database connection pool handling
"""
import pytest
import json
import hashlib
import concurrent.futures
from datetime import datetime, timezone
from api.webhooks import router
from fastapi.testclient import TestClient
from fastapi import FastAPI

# Create test app
app = FastAPI()
app.include_router(router)
client = TestClient(app)


def generate_test_transaction(index: int):
    """Generate a unique test transaction."""
    return {
        "signature": f"load_test_tx_{index}",
        "slot": 100000 + index,
        "timestamp": int(datetime.now(timezone.utc).timestamp()),
        "events": {
            "swap": {
                "program": "test_dex",
                "tokenInputs": [
                    {
                        "mint": "TOKEN_TEST",
                        "userAccount": f"wallet_{index}",
                        "rawTokenAmount": {"tokenAmount": str(100 * index)}
                    }
                ]
            }
        }
    }


def send_webhook_request(payload_data):
    """Send a single webhook request."""
    payload_json = json.dumps(payload_data).encode()
    headers = {"authorization": "x-helius-signature:test_secret"}
    
    try:
        response = client.post("/webhooks/helius", content=payload_json, headers=headers)
        return {
            "status_code": response.status_code,
            "success": response.status_code == 200,
            "response": response.json() if response.status_code == 200 else None,
        }
    except Exception as e:
        return {
            "status_code": 500,
            "success": False,
            "error": str(e),
        }


def test_concurrent_requests():
    """Test handling of concurrent webhook requests."""
    num_requests = 20
    payloads = [[generate_test_transaction(i)] for i in range(num_requests)]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(send_webhook_request, payload) for payload in payloads]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    # Check that all requests succeeded
    successful = sum(1 for r in results if r["success"])
    assert successful == num_requests, f"Only {successful}/{num_requests} requests succeeded"
    
    # Verify no exceptions
    errors = [r.get("error") for r in results if "error" in r]
    assert len(errors) == 0, f"Errors occurred: {errors}"


def test_large_payload():
    """Test handling of large webhook payloads."""
    # Generate payload with 100 transactions
    large_payload = [generate_test_transaction(i) for i in range(100)]
    
    result = send_webhook_request(large_payload)
    
    assert result["success"], f"Large payload failed: {result.get('error')}"
    assert result["response"]["status"] == "ok"
    assert result["response"]["events_received"] == 100


def test_duplicate_payload_handling():
    """Test that duplicate payloads are properly rejected."""
    payload = [generate_test_transaction(999)]
    
    # First request
    result1 = send_webhook_request(payload)
    assert result1["success"]
    assert result1["response"]["status"] == "ok"
    
    # Second request (duplicate)
    result2 = send_webhook_request(payload)
    assert result2["success"]
    assert result2["response"].get("replay") == "ignored"


def test_burst_traffic():
    """Test handling of burst traffic (many requests in short time)."""
    num_bursts = 50
    
    # Create unique payloads to avoid replay protection
    payloads = [[generate_test_transaction(1000 + i)] for i in range(num_bursts)]
    
    # Send all requests as fast as possible
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(send_webhook_request, payload) for payload in payloads]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]
    
    # Most should succeed (some might fail due to rate limiting, but most should work)
    successful = sum(1 for r in results if r["success"])
    success_rate = successful / num_bursts
    
    assert success_rate >= 0.8, f"Only {success_rate*100:.1f}% success rate in burst"


def test_malformed_payload_resilience():
    """Test that malformed payloads don't crash the endpoint."""
    malformed_payloads = [
        b"not json",
        b"{}",
        b"[]",
        json.dumps({"invalid": "structure"}).encode(),
        json.dumps([{"signature": "test"}]).encode(),  # Missing required fields
        json.dumps([{"signature": None, "slot": None}]).encode(),
    ]
    
    headers = {"authorization": "x-helius-signature:test_secret"}
    
    for payload in malformed_payloads:
        response = client.post("/webhooks/helius", content=payload, headers=headers)
        # Should not crash (500 error), should gracefully handle
        assert response.status_code in [200, 400], f"Unexpected status: {response.status_code}"


def test_expired_events_handling():
    """Test that old events are properly filtered out."""
    # Create transaction with timestamp more than 10 minutes old
    old_timestamp = int(datetime.now(timezone.utc).timestamp()) - 700  # 11+ minutes old
    
    old_tx = {
        "signature": "old_tx_123",
        "slot": 88888,
        "timestamp": old_timestamp,
        "events": {
            "swap": {
                "program": "test_dex",
                "tokenInputs": [
                    {
                        "mint": "TOKEN_TEST",
                        "userAccount": "wallet_old",
                        "rawTokenAmount": {"tokenAmount": "100"}
                    }
                ]
            }
        }
    }
    
    payload = [old_tx]
    result = send_webhook_request(payload)
    
    assert result["success"]
    # Should be marked as expired
    assert result["response"].get("expired") == True


def test_authentication_required():
    """Test that missing or invalid auth is rejected."""
    payload = [generate_test_transaction(9999)]
    payload_json = json.dumps(payload).encode()
    
    # No auth header
    response1 = client.post("/webhooks/helius", content=payload_json)
    assert response1.status_code == 401
    
    # Invalid auth header
    headers_invalid = {"authorization": "x-helius-signature:wrong_secret"}
    response2 = client.post("/webhooks/helius", content=payload_json, headers=headers_invalid)
    assert response2.status_code == 401


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
