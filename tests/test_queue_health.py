import pytest
from unittest.mock import MagicMock, patch
import queue_health

@pytest.fixture
def mock_db():
    with patch('queue_health.get_db_connection') as mock_conn:
        conn = MagicMock()
        mock_conn.return_value = conn
        cur = MagicMock()
        conn.cursor.return_value = cur
        yield conn, cur

def test_fetch_health_metrics_is_read_only(mock_db):
    conn, cur = mock_db
    
    # Setup mock returns for the 5 execute calls
    cur.fetchall.side_effect = [
        # 1. Overall
        [("pending", 10), ("processing", 2), ("completed", 5)],
        # 2. Per-Site
        [("filmyzilla", "pending", 5), ("mkvcinemas", "processing", 2)],
        # 4. Recovery (skipping 3 because it's fetchone)
        [("filmyzilla", 3)],
        # 5. Crawl health
        [("filmyzilla", "worker", None, "success", 0, 10, 0, 120.5)]
    ]
    cur.fetchone.side_effect = [
        # 3. Ages
        (10.5, 2.0, 1.5, 5.0, 0)
    ]
    
    metrics = queue_health.fetch_health_metrics()
    
    # Verify set_session(readonly=True) was called
    conn.set_session.assert_called_once_with(readonly=True)
    
    # Verify all executed queries are SELECTs
    for call in cur.execute.call_args_list:
        query = call[0][0].strip().upper()
        assert query.startswith("SELECT"), f"Query must be read-only SELECT, found: {query}"
        assert "UPDATE" not in query
        assert "DELETE" not in query
        assert "INSERT" not in query
        assert "FOR UPDATE" not in query

def test_metrics_aggregation(mock_db):
    conn, cur = mock_db
    
    cur.fetchall.side_effect = [
        [("pending", 15), ("dead", 1)],
        [("hdhub4u", "pending", 15)],
        [("hdhub4u", 2)],
        [("hdhub4u", "discovery", None, "success", 100, 0, 0, 45.0)]
    ]
    cur.fetchone.side_effect = [
        (100.0, None, None, None, 0)
    ]
    
    metrics = queue_health.fetch_health_metrics()
    
    assert metrics["overall"]["pending"] == 15
    assert metrics["overall"]["dead"] == 1
    assert metrics["per_site"]["hdhub4u"]["pending"] == 15
    assert metrics["ages"]["oldest_pending_minutes"] == 100.0
    assert metrics["ages"]["oldest_processing_minutes"] is None
    assert metrics["recovery"]["high_retry_jobs_total"] == 2
    assert metrics["recovery"]["high_retry_jobs_per_site"]["hdhub4u"] == 2
    assert metrics["crawl_health"]["hdhub4u"]["discovery"]["urls_discovered"] == 100
