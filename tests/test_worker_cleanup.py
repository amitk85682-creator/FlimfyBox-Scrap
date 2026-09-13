import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import main

@pytest.fixture
def mock_dependencies():
    with patch("main.get_db_connection") as mock_get_db, \
         patch("main.release_db_connection") as mock_release_db, \
         patch("main.recover_expired_leases") as mock_recover, \
         patch("main.cleanup_old_jobs") as mock_cleanup, \
         patch("main.get_queue_stats") as mock_stats, \
         patch("main.HeartbeatManager") as mock_hb, \
         patch("main.async_playwright") as mock_pw:
         
        mock_get_db.return_value = MagicMock()
        mock_recover.return_value = 5
        mock_cleanup.return_value = 10
        mock_stats.return_value = {"pending": 0}
        
        hb_instance = MagicMock()
        mock_hb.return_value = hb_instance
        
        yield {
            "get_db": mock_get_db,
            "release_db": mock_release_db,
            "recover": mock_recover,
            "cleanup": mock_cleanup,
            "stats": mock_stats,
            "hb": mock_hb
        }

def test_worker_startup_cleanup_invoked(mock_dependencies):
    plugin = MagicMock()
    plugin.SITE_NAME = "test_site"
    
    # We will let the worker loop exit immediately by having asyncio.sleep raise an exception
    with patch("main.claim_next_job", return_value=None):
        with patch("main.create_crawl_run", return_value=123):
            with patch("main.finish_crawl_run"):
                with patch("asyncio.sleep", side_effect=Exception("Exit Loop")):
                    try:
                        asyncio.run(main.run_worker_mode(plugin, max_jobs=5))
                    except Exception as e:
                        assert str(e) == "Exit Loop"
    
    # Assert cleanup was called exactly once with the connection
    conn = mock_dependencies["get_db"].return_value
    mock_dependencies["cleanup"].assert_called_once_with(conn)

def test_worker_startup_cleanup_failure_handled(mock_dependencies):
    plugin = MagicMock()
    plugin.SITE_NAME = "test_site"
    
    # Force cleanup to fail
    mock_dependencies["cleanup"].side_effect = Exception("Simulated cleanup failure")
    
    with patch("main.claim_next_job", return_value=None) as mock_claim:
        with patch("main.create_crawl_run", return_value=123):
            with patch("main.finish_crawl_run"):
                with patch("asyncio.sleep", side_effect=Exception("Exit Loop")):
                    try:
                        asyncio.run(main.run_worker_mode(plugin, max_jobs=5))
                    except Exception as e:
                        assert str(e) == "Exit Loop"
    
    # Assert cleanup was called and failed, but execution continued to the loop (claim_next_job was called)
    conn = mock_dependencies["get_db"].return_value
    mock_dependencies["cleanup"].assert_called_once_with(conn)
    mock_claim.assert_called_once()
