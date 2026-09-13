import pytest
import os
from unittest.mock import patch
import main

@patch("main.DB_POOL_SIZE", 4)
def test_site_config_filmyzilla():
    config = main.get_site_config("filmyzilla")
    assert config["enabled"] is True
    assert config["max_active"] == 2

@patch("main.DB_POOL_SIZE", 4)
def test_site_config_hdhub4u():
    config = main.get_site_config("hdhub4u")
    assert config["enabled"] is True
    assert config["max_active"] == 2

@patch("main.DB_POOL_SIZE", 4)
def test_site_config_mkvcinemas():
    config = main.get_site_config("mkvcinemas")
    assert config["enabled"] is False
    assert config["max_active"] == 2

@patch("main.DB_POOL_SIZE", 4)
def test_site_config_unknown_site():
    config = main.get_site_config("unknown_new_site")
    # Should get safe defaults
    assert config["enabled"] is True
    assert config["max_active"] == 2

@patch("main.DB_POOL_SIZE", 3)
@patch.dict(main.SITE_CONFIG, {"high_concurrency_site": {"max_active": 50}})
def test_site_config_safe_bounds():
    # If DB_POOL_SIZE is 3, max_safe_active is max(1, 3 - 2) = 1
    # Even if site wants 50, it should be capped at 1.
    config = main.get_site_config("high_concurrency_site")
    assert config["max_active"] == 1

@patch("main.DB_POOL_SIZE", 1)
@patch.dict(main.SITE_CONFIG, {"bad_site": {"max_active": -5}})
def test_site_config_no_zero_or_negative():
    # DB_POOL_SIZE is 1, max_safe_active is max(1, 1 - 2) = 1
    # If site asks for -5, it should be floored at 1.
    config = main.get_site_config("bad_site")
    assert config["max_active"] == 1
