import pytest
import asyncio
from unittest.mock import patch
from sites.cinejoy import SitePlugin
import main

@pytest.fixture
def plugin():
    return SitePlugin()

def test_plugin_properties(plugin):
    assert plugin.SITE_NAME == "CineJoy"
    assert plugin.TARGET_WEBSITE == "https://cinejoy.to"

@patch("main.DB_POOL_SIZE", 4)
def test_cinejoy_site_config():
    # Test that get_site_config correctly resolves the lowercased key
    config = main.get_site_config("CineJoy")
    assert config["enabled"] is True
    assert config["max_active"] == 1

@patch("main.DB_POOL_SIZE", 4)
def test_existing_site_configs():
    # Verify the fix works for other sites too
    fz_config = main.get_site_config("FilmyZilla")
    assert fz_config["enabled"] is True
    
    hd_config = main.get_site_config("HDHub4u")
    assert hd_config["enabled"] is True
    
    mkv_config = main.get_site_config("MKVCinemas")
    assert mkv_config["enabled"] is False
    
    unknown = main.get_site_config("UnknownSite")
    assert unknown["enabled"] is True
    assert unknown["max_active"] == 2

def test_parse_cinejoy_raw_info_movie(plugin):
    raw = "Vidplay | 1080p | 1.2 GB | HC-SUB"
    res = plugin._parse_cinejoy_raw_info(raw, "Movie", "Full Movie")
    assert res["server_name"] == "Vidplay"
    assert res["quality"] == "1080p HC-SUB"
    assert res["size"] == "1.2 GB"
    assert res["extra_info"] == ""

def test_parse_cinejoy_raw_info_series(plugin):
    raw = "MyCloud | 720p | 300 MB | "
    res = plugin._parse_cinejoy_raw_info(raw, "Season 2", "Episode 5")
    assert res["server_name"] == "MyCloud"
    assert res["quality"] == "720p"
    assert res["size"] == "300 MB"
    assert res["extra_info"] == "S02E05"

def test_parse_cinejoy_raw_info_special(plugin):
    raw = "Filemoon | HD | |"
    res = plugin._parse_cinejoy_raw_info(raw, "Specials", "Episode 1")
    assert res["server_name"] == "Filemoon"
    assert res["quality"] == "HD"
    assert res["size"] == ""
    assert res["extra_info"] == "S00E01"

def test_parse_cinejoy_raw_info_malformed(plugin):
    raw = "JustServer"
    res = plugin._parse_cinejoy_raw_info(raw, "", "")
    assert res["server_name"] == "JustServer"
    assert res["quality"] == "HD"
    assert res["size"] == ""
    assert res["extra_info"] == ""

def test_no_credentials_in_plugin():
    with open("sites/cinejoy.py", "r", encoding="utf-8") as f:
        content = f.read()
    assert "DATABASE_URL" not in content
    assert "TMDB_KEY" not in content
    assert "postgres://" not in content

def test_bypass_links_movie(plugin):
    raw_links = [{
        "season": "Movie",
        "episode": "Full Movie",
        "raw_info": "Vidplay | 1080p | 2 GB |",
        "url": "https://example.com/dl/123"
    }]
    
    bypassed = asyncio.run(plugin.bypass_links(None, None, raw_links))
    
    assert len(bypassed) == 1
    assert bypassed[0]["quality"] == "1080p"
    assert bypassed[0]["size"] == "2 GB"
    assert bypassed[0]["extra_info"] == ""
    assert bypassed[0]["direct_links"][0]["server_name"] == "Vidplay"
    assert bypassed[0]["direct_links"][0]["url"] == "https://example.com/dl/123"

def test_bypass_links_series(plugin):
    raw_links = [{
        "season": "Season 1",
        "episode": "Episode 2",
        "raw_info": "Server2 | 720p | 500 MB | Subs",
        "url": "https://example.com/dl/456"
    }]
    
    bypassed = asyncio.run(plugin.bypass_links(None, None, raw_links))
    
    assert len(bypassed) == 1
    assert bypassed[0]["quality"] == "720p Subs"
    assert bypassed[0]["extra_info"] == "S01E02"
    assert bypassed[0]["direct_links"][0]["server_name"] == "Server2"
