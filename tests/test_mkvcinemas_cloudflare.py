import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from sites.mkvcinemas import SitePlugin, CloudflareBlockedError

@pytest.fixture
def plugin():
    return SitePlugin()

def test_extract_movie_data_cloudflare_blocked(plugin):
    async def run():
        mock_page = MagicMock()
        mock_page.locator.return_value.first.inner_text = AsyncMock(return_value="mkvcinemas.tl")
        mock_page.title = AsyncMock(return_value="Just a moment...")
        
        with pytest.raises(CloudflareBlockedError, match="CLOUDFLARE_BLOCKED"):
            await plugin.extract_movie_data(mock_page)
    asyncio.run(run())

def test_extract_movie_data_normal_no_aggregator(plugin):
    async def run():
        mock_page = MagicMock()
        mock_page.locator.return_value.first.inner_text = AsyncMock(return_value="Movie Title 2024")
        mock_page.title = AsyncMock(return_value="Movie Title 2024 - MKVCinemas")
        mock_page.evaluate = AsyncMock(side_effect=["http://poster.jpg", None])
        
        result = await plugin.extract_movie_data(mock_page)
        assert result is None
    asyncio.run(run())

def test_extract_movie_data_unrelated_exception(plugin):
    async def run():
        mock_page = MagicMock()
        mock_page.locator.return_value.first.inner_text = AsyncMock(side_effect=Exception("Some arbitrary DOM exception"))
        
        result = await plugin.extract_movie_data(mock_page)
        assert result is None
    asyncio.run(run())

def test_bypass_hubcloud_chain_cloudflare_blocked(plugin):
    async def run():
        mock_context = AsyncMock()
        mock_page = AsyncMock()
        mock_context.new_page.return_value = mock_page
        mock_page.title.return_value = "Attention Required! | Cloudflare"
        
        with pytest.raises(CloudflareBlockedError, match="CLOUDFLARE_BLOCKED"):
            await plugin.bypass_hubcloud_chain(mock_context, "http://hubdrive.link/123")
    asyncio.run(run())

def test_bypass_links_cloudflare_blocked(plugin):
    async def run():
        mock_context = AsyncMock()
        mock_browser = AsyncMock()
        mock_page = AsyncMock()
        mock_context.new_page.return_value = mock_page
        
        mock_page.title.return_value = "Just a moment..."
        raw_links = [{"url": "http://hubcloud.link/123", "quality": "1080p", "size": "1GB"}]
        
        with pytest.raises(CloudflareBlockedError, match="CLOUDFLARE_BLOCKED"):
            await plugin.bypass_links(mock_context, mock_browser, raw_links)
    asyncio.run(run())
