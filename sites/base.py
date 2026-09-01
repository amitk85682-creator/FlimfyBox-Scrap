"""
=========================================================================
 sites/base.py — Plugin Interface Contract
=========================================================================
 Every site plugin MUST subclass BaseSitePlugin and implement all three
 async methods. The engine (main.py) calls these methods polymorphically.

 To add a new site, create sites/newsite.py with:
   class SitePlugin(BaseSitePlugin):
       SITE_NAME = "NewSite"
       TARGET_WEBSITE = "https://newsite.example"
       async def get_all_urls(self, context=None): ...
       async def extract_movie_data(self, page): ...
       async def bypass_links(self, context, browser, raw_links): ...
=========================================================================
"""


class BaseSitePlugin:
    """
    Abstract base class that every site plugin must implement.

    Class Attributes:
        SITE_NAME       (str): Human-readable site name for logging.
        TARGET_WEBSITE  (str): Base URL of the target website.
        WATCHDOG_LIMIT  (int): Max URLs to process in watchdog mode (default 50).
    """

    SITE_NAME: str = ""
    TARGET_WEBSITE: str = ""
    WATCHDOG_LIMIT: int = 50

    # ------------------------------------------------------------------
    # 1. URL DISCOVERY
    # ------------------------------------------------------------------
    async def get_all_urls(self, context=None, watchdog_mode=False):
        """
        Return a list of ALL movie/post URLs from the site.

        Args:
            context: An optional Playwright BrowserContext that the plugin
                     can use for JS-rendered page crawling. Plugins that
                     use XML sitemaps via `requests` may ignore this.
            watchdog_mode: If True, the plugin should optimize and only
                     fetch enough pages/data to satisfy WATCHDOG_LIMIT.

        Returns:
            list[str]: Ordered list of movie URLs. Most-recent items should
                       come FIRST (index 0) so that watchdog mode can slice
                       the top N correctly.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_all_urls()"
        )

    # ------------------------------------------------------------------
    # 2. MOVIE DATA EXTRACTION
    # ------------------------------------------------------------------
    async def extract_movie_data(self, page):
        """
        Extract structured metadata from a single movie page.

        The engine will have already navigated the `page` to the movie URL.

        Args:
            page: A Playwright Page object already at the movie URL.

        Returns:
            dict with AT MINIMUM the following keys:
                Raw_Title   (str): Unprocessed title string
                Genre       (str): Genre(s) or 'N/A'
                Stars       (str): Cast or 'N/A'
                Language    (str): Language or 'N/A'
                Description (str): Plot/synopsis or 'N/A'
                IMDb        (str): IMDb rating from page or 'N/A'
                Poster      (str): Poster image URL or ''
                Director    (str): Director or 'N/A'
                Creator     (str): Creator (for TV) or 'N/A'
                Type        (str): 'Movies' or 'Web Series' (page-level hint)
                raw_download_links (list[dict]): Each dict has:
                    quality (str): Quality description text
                    size    (str): File size string or ''
                    url     (str): Intermediate/bypass URL to process

            Return None if extraction fails entirely.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement extract_movie_data()"
        )

    # ------------------------------------------------------------------
    # 3. DOWNLOAD LINK BYPASS
    # ------------------------------------------------------------------
    async def bypass_links(self, context, browser, raw_links):
        """
        Process raw intermediate download links into final direct URLs.

        Args:
            context:   The main Playwright BrowserContext (for lightweight
                       operations like opening new pages).
            browser:   The Playwright Browser instance (for creating
                       isolated contexts when needed, e.g., HubCloud bypass).
            raw_links: list[dict] from extract_movie_data's
                       'raw_download_links' field.

        Returns:
            list[dict], each with:
                quality      (str):  Quality description
                size         (str):  File size string
                direct_links (list[dict]): Each has:
                    server_name (str): Human-readable server name
                    url         (str): Final direct download URL
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement bypass_links()"
        )
