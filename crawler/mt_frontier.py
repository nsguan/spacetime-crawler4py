# crawler/mt_frontier.py
import time
from collections import deque
from threading import Lock
from urllib.parse import urlparse

from utils import get_logger


class Frontier:
    """
    Thread-safe frontier with per-domain politeness.

    Implements the same interface as crawler/frontier.py:
      - __init__(config, restart)
      - get_tbd_url()
      - add_url(url)
      - mark_url_complete(url)
    """
    def __init__(self, config, restart: bool):
        self.logger = get_logger("ThreadedFrontier")
        self.config = config

        self._queue = deque()
        self._seen = set()
        self._completed = set()
        self._in_progress = 0
        self._domain_next_allowed = {}
        self._lock = Lock()

        delay = getattr(self.config, "time_delay", 0.5)
        self._delay = max(delay, 0.5)

        # ----- FIXED SEED HANDLING -----
        seeds = list(getattr(self.config, "seed_urls", []))
        if not seeds:
            raise ValueError("Config missing seed_urls")

        for url in seeds:
            self.add_url(url)

        self.logger.info(
            f"ThreadedFrontier initialized with {len(seeds)} seeds, "
            f"politeness {self._delay}s"
        )

    # ---------- public API used by Crawler / Workers ----------

    def get_tbd_url(self):
        """
        Get one URL to be downloaded, respecting per-domain politeness
        across all threads.

        Returns:
          - url (str) or
          - None if there is nothing left to crawl.
        """
        while True:
            with self._lock:
                if not self._queue:
                    # No URLs *currently* queued.
                    # If nothing in progress, we're truly done.
                    if self._in_progress == 0:
                        return None
                    # Otherwise, some worker is still crawling and may add URLs.
                    # Fall through to sleep below.
                    sleep_time = self._delay
                else:
                    now = time.time()
                    min_wait = None

                    # Try each URL in the queue once per pass
                    for _ in range(len(self._queue)):
                        url = self._queue.popleft()
                        domain = urlparse(url).netloc

                        next_allowed = self._domain_next_allowed.get(domain, 0.0)
                        wait = max(0.0, next_allowed - now)

                        if wait <= 0:
                            # Reserve this domain and return URL
                            self._domain_next_allowed[domain] = now + self._delay
                            self._in_progress += 1      # <-- NEW
                            return url

                        # Not ready yet; put it back at the end
                        self._queue.append(url)
                        if min_wait is None or wait < min_wait:
                            min_wait = wait

                    # No URL is ready yet. Decide how long to sleep before retry.
                    sleep_time = max(min_wait or self._delay, 0.01)

            # IMPORTANT: sleep outside the lock
            time.sleep(sleep_time)

    def add_url(self, url: str):
        """Add one URL to the frontier if we haven't seen it before."""
        if not url:
            return
        with self._lock:
            if url in self._seen or url in self._completed:
                return
            self._seen.add(url)
            self._queue.append(url)

    def mark_url_complete(self, url: str):
        """Mark a URL as completed so we do not re-crawl it."""
        if not url:
            return
        with self._lock:
            self._completed.add(url)
            # One less URL being processed
            if self._in_progress > 0:
                self._in_progress -= 1