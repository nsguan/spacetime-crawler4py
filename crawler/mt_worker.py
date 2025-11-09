# crawler/mt_worker.py
from threading import Thread

from scraper import scraper
from utils.download import download
from utils import get_logger


class Worker(Thread):
    """
    Multi-threaded worker that cooperates with the thread-safe Frontier.
    """

    def __init__(self, worker_id: int, config, frontier):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.config = config
        self.frontier = frontier
        self.logger = get_logger(f"Worker-{worker_id}")

    def run(self):
        while True:
            url = self.frontier.get_tbd_url()
            if url is None:
                self.logger.info("No more URLs to crawl, exiting.")
                break

            self.logger.info(f"Worker-{self.worker_id} downloading {url}")
            resp = download(url, self.config)

            try:
                next_links = scraper(url, resp) or []
            except Exception as e:
                self.logger.error(f"Error in scraper for {url}: {e}")
                next_links = []

            for link in next_links:
                self.frontier.add_url(link)

            self.frontier.mark_url_complete(url)

            # No sleep here: the Frontier enforces per-domain politeness.