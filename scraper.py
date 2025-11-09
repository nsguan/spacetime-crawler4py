import re
import atexit
import json
from collections import Counter
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup
from threading import Lock
import time

# ---------------------------------------------------
# Global analytics state
# ---------------------------------------------------
seen_urls = set()
word_freq = Counter()
longest_page = {"url": None, "word_count": 0}
subdomain_counts = {}
START_TIME = time.time()

analytics_lock = Lock()

# domains we’re allowed to crawl
ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

# trap words / patterns
TRAP_WORDS = [
    "format=pdf",
    "action=download",
    "share=",
    "sort=",
    "view=all",
    "ngs.ics",
    "wics.ics",
    "wp-content/uploads",
    "doku.php",
    "do=diff",
    "do=edit",
    "do=revisions",
    "do=backlink",
    "do=media",
    "namespace=",
    "idx=",
]

# Basic English stopwords
STOPWORDS = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
    "any", "are", "aren't", "as", "at", "be", "because", "been", "before",
    "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't",
    "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
    "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how",
    "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is",
    "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most",
    "mustn't", "my", "myself", "no", "nor", "not", "now", "of", "off", "on",
    "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out",
    "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's",
    "should", "shouldn't", "so", "some", "such", "than", "that", "that's",
    "the", "their", "theirs", "them", "themselves", "then", "there", "there's",
    "these", "they", "they'd", "they'll", "they're", "they've", "this", "those",
    "through", "to", "too", "under", "until", "up", "very", "was", "wasn't",
    "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's",
    "when", "when's", "where", "where's", "which", "while", "who", "who's",
    "whom", "why", "why's", "will", "with", "won't", "would", "wouldn't", "you",
    "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves",
    # custom UI / site noise words
    "support", "media", "home", "contact", "menu", "login", "copyright",
    "cookie", "privacy", "wiki", "files", "tools", "png", "page", "manager",
    "upload", "services", "kb", "sitemap", "user", "webapps", "log", "recent",
    "changes", "root", "namespaces", "backlinks", "thumbnails", "jpg",
}

def dump_stats():
    end_time = time.time()
    elapsed = end_time - START_TIME
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    elapsed_hms = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    with open("stats.json", "w") as f:
        json.dump(
            {
                "unique_urls": len(seen_urls),
                "longest_page": longest_page,
                "word_freq": word_freq.most_common(200),
                "subdomains": subdomain_counts,                
                "run_start": START_TIME,
                "run_end": end_time,
                "elapsed_time": elapsed_hms,
            },
            f,
            indent=2,
        )

atexit.register(dump_stats)

# ---------------------------------------------------
# Scraper
# ---------------------------------------------------

def scraper(url, resp):
    if resp.status != 200 or resp.raw_response is None:
        return []

    raw = resp.raw_response
    clean_url, _ = urldefrag(raw.url or url)

    content_type = raw.headers.get("Content-Type", "")
    num_words = 0  # default

    if "text/html" in content_type:
        try:
            soup = BeautifulSoup(raw.content, "lxml")
        except Exception:
            soup = None

        if soup is not None:
            for tag in soup(["script", "style", "noscript"]):
                tag.extract()

            text = soup.get_text(separator=" ")
            tokens = re.findall(r"[a-zA-Z]+", text.lower())
            filtered = [t for t in tokens if t not in STOPWORDS and len(t) > 1]
            num_words = len(tokens)

            # ---- analytics: guard with lock ----
            with analytics_lock:
                seen_urls.add(clean_url)
                word_freq.update(filtered)

                if num_words > longest_page["word_count"]:
                    longest_page["word_count"] = num_words
                    longest_page["url"] = clean_url

                parsed = urlparse(clean_url)
                host = (parsed.hostname or "").lower()
                if host.endswith(".uci.edu"):
                    subdomain_counts[host] = subdomain_counts.get(host, 0) + 1
    else:
        # not HTML, but still want to count URL as seen
        with analytics_lock:
            seen_urls.add(clean_url)
    
    links = extract_next_links(url, resp)

    if num_words < 50:
    # low-information page: don’t expand it further
        return []
    return [link for link in links if is_valid(link)]


def extract_next_links(url, resp):
    # Return a list with the hyperlinks (as strings) scraped from the page
    links = []

    raw = resp.raw_response
    if raw is None:
        return links

    content_type = raw.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return links

    try:
        soup = BeautifulSoup(raw.content, "lxml")
    except Exception:
        return links

    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    base_url = raw.url or url

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue

        absolute = urljoin(base_url, href)
        absolute, _ = urldefrag(absolute)

        # skip mailto: javascript: etc.
        if absolute.startswith("mailto:") or absolute.startswith("javascript:"):
            continue

        links.append(absolute)

    return links


def is_valid(url):
    # Decide whether to crawl this url or not.
    try:
        parsed = urlparse(url)

        # Only http / https
        if parsed.scheme not in {"http", "https"}:
            return False

        # Hostname check (domains)
        hostname = parsed.hostname
        if hostname is None:
            return False

        hostname = hostname.lower()

        # must be in one of the allowed domains or subdomains
        if not any(
            hostname == d or hostname.endswith("." + d)
            for d in ALLOWED_DOMAINS
        ):
            return False

        path = parsed.path.lower()
        query = (parsed.query or "").lower()
        full = path + "?" + query

        # Trap words / patterns (from class Discord)
        for t in TRAP_WORDS:
            if t in full:
                return False

        # File extensions to skip
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$",
            path,
        ):
            return False

        return True

    except TypeError:
        print("TypeError for ", url)
        return False
