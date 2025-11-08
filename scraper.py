import re
import atexit
import json
from collections import Counter
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup

# ---------------------------------------------------
# Global analytics state
# ---------------------------------------------------

seen_urls = set()
word_freq = Counter()
longest_page = {"url": None, "word_count": 0}
subdomain_counts = {}

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
    "ngs.ics",              # NGS trap
    "wics.ics",             # WICS trap
    "wp-content/uploads",   # lots of pdfs/media
]

# Basic English stopwords – you can tweak this to match the list from the spec
STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any","are",
    "as","at","be","because","been","before","being","below","between","both","but",
    "by","could","did","do","does","doing","down","during","each","few","for","from",
    "further","had","has","have","having","he","her","here","hers","herself","him",
    "himself","his","how","i","if","in","into","is","it","its","itself","just",
    "me","more","most","my","myself","no","nor","not","now","of","off","on","once",
    "only","or","other","our","ours","ourselves","out","over","own","same","she",
    "should","so","some","such","than","that","the","their","theirs","them",
    "themselves","then","there","these","they","this","those","through","to","too",
    "under","until","up","very","was","we","were","what","when","where","which",
    "while","who","whom","why","will","with","you","your","yours","yourself",
    "yourselves","support", "media", "home", "contact", "menu",
    "login", "copyright", "cookie", "privacy"
}

def dump_stats():
    with open("stats.json", "w") as f:
        json.dump(
            {
                "unique_urls": len(seen_urls),
                "longest_page": longest_page,
                "word_freq": word_freq.most_common(200),
                "subdomains": subdomain_counts,
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

    # normalize URL (remove fragment)
    clean_url, _ = urldefrag(raw.url or url)
    seen_urls.add(clean_url)

    # parse HTML + analytics
    content_type = raw.headers.get("Content-Type", "")
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

            num_words = len(filtered)
            word_freq.update(filtered)

            if num_words > longest_page["word_count"]:
                longest_page["word_count"] = num_words
                longest_page["url"] = clean_url

            # subdomain counts under uci.edu
            parsed = urlparse(clean_url)
            host = (parsed.hostname or "").lower()
            if host.endswith(".uci.edu"):
                subdomain_counts[host] = subdomain_counts.get(host, 0) + 1

    links = extract_next_links(url, resp)
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
