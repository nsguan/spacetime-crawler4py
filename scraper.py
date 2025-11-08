import re
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup

# domains we’re allowed to crawl
ALLOWED_DOMAINS = (
    "ics.uci.edu",
    "cs.uci.edu",
    "informatics.uci.edu",
    "stat.uci.edu",
)

# fill this with the trap words / patterns you got from Discord
TRAP_WORDS = [
    "format=pdf",
    "action=download",
    "share=",
    "sort=",
    "view=all",
    "ngs.ics",
    "wics.ics",
    "event",
]

def scraper(url, resp):
        # Skip if we didn’t actually get a page
    if resp.status != 200 or resp.raw_response is None:
        return []

    # Some cache error codes (6xx etc.) – just bail
    if resp.status >= 600:
        return []

    links = extract_next_links(url, resp)
    # Only keep links that pass our domain/extension/trap filters
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    links = []

    raw = resp.raw_response
    if raw is None:
        return links

    # Only process HTML pages
    content_type = raw.headers.get("Content-Type", "")
    if "text/html" not in content_type:
        return links

    try:
        soup = BeautifulSoup(raw.content, "lxml")  # or "html.parser"
    except Exception:
        return links

    # Remove script/style so they don’t clutter text later if you do analytics
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()

    base_url = raw.url or url

    for a in soup.find_all("a", href=True):
        href = a.get("href")

        # Build absolute URL (handles relative links)
        absolute = urljoin(base_url, href)

        # Remove fragment (#section)
        absolute, _ = urldefrag(absolute)

        # You can skip mailto:, javascript:, etc. here too if you want
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

        # File extensions to skip (your original regex)
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
