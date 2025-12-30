from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Optional, Tuple, List, Dict

import requests

BASE_URL = "https://www.statistics.gr"


# ----------------------------
# Helpers: URLs + HTTP
# ----------------------------

def _abs_url(href: str) -> str:
    """Convert a relative ELSTAT link to an absolute URL."""
    if not href:
        return href
    href = href.strip()

    # ELSTAT sometimes returns absolute URLs with :443; that's valid, but we normalize.
    href = href.replace("https://www.statistics.gr:443", "https://www.statistics.gr")

    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href.lstrip("/")


def _get_html(url: str, headers: Dict[str, str]) -> str:
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def _parse_period(period: str) -> Tuple[int, int]:
    """
    Convert "2025-M09" -> (2025, 9)
            "2025-Q3"  -> (2025, 3)
    Used for comparing periods and selecting the latest.
    """
    m = re.fullmatch(r"(\d{4})-M(\d{2})", period)
    if m:
        return int(m.group(1)), int(m.group(2))

    q = re.fullmatch(r"(\d{4})-Q([1-4])", period)
    if q:
        return int(q.group(1)), int(q.group(2))

    raise ValueError(f"Unrecognized period format: {period}")


# ----------------------------
# HTML parsing (context-aware)
# ----------------------------

@dataclass
class LinkItem:
    href: str
    anchor_text: str
    context_text: str


class _ContextLinkParser(HTMLParser):
    """
    Collects links with both:
      - anchor_text: visible text inside <a>
      - context_text: rolling window of nearby text (helps when title isn't in <a>)
    """
    def __init__(self, context_window: int = 40):
        super().__init__()
        self.links: List[LinkItem] = []

        self._in_a = False
        self._href: str = ""
        self._a_text_parts: List[str] = []

        self._recent_text: List[str] = []
        self._context_window = context_window

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            self._in_a = True
            self._href = dict(attrs).get("href", "") or ""
            self._a_text_parts = []

    def handle_data(self, data):
        txt = (data or "").strip()
        if not txt:
            return

        # track rolling context text
        self._recent_text.append(txt)
        if len(self._recent_text) > self._context_window:
            self._recent_text.pop(0)

        if self._in_a:
            self._a_text_parts.append(txt)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._in_a:
            anchor = " ".join(self._a_text_parts).strip()
            ctx = " ".join(self._recent_text).strip()
            href = self._href.strip()

            if href:
                self.links.append(LinkItem(href=href, anchor_text=anchor, context_text=ctx))

            self._in_a = False
            self._href = ""
            self._a_text_parts = []


def _norm_text(s: str) -> str:
    """Normalize text for matching."""
    s = (s or "").strip().lower()
    # collapse whitespace
    s = re.sub(r"\s+", " ", s)
    return s


# ----------------------------
# Public API
# ----------------------------

def get_latest_publication_url(
    publication_code: str,
    locale: str = "en",
    frequency: str = "monthly",  # "monthly" or "quarterly"
    headers: Optional[Dict[str, str]] = None,
) -> str:
    """
    Returns the latest publication page URL for a publication code.
    Examples:
      monthly:   .../publication/SOP03/2025-M09
      quarterly: .../publication/DKT66/2025-Q3

    It fetches the index page: .../publication/<CODE>/-
    then finds ALL matching periods and returns the max (latest).
    """
    headers = headers or {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}

    index_url = f"{BASE_URL}/{locale}/statistics/-/publication/{publication_code}/-"
    html = _get_html(index_url, headers=headers)

    if frequency == "monthly":
        # capture 2025-M09 etc.
        pat = rf"/{re.escape(locale)}/statistics/-/publication/{re.escape(publication_code)}/(\d{{4}}-M\d{{2}})"
        periods = re.findall(pat, html)

        # fallback if locale isn't explicitly in href
        if not periods:
            pat2 = rf"/statistics/-/publication/{re.escape(publication_code)}/(\d{{4}}-M\d{{2}})"
            periods = re.findall(pat2, html)

    elif frequency == "quarterly":
        # capture 2025-Q3 etc.
        pat = rf"/{re.escape(locale)}/statistics/-/publication/{re.escape(publication_code)}/(\d{{4}}-Q[1-4])"
        periods = re.findall(pat, html)

        if not periods:
            pat2 = rf"/statistics/-/publication/{re.escape(publication_code)}/(\d{{4}}-Q[1-4])"
            periods = re.findall(pat2, html)
    else:
        raise ValueError("frequency must be 'monthly' or 'quarterly'")

    if not periods:
        raise RuntimeError(
            f"Could not find any {frequency} periods for publication {publication_code} on {index_url}"
        )

    latest_period = max(periods, key=_parse_period)
    return f"{BASE_URL}/{locale}/statistics/-/publication/{publication_code}/{latest_period}"


def get_download_url_by_title(
    publication_url: str,
    target_title: str,
    headers: Optional[Dict[str, str]] = None,
    require_downloadresources: bool = True,
) -> str:
    """
    Given a publication page URL (e.g. .../SOP03/2025-M09 or .../DKT66/2025-Q3),
    finds the downloadable file link for the given target title.

    It matches the title against:
      - the <a> visible text, OR
      - nearby surrounding text (context window)

    If require_downloadresources=True (default), it prefers links that contain
    'downloadResources' in the URL to avoid grabbing navigation links.
    """
    headers = headers or {"User-Agent": "Mozilla/5.0", "Accept": "text/html,*/*"}
    html = _get_html(publication_url, headers=headers)

    parser = _ContextLinkParser()
    parser.feed(html)

    target = _norm_text(target_title)

    # 1) Strong match: title appears in anchor or context
    candidates: List[LinkItem] = []
    for item in parser.links:
        combined = _norm_text(f"{item.anchor_text} {item.context_text}")
        if target in combined:
            candidates.append(item)

    if not candidates:
        # Helpful error: show a few link texts for debugging
        sample = []
        for item in parser.links[:12]:
            sample.append(f"- text='{item.anchor_text}' href='{item.href}'")
        raise RuntimeError(
            f"Could not find a link matching title:\n  {target_title}\n"
            f"on page:\n  {publication_url}\n"
            f"Sample links seen:\n" + "\n".join(sample)
        )

    # 2) Prefer downloadResources links (actual file downloads)
    if require_downloadresources:
        dl = [c for c in candidates if "downloadresources" in c.href.lower()]
        if dl:
            return _abs_url(dl[0].href)

    # 3) Otherwise return the first match
    return _abs_url(candidates[0].href)
