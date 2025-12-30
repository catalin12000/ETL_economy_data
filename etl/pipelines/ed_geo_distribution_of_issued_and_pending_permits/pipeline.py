from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, List
from html.parser import HTMLParser

import requests

from etl.core.download import download_file, sha256_file, is_new_by_hash


class _LinkParser(HTMLParser):
    """Collect <a href="...">TEXT</a> links."""
    def __init__(self) -> None:
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._in_a = False
        self._href = ""
        self._text_parts: List[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            self._in_a = True
            self._href = dict(attrs).get("href", "") or ""
            self._text_parts = []

    def handle_data(self, data):
        if self._in_a:
            t = (data or "").strip()
            if t:
                self._text_parts.append(t)

    def handle_endtag(self, tag):
        if tag.lower() == "a" and self._in_a:
            text = " ".join(self._text_parts).strip()
            href = self._href.strip()
            if href and text:
                self.links.append((href, text))
            self._in_a = False
            self._href = ""
            self._text_parts = []


# Greek month name -> month number
_MONTHS_GR = {
    "ιανουάριος": 1, "ιανουαριος": 1,
    "φεβρουάριος": 2, "φεβρουαριος": 2,
    "μάρτιος": 3, "μαρτιος": 3,
    "απρίλιος": 4, "απριλιος": 4,
    "μάιος": 5, "μαιος": 5,
    "ιούνιος": 6, "ιουνιος": 6,
    "ιούλιος": 7, "ιουλιος": 7,
    "αύγουστος": 8, "αυγουστος": 8,
    "σεπτέμβριος": 9, "σεπτεμβριος": 9,
    "οκτώβριος": 10, "οκτωβριος": 10,
    "νοέμβριος": 11, "νοεμβριος": 11,
    "δεκέμβριος": 12, "δεκεμβριος": 12,
}


def _get_html(url: str, headers: Dict[str, str]) -> str:
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.text


def _abs_url(href: str) -> str:
    if href.startswith("http://") or href.startswith("https://"):
        return href
    # migration.gov.gr uses absolute site root links
    if href.startswith("/"):
        return "https://migration.gov.gr" + href
    return "https://migration.gov.gr/" + href.lstrip("/")


def _parse_month_year_from_title(title: str) -> Optional[Tuple[int, int]]:
    """
    Extract (year, month) from strings like:
    'Νοέμβριος 2025 – Νόμιμη Μετανάστευση | Παράρτημα Β'
    """
    t = " ".join(title.split()).strip().lower()

    # capture "<monthname> <year>"
    m = re.search(r"([α-ωάέήίόύώϊΐϋΰ]+)\s+(\d{4})", t)
    if not m:
        return None

    month_name = m.group(1)
    year = int(m.group(2))

    month = _MONTHS_GR.get(month_name)
    if not month:
        return None

    return year, month


def resolve_latest_migration_appendix_b_pdf_url(
    index_url: str,
    must_contain_text: str,
    headers: Dict[str, str],
) -> Tuple[str, str]:
    """
    Returns (pdf_url, period_string) for the latest link matching the criteria.
    period_string example: '2025-11'
    """
    html = _get_html(index_url, headers=headers)
    p = _LinkParser()
    p.feed(html)

    needle = must_contain_text.lower()

    candidates: List[Tuple[int, int, str, str]] = []  # (year, month, href, text)
    for href, text in p.links:
        if needle in text.lower():
            ym = _parse_month_year_from_title(text)
            if ym:
                y, mth = ym
                candidates.append((y, mth, href, text))

    if not candidates:
        # give useful debugging info
        sample = "\n".join([f"- {txt}" for _, txt in p.links[:20]])
        raise RuntimeError(
            f"Could not find any links containing '{must_contain_text}' on {index_url}.\n"
            f"Sample links seen:\n{sample}"
        )

    # latest by (year, month)
    y, mth, href, _text = max(candidates, key=lambda x: (x[0], x[1]))
    pdf_url = _abs_url(href)
    period = f"{y}-{mth:02d}"
    return pdf_url, period


class Pipeline:
    pipeline_id = "ed_geo_distribution_of_issued_and_pending_permits"
    display_name = "Geo distribution of issued and pending permits (Migration.gov.gr)"

    INDEX_URL = "https://migration.gov.gr/en/statistika/"

    # This is the stable part we match in the link text
    LINK_TEXT_MATCH = "Νόμιμη Μετανάστευση | Παράρτημα Β"

    def run(self, state: Dict[str, Any]) -> Dict[str, Any]:
        out_dir = Path("data/downloads") / self.pipeline_id
        out_dir.mkdir(parents=True, exist_ok=True)

        pdf_path = out_dir / "migration_appendix_b.pdf"  # keep extension .pdf

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}

        # 1) Resolve latest PDF URL by scanning the index page
        pdf_url, period = resolve_latest_migration_appendix_b_pdf_url(
            index_url=self.INDEX_URL,
            must_contain_text=self.LINK_TEXT_MATCH,
            headers=headers,
        )

        # 2) Download
        meta = download_file(pdf_url, pdf_path, headers=headers)
        file_hash = sha256_file(pdf_path)

        # 3) State
        new_state = dict(state)
        new_state.update({
            "source_page": self.INDEX_URL,
            "resolved_pdf_url": pdf_url,
            "latest_period_seen": period,
            "file_sha256": file_hash,
            "downloaded_filename": pdf_path.name,
            "last_download_path": str(pdf_path),
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
            "content_length": meta.get("content_length"),
            "final_url": meta.get("final_url"),
            "downloaded_at_utc": meta.get("downloaded_at_utc"),
        })

        if not is_new_by_hash(state.get("file_sha256"), file_hash):
            return {"status": "skipped", "message": "No new file detected (same file SHA256).", "state": new_state}

        return {"status": "delivered", "message": f"Downloaded latest Appendix B PDF ({period}) to {pdf_path}", "state": new_state}
