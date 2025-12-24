import datetime
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional

import requests


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def download_file(
    url: str,
    out_path: Path,
    timeout: int = 60,
    headers: Optional[dict] = None,
) -> Dict[str, Any]:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    headers = headers or {
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
    }

    with requests.Session() as s:
        r = s.get(url, headers=headers, allow_redirects=True, stream=True, timeout=timeout)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    downloaded_at_utc = datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    return {
        "bytes": out_path.stat().st_size,
        "last_modified": r.headers.get("Last-Modified"),
        "etag": r.headers.get("ETag"),
        "url": url,
        "final_url": r.url,
        "path": str(out_path),
        "downloaded_at_utc": downloaded_at_utc,
        "content_type": r.headers.get("Content-Type"),
        "content_length": r.headers.get("Content-Length"),
    }


def is_new_by_hash(prev_hash: Optional[str], new_hash: str) -> bool:
    return prev_hash != new_hash
