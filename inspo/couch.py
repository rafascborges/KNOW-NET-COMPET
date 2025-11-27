import json
from typing import Iterable, List

import requests
from requests.adapters import HTTPAdapter, Retry

DEFAULT_ALLOWED_METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"]


def make_session(retries: int = 5, backoff: float = 0.5):
    """Create a configured requests session with retry/backoff."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=DEFAULT_ALLOWED_METHODS,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def ensure_db_exists(session: requests.Session, db_base: str, timeout: int = 30):
    """Ensure a CouchDB database exists, creating it if missing."""
    base = db_base.rstrip("/")
    resp = session.get(base, timeout=timeout)
    if resp.status_code == 404:
        create = session.put(base, timeout=timeout)
        create.raise_for_status()
        return
    resp.raise_for_status()


def post_bulk(
    session: requests.Session,
    db_base: str,
    docs: List[dict],
    new_edits: bool | None = None,
    timeout: int = 180,
):
    """Send documents to CouchDB via _bulk_docs."""
    payload: dict = {"docs": docs}
    if new_edits is not None:
        payload["new_edits"] = new_edits
    resp = session.post(db_base.rstrip("/") + "/_bulk_docs", json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def iter_docs(
    session: requests.Session,
    db_base: str,
    batch_size: int = 1000,
    timeout: int = 180,
    include_design_docs: bool = False,
):
    """Yield lists of full documents from CouchDB using _all_docs pagination."""
    base = db_base.rstrip("/")
    url = base + "/_all_docs"
    startkey = None

    while True:
        params = {"include_docs": "true", "limit": batch_size}
        if startkey is not None:
            params["startkey"] = json.dumps(startkey)
            params["skip"] = 1  # avoid repeating the last key

        resp = session.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        rows = resp.json().get("rows", [])
        if not rows:
            break

        docs = []
        for row in rows:
            doc = row.get("doc")
            if not doc:
                continue
            if not include_design_docs and row.get("id", "").startswith("_design/"):
                continue
            docs.append(doc)

        if docs:
            yield docs

        if len(rows) < batch_size:
            break
        startkey = rows[-1]["key"]

