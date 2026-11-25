"""Tushare's public documentation inventory and reviewed collection profiles."""

from __future__ import annotations

import json
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlsplit


SOURCE = "https://tushare.pro/document/2"
CATALOG_DIR = Path(__file__).resolve().parents[1] / "catalog"


def load_profiles() -> dict[str, dict]:
    """Return collection profiles. Public documentation hints are not runnable plans."""
    payload = json.loads((CATALOG_DIR / "profiles.json").read_text(encoding="utf-8"))
    return {profile["id"]: profile for profile in payload["profiles"]}


def catalog_snapshot(profiles: dict[str, dict] | None = None) -> dict:
    """Keep the official hierarchy independent from the registered ingestion profiles."""
    snapshot = json.loads((CATALOG_DIR / "tushare.json").read_text(encoding="utf-8"))
    if profiles is None:
        profiles = load_profiles()

    profiles_by_doc: dict[int, list[str]] = {}
    for name, profile in profiles.items():
        link = urlsplit(profile.get("doc_url") or profile.get("source_url") or "")
        doc = parse_qs(link.query).get("doc_id", [""])[0]
        if link.hostname == "tushare.pro" and doc.isdecimal():
            profiles_by_doc.setdefault(int(doc), []).append(name)

    counts = {name: {"total": 0, "configured": 0, "pending_review": 0, "other": 0}
              for name in snapshot["categories"]}
    tree: list[dict] = []
    branches: dict[tuple[str, ...], dict] = {}
    for entry in snapshot["entries"]:
        linked = list(dict.fromkeys(name for name in [*entry.get("profile_ids", []), *profiles_by_doc.get(entry["doc_id"], [])]
                                    if name in profiles))
        entry["profile_ids"] = linked
        if linked and not entry.get("output_fields"):
            entry["output_fields"] = profiles[linked[0]].get("fields", [])
        mode = entry["mode"]
        if linked and mode == "pending_review":
            mode = profiles[linked[0]].get("mode", mode)
            entry["mode"] = mode
        configured = any(profiles[name].get("api_name") and profiles[name].get("mode") in {"history", "snapshot"}
                         and profiles[name].get("request", {}).get("strategy") != "unsupported" for name in linked)
        status = "configured" if configured else mode
        entry["collection_status"] = status
        category_counts = counts[entry["category"]]
        category_counts["total"] += 1
        bucket = status if status in {"configured", "pending_review"} else "other"
        category_counts[bucket] += 1

        children = tree
        for index, title in enumerate(entry["path"][:-1], 1):
            path = tuple(entry["path"][:index])
            branch = branches.get(path)
            if branch is None:
                branch = {"title": title, "children": [], "entry_count": 0}
                branches[path] = branch
                children.append(branch)
            branch["entry_count"] += 1
            children = branch["children"]
        children.append({"title": entry["title"], "doc_id": entry["doc_id"]})

    snapshot["configured_count"] = sum(item["configured"] for item in counts.values())
    snapshot["coverage"] = counts
    snapshot["tree"] = tree
    return snapshot


class _CatalogTree(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.sidebar = False
        self.levels: list[dict] = []
        self.title_parts: list[str] | None = None
        self.entries: list[dict] = []

    def handle_starttag(self, tag: str, attributes: list[tuple[str, str | None]]) -> None:
        attrs = dict(attributes)
        if tag == "nav" and "sidebar" in (attrs.get("class") or "").split():
            self.sidebar = True
        if not self.sidebar:
            return
        if tag == "li":
            if self.levels:
                self.levels[-1]["has_children"] = True
            self.levels.append({"has_children": False})
        if tag == "a" and self.levels:
            path = urlsplit(urljoin(SOURCE, attrs.get("href") or ""))
            doc_id = parse_qs(path.query).get("doc_id", [""])[0]
            if (path.scheme, path.netloc, path.path) == ("https", "tushare.pro", "/document/2") and doc_id.isdecimal():
                self.levels[-1]["doc_id"] = int(doc_id)
                self.title_parts = []

    def handle_data(self, data: str) -> None:
        if self.title_parts is not None:
            self.title_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if not self.sidebar:
            return
        if tag == "a" and self.title_parts is not None and self.levels:
            self.levels[-1]["title"] = " ".join("".join(self.title_parts).split())
            self.title_parts = None
        if tag == "li" and self.levels:
            entry = self.levels.pop()
            if not entry["has_children"] and "doc_id" in entry and "title" in entry:
                path = [ancestor["title"] for ancestor in self.levels if "title" in ancestor]
                if path:
                    self.entries.append({
                        "doc_id": entry["doc_id"],
                        "title": entry["title"],
                        "category": path[0],
                        "path": [*path, entry["title"]],
                        "document_url": f"{SOURCE}?doc_id={entry['doc_id']}",
                    })
        if tag == "nav":
            self.sidebar = False


def discover_catalog(html: str) -> dict:
    """Parse the official sidebar, preserving occurrences and their hierarchy."""
    tree = _CatalogTree()
    tree.feed(html)
    tree.close()
    if not tree.entries:
        raise ValueError("No public data documentation entries found in the Tushare sidebar")
    return {
        "source": SOURCE,
        "entry_count": len(tree.entries),
        "categories": dict(Counter(entry["category"] for entry in tree.entries)),
        "entries": tree.entries,
    }
