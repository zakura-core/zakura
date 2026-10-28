#!/usr/bin/env python3
"""Validate the legacy Pages redirect without fetching or running an installer."""

import argparse
from html.parser import HTMLParser
from pathlib import Path


class DownloadRedirect(HTMLParser):
    """Collect the browser redirect and the canonical and fallback links."""

    def __init__(self):
        super().__init__()
        self.refreshes = []
        self.canonicals = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta" and attrs.get("http-equiv", "").lower() == "refresh":
            self.refreshes.append(attrs.get("content"))
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonicals.append(attrs.get("href"))
        if tag == "a":
            self.links.append(attrs.get("href"))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("page", type=Path)
    args = parser.parse_args(argv)
    source = args.page.read_text(encoding="utf-8")
    page = DownloadRedirect()
    page.feed(source)
    destination = "https://zakura.com/download/"
    if (
        page.refreshes != [f"0; url={destination}"]
        or page.canonicals != [destination]
        or destination not in page.links
    ):
        parser.error("redirect, canonical URL, and fallback link must use the official download page")
    if "install-zakura.sh" in source or "curl " in source:
        parser.error("legacy Pages must not duplicate installer commands or release asset URLs")
    print(f"Validated legacy Pages redirect to {destination}")


if __name__ == "__main__":
    main()
