"""Fetch and parse the Rest Is History Club RSS feed."""

from __future__ import annotations

import html
import os
import re
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from xml.etree import ElementTree

# Personal subscription feed. Override with the RIH_FEED_URL environment variable.
DEFAULT_FEED_URL = (
    "https://therestishistory.supportingcast.fm/content/"
    "eyJ0IjoicCIsImMiOiIxNDc3IiwidSI6Ijg2MzM0OCIsImQiOiIxNjM0OTQwODcyIiwiayI6MjY3fXwz"
    "MjIzYTc1NWIxYzRiMzAxYTg4ZTU3YTgyZTg2MDRkZGMzMTk5MmNkOWNlZDM3ZDM3YmY4YzliYzUzZDYyMWMw.rss"
)

ITUNES = "{http://www.itunes.com/dtds/podcast-1.0.dtd}"

# Everything after one of these markers in a description is promo/credits, not content.
_BOILERPLATE_MARKERS = (
    "_______",
    "*The Rest Is History",
    "Become a member",
    "As a member",
    "Win a signed",
    "Sign up to our",
    "Getty Images",
    "FUTURE EPISODES",
    "NEXT WEEK",
    "Tickets available",
    "\n--",
)

_PART_RE = re.compile(r"\(Part\s+(\d+)\)", re.IGNORECASE)


@dataclass(frozen=True)
class Episode:
    guid: str
    title: str
    published: datetime
    episode_type: str  # "full", "bonus" or "trailer"
    number: int | None
    description: str  # plain-text content summary, promo stripped
    audio_url: str | None
    duration: str | None

    @property
    def is_bonus(self) -> bool:
        return self.episode_type == "bonus"

    @property
    def part(self) -> int | None:
        m = _PART_RE.search(self.title)
        return int(m.group(1)) if m else None

    @property
    def series_prefix(self) -> str | None:
        """'The Terror' for 'The Terror: Killing God (Part 5)'."""
        if ":" not in self.title:
            return None
        return self.title.split(":", 1)[0].strip()


def feed_url() -> str:
    return os.environ.get("RIH_FEED_URL", DEFAULT_FEED_URL)


def fetch_feed(url: str | None = None, timeout: float = 60) -> str:
    req = urllib.request.Request(url or feed_url(), headers={"User-Agent": "rih/0.2"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def clean_description(raw: str | None) -> str:
    text = html.unescape(raw or "")
    text = re.sub(r"<[^>]+>", " ", text)
    for marker in _BOILERPLATE_MARKERS:
        idx = text.find(marker)
        if idx != -1:
            text = text[:idx]
    text = text.replace("\xa0", " ").replace("⁠", "")
    return re.sub(r"\s+", " ", text).strip()


def parse_feed(xml_text: str) -> list[Episode]:
    root = ElementTree.fromstring(xml_text)
    episodes = []
    for item in root.iterfind("./channel/item"):
        number = item.findtext(f"{ITUNES}episode")
        enclosure = item.find("enclosure")
        title = re.sub(r"\s+", " ", item.findtext("title") or "").strip()
        episodes.append(
            Episode(
                guid=(item.findtext("guid") or title).strip(),
                title=title,
                published=parsedate_to_datetime(item.findtext("pubDate")),
                episode_type=(item.findtext(f"{ITUNES}episodeType") or "full").strip(),
                number=int(number) if number and number.strip().isdigit() else None,
                description=clean_description(item.findtext("description")),
                audio_url=enclosure.get("url") if enclosure is not None else None,
                duration=item.findtext(f"{ITUNES}duration"),
            )
        )
    episodes.sort(key=lambda e: (e.published, e.number or 0))
    return episodes
