"""Group episodes into series: multi-part main series, bonus mini-series, and
member bonuses attached to the main series they follow up on.

Pipeline:
  1. Main series   - full episodes titled "... (Part N)" are chained into series.
                     Other full episodes become single-episode "standalone" groups.
  2. Bonus strands - bonus-only mini-series recognised from the title
                     ("... | History in Photos", "Greatest Paintings: ...") or from a
                     shared "mini series" sentence in the description.
  3. Attachment    - each remaining bonus is compared (TF-IDF cosine) with the main
                     groups released shortly before it; the best match above a
                     threshold claims it.
  4. Overrides     - overrides.toml can rename groups or force a bonus's placement.
"""

from __future__ import annotations

import math
import re
import tomllib
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from .feed import Episode

ATTACH_WINDOW = timedelta(days=24)
ATTACH_THRESHOLD = 0.05
# Weekly listener Q&As titled "RIHC: A, B, and C" cover several unrelated topics,
# so they only join a series on a strong match.
GRAB_BAG_THRESHOLD = 0.16
_GRAB_BAG_RE = re.compile(r", [^,]+,? and ", re.IGNORECASE)

# Title prefixes that label the feed rather than a series ("RIHC: ...").
_NON_SERIES_PREFIXES = {"rihc"}
_PART_SUFFIX_RE = re.compile(r"\((?:Part|Ep)\s+\d+\)", re.IGNORECASE)
_TITLE_NOISE_RE = re.compile(r"\(?\bFULL EPISODE\b\)?", re.IGNORECASE)

_STOPWORDS = set(
    """
    a about after again against all also an and any are as at be because been before
    being between both but by can could did do does during each few for from further
    had has have having he her here hers him his how i if in into is it its just more
    most no nor not now of off on once only or other our out over own same she should
    so some such than that the their them then there these they this those through to
    too under until up very was we were what when where which while who whom why will
    with would you your one two three four five six first new really ever forever
    tom dominic holland sandbrook join joined joins today today's week week's this
    episode episodes bonus special part discuss discussing delve deeper dive launch
    friend show history historian historical author rest club member members member's
    talk great greatest famous most world story stories time times came come become
    became make made like did happen happened did so us way well still
    """.split()
)


@dataclass
class Group:
    kind: str  # "series" | "standalone" | "bonus-series" | "bonus"
    name: str
    main: list[Episode] = field(default_factory=list)
    bonus: list[Episode] = field(default_factory=list)

    @property
    def episodes(self) -> list[Episode]:
        return sorted(self.main + self.bonus, key=lambda e: e.published)

    @property
    def start(self):
        return min(e.published for e in self.main + self.bonus)

    @property
    def main_end(self):
        return max(e.published for e in self.main)


@dataclass
class Overrides:
    names: dict[str, str] = field(default_factory=dict)  # title substring -> group name
    assign: dict[str, str] = field(default_factory=dict)  # bonus title substring -> group name / "standalone"

    @classmethod
    def load(cls, path: Path | None) -> "Overrides":
        if path is None or not path.exists():
            return cls()
        data = tomllib.loads(path.read_text())
        return cls(names=data.get("names", {}), assign=data.get("assign", {}))

    def name_for(self, episodes: list[Episode]) -> str | None:
        for needle, name in self.names.items():
            if any(needle.lower() in e.title.lower() for e in episodes):
                return name
        return None

    def assignment_for(self, episode: Episode) -> str | None:
        for needle, target in self.assign.items():
            if needle.lower() in episode.title.lower():
                return target
        return None


# --------------------------------------------------------------------------- text


def _tokens(text: str) -> list[str]:
    words = re.findall(r"[a-zà-ÿ]+", text.lower().replace("’", "'"))
    return [w for w in words if len(w) > 2 and w not in _STOPWORDS]


def _episode_text(e: Episode) -> str:
    # Title words count double: they're the most specific signal.
    return f"{e.title} {e.title} {e.description}"


class _Tfidf:
    def __init__(self, corpus: list[Episode]):
        df: Counter[str] = Counter()
        for e in corpus:
            df.update(set(_tokens(_episode_text(e))))
        n = len(corpus)
        self.idf = {w: math.log((1 + n) / (1 + c)) + 1 for w, c in df.items()}

    def vector(self, text: str) -> dict[str, float]:
        tf = Counter(_tokens(text))
        vec = {w: (1 + math.log(c)) * self.idf.get(w, 1.0) for w, c in tf.items()}
        norm = math.sqrt(sum(v * v for v in vec.values())) or 1.0
        return {w: v / norm for w, v in vec.items()}

    @staticmethod
    def cosine(a: dict[str, float], b: dict[str, float]) -> float:
        if len(a) > len(b):
            a, b = b, a
        return sum(v * b.get(w, 0.0) for w, v in a.items())


# --------------------------------------------------------------------- main series


def _common_prefix_name(episodes: list[Episode]) -> str | None:
    prefixes = {e.series_prefix for e in episodes}
    if len(prefixes) == 1 and None not in prefixes:
        return prefixes.pop()
    return None


def build_main_groups(episodes: list[Episode]) -> list[Group]:
    """Chain "(Part N)" full episodes into series; everything else stands alone."""
    full = [e for e in episodes if not e.is_bonus and e.episode_type != "trailer"]
    full.sort(key=lambda e: (e.published.date(), e.part or 0, e.number or 0))

    groups: list[Group] = []
    open_series: list[Group] = []  # series that may still receive a next part
    for e in full:
        part = e.part
        if part is None:
            groups.append(Group("standalone", e.title, main=[e]))
            continue
        target = None
        if part > 1:
            for g in reversed(open_series):
                last = g.main[-1]
                gap = e.published - last.published
                # Prefixes may change mid-series ("Custer vs. Crazy Horse" parts 1-4,
                # "Custer's Last Stand" parts 5-8), so only the part sequence matters.
                if last.part == part - 1 and gap <= timedelta(days=21):
                    target = g
                    break
        if target is None:
            target = Group("series", "", main=[])
            groups.append(target)
            open_series.append(target)
        target.main.append(e)

    for g in groups:
        if g.kind == "series" and len(g.main) == 1:
            g.kind = "standalone"
            g.name = g.main[0].title
        elif g.kind == "series":
            first = g.main[0]
            g.name = _common_prefix_name(g.main) or first.series_prefix or (
                _PART_SUFFIX_RE.sub("", first.title).strip()
            )
    return groups


# -------------------------------------------------------------------- bonus strands


def _strand_key(e: Episode) -> str | None:
    title = _TITLE_NOISE_RE.sub("", e.title).strip(" |")
    if " | " in title:
        return title.rsplit(" | ", 1)[1].strip()
    return None


def _prefix_key(e: Episode) -> str | None:
    prefix = e.series_prefix
    if prefix and prefix.lower() not in _NON_SERIES_PREFIXES:
        return prefix
    return None


def _title_tail_key(e: Episode) -> str | None:
    """'Dracula, by Bram Stoker' -> book club; 'The Trojan War, with Mary Beard' -> guest run."""
    title = _PART_SUFFIX_RE.sub("", _TITLE_NOISE_RE.sub("", e.title)).strip(" |")
    if re.search(r", by [A-Z]", title):
        return "Book Club"
    if m := re.search(r", with ([^,|]+)$", title):
        return f"with {m.group(1).strip()}"
    return None


def _mini_series_sentence(e: Episode) -> str | None:
    for sentence in re.split(r"(?<=[.?!])\s+", e.description):
        if re.search(r"\bmini[- ]?series\b|\bclub series\b", sentence, re.IGNORECASE):
            return sentence.strip()
    return None


def build_bonus_strands(bonuses: list[Episode]) -> tuple[list[Group], list[Episode]]:
    """Find bonus-only mini-series. Returns (strands, unclaimed bonuses)."""
    claimed: set[str] = set()
    strands: list[Group] = []

    for key_fn in (_strand_key, _prefix_key, _title_tail_key, _mini_series_sentence):
        buckets: dict[str, list[Episode]] = defaultdict(list)
        for e in bonuses:
            if e.guid not in claimed and (key := key_fn(e)):
                buckets[key].append(e)
        for key, eps in buckets.items():
            if len(eps) < 2:
                continue
            name = key if key_fn is not _mini_series_sentence else f"Mini-series: {eps[0].title}"
            strands.append(Group("bonus-series", name, bonus=eps))
            claimed.update(e.guid for e in eps)

    return strands, [e for e in bonuses if e.guid not in claimed]


# ---------------------------------------------------------------------- attachment


def attach_bonuses(
    main_groups: list[Group], bonuses: list[Episode], corpus: list[Episode]
) -> tuple[list[Group], list[tuple[Episode, Group, float]]]:
    """Attach bonuses to the main group they follow up on.

    Returns (leftover single-bonus groups, decisions) where decisions records the
    score for each attachment, useful for --explain.
    """
    tfidf = _Tfidf(corpus)
    group_vecs = {
        id(g): tfidf.vector(" ".join(_episode_text(e) for e in g.main)) for g in main_groups
    }
    leftovers: list[Group] = []
    decisions = []
    for b in bonuses:
        bvec = tfidf.vector(_episode_text(b))
        candidates = [
            g
            for g in main_groups
            if g.start <= b.published and b.published - g.main_end <= ATTACH_WINDOW
        ]
        scored = sorted(
            ((tfidf.cosine(bvec, group_vecs[id(g)]), g) for g in candidates),
            key=lambda t: t[0],
            reverse=True,
        )
        threshold = GRAB_BAG_THRESHOLD if _GRAB_BAG_RE.search(b.title) else ATTACH_THRESHOLD
        if scored and scored[0][0] >= threshold:
            score, g = scored[0]
            g.bonus.append(b)
            decisions.append((b, g, score))
        else:
            leftovers.append(Group("bonus", b.title, bonus=[b]))
            decisions.append((b, None, scored[0][0] if scored else 0.0))
    return leftovers, decisions


# ------------------------------------------------------------------------- driver


def group_episodes(
    episodes: list[Episode], overrides: Overrides | None = None
) -> tuple[list[Group], list]:
    overrides = overrides or Overrides()
    main_groups = build_main_groups(episodes)
    bonuses = [e for e in episodes if e.is_bonus]

    for g in main_groups:
        if name := overrides.name_for(g.main):
            g.name = name

    # Forced placements first, then automatic strands / attachment for the rest.
    forced: dict[str, list[Episode]] = defaultdict(list)
    auto: list[Episode] = []
    for b in bonuses:
        if target := overrides.assignment_for(b):
            forced[target].append(b)
        else:
            auto.append(b)

    strands, unclaimed = build_bonus_strands(auto)
    for g in strands:
        if name := overrides.name_for(g.bonus):
            g.name = name
    leftovers, decisions = attach_bonuses(main_groups, unclaimed, episodes)

    groups = main_groups + strands + leftovers
    by_name = {g.name: g for g in groups}
    for target, eps in forced.items():
        if target.lower() == "standalone":
            groups.extend(Group("bonus", e.title, bonus=[e]) for e in eps)
        elif target in by_name:
            by_name[target].bonus.extend(eps)
        else:
            new = Group("bonus-series", target, bonus=eps)
            groups.append(new)
            by_name[target] = new
        decisions.extend((e, by_name.get(target), None) for e in eps)

    # A standalone full episode with no bonuses is just a standalone episode;
    # one that picked up bonuses reads better as a (small) series.
    for g in groups:
        g.bonus.sort(key=lambda e: e.published)
        if g.kind == "standalone" and g.bonus:
            g.kind = "series"

    groups.sort(key=lambda g: g.start)
    return groups, decisions
