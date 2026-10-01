"""Command line entry point: `python -m rih --year 2026`."""

from __future__ import annotations

import argparse
import json
import sys
import tomllib
from pathlib import Path

from .feed import fetch_feed, parse_feed
from .grouping import Group, Overrides, group_episodes

DEFAULT_OVERRIDES = Path(__file__).resolve().parent.parent / "overrides.toml"
DEFAULT_LISTENED = Path(__file__).resolve().parent.parent / "listened.toml"

_KIND_LABEL = {
    "series": "Series",
    "standalone": "Standalone episode",
    "bonus-series": "Members' mini-series",
    "bonus": "Members' bonus (standalone)",
}


def _filter_year(groups: list[Group], year: int | None) -> list[Group]:
    if year is None:
        return groups
    out = []
    for g in groups:
        main = [e for e in g.main if e.published.year == year]
        bonus = [e for e in g.bonus if e.published.year == year]
        if main or bonus:
            out.append(Group(g.kind, g.name, main=main, bonus=bonus))
    return out


def _norm(text: str) -> str:
    return text.lower().replace("\u2019", "'")


def drop_listened(groups: list[Group], path: Path) -> list[Group]:
    if not path.exists():
        return groups
    listened = [_norm(n) for n in tomllib.loads(path.read_text()).get("listened", [])]
    return [g for g in groups if not any(n in _norm(g.name) for n in listened)]


def _ep_line(e, tag: str) -> str:
    num = f"#{e.number} " if e.number else ""
    return f"- {e.published:%a %d %b} · {tag} · {num}{e.title}"


def render_markdown(groups: list[Group], year: int | None) -> str:
    n_eps = sum(len(g.main) + len(g.bonus) for g in groups)
    n_series = sum(g.kind in ("series", "bonus-series") for g in groups)
    lines = [
        f"# The Rest Is History Club{f' — {year}' if year else ''}",
        "",
        f"{n_eps} episodes in {len(groups)} groups ({n_series} series).",
        "",
    ]
    for g in groups:
        counts = []
        if g.main:
            counts.append(f"{len(g.main)} main")
        if g.bonus:
            counts.append(f"{len(g.bonus)} bonus")
        lines.append(f"## {g.name}")
        lines.append(f"*{_KIND_LABEL[g.kind]} · {', '.join(counts)} · from {g.start:%d %b %Y}*")
        lines.append("")
        for e in sorted(g.main, key=lambda e: (e.part or 0, e.published)):
            lines.append(_ep_line(e, "main "))
        for e in g.bonus:
            lines.append(_ep_line(e, "bonus"))
        lines.append("")
    return "\n".join(lines)


def render_json(groups: list[Group]) -> str:
    def ep(e, role):
        return {
            "role": role,
            "title": e.title,
            "number": e.number,
            "part": e.part,
            "published": e.published.isoformat(),
            "type": e.episode_type,
            "audio_url": e.audio_url,
            "guid": e.guid,
        }

    return json.dumps(
        [
            {
                "name": g.name,
                "kind": g.kind,
                "episodes": [ep(e, "main") for e in g.main] + [ep(e, "bonus") for e in g.bonus],
            }
            for g in groups
        ],
        indent=2,
        ensure_ascii=False,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--year", type=int, help="only show episodes published in this year")
    p.add_argument("--feed-file", type=Path, help="read the RSS from a file instead of fetching")
    p.add_argument("--save-feed", type=Path, help="also save the fetched RSS to this file")
    p.add_argument("--overrides", type=Path, default=DEFAULT_OVERRIDES)
    p.add_argument(
        "--unlistened", action="store_true", help="hide groups listed in listened.toml"
    )
    p.add_argument("--listened", type=Path, default=DEFAULT_LISTENED)
    p.add_argument("--format", choices=["md", "json"], default="md")
    p.add_argument("-o", "--output", type=Path, help="write to file instead of stdout")
    p.add_argument(
        "--explain", action="store_true", help="print how each bonus was placed (to stderr)"
    )
    args = p.parse_args(argv)

    xml_text = args.feed_file.read_text() if args.feed_file else fetch_feed()
    if args.save_feed:
        args.save_feed.write_text(xml_text)

    groups, decisions = group_episodes(parse_feed(xml_text), Overrides.load(args.overrides))
    if args.explain:
        for b, g, score in decisions:
            if args.year and b.published.year != args.year:
                continue
            where = g.name if g else "-- standalone --"
            how = "override" if score is None else f"{score:.3f}"
            print(f"{b.published:%Y-%m-%d} [{how}] {b.title}  ->  {where}", file=sys.stderr)

    groups = _filter_year(groups, args.year)
    if args.unlistened:
        groups = drop_listened(groups, args.listened)
    out = render_markdown(groups, args.year) if args.format == "md" else render_json(groups)
    if args.output:
        args.output.write_text(out + "\n")
    else:
        print(out)
    return 0
