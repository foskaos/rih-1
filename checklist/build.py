"""Build the listening-log checklist page: python checklist/build.py feed.xml out.html [years...]"""

import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rih.feed import parse_feed  # noqa: E402
from rih.grouping import Overrides, group_episodes  # noqa: E402


def catalog(xml_text: str, years: set[int]) -> list[dict]:
    groups, _ = group_episodes(parse_feed(xml_text), Overrides.load(ROOT / "overrides.toml"))
    out = []
    for g in groups:
        anchor = min(e.published for e in (g.main or g.bonus))
        if anchor.year not in years:
            continue
        slug = re.sub(r"[^a-z0-9]+", "-", g.name.lower().replace("’", "").replace("'", "")).strip("-")[:50]
        eps = [
            {"id": hashlib.sha1(e.guid.encode()).hexdigest()[:10], "t": e.title,
             "d": e.published.strftime("%Y-%m-%d"), "r": role, "n": e.number}
            for role, lst in (("main", sorted(g.main, key=lambda e: (e.part or 0, e.published))), ("bonus", g.bonus))
            for e in lst
        ]
        out.append({"id": f"{anchor:%Y%m%d}-{slug}", "name": g.name, "kind": g.kind,
                    "year": anchor.year, "date": anchor.strftime("%Y-%m-%d"), "eps": eps})
    return out


if __name__ == "__main__":
    feed, dest, *ys = sys.argv[1:]
    data = json.dumps(catalog(Path(feed).read_text(), {int(y) for y in ys} or {2024, 2025, 2026}),
                      ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    page = (Path(__file__).parent / "template.html").read_text().replace("__CATALOG__", data)
    Path(dest).write_text(page)
