from rih.feed import clean_description, parse_feed
from rih.grouping import Overrides, group_episodes


def item(title, date, kind="full", number=None, desc=""):
    num = f"<itunes:episode>{number}</itunes:episode>" if number else ""
    return f"""<item><title>{title}</title><guid>{title}</guid>
      <pubDate>{date} 2026 12:00:00 -0000</pubDate>
      <itunes:episodeType>{kind}</itunes:episodeType>{num}
      <description>{desc}</description></item>"""


def feed(*items):
    return f"""<?xml version="1.0"?><rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd">
      <channel>{''.join(items)}</channel></rss>"""


XML = feed(
    item("The Terror: Marat (Part 1)", "Sun, 20 Sep", number=707,
         desc="Robespierre, Marat and the guillotine in revolutionary Paris."),
    item("The Terror: Robespierre Falls (Part 2)", "Sun, 20 Sep", number=708,
         desc="Robespierre and the Jacobins meet the guillotine."),
    item("USA: The Banner (Part 1)", "Sun, 07 Jun", number=677, desc="Anthems."),
    item("Britain: God Save the King (Part 2)", "Sun, 07 Jun", number=678, desc="Anthems."),
    item("The Man Who Painted Marat", "Tue, 22 Sep", kind="bonus",
         desc="Jacques-Louis David painted Marat during the revolutionary Terror under Robespierre."),
    item("Satire with Ian Hislop", "Thu, 24 Sep", kind="bonus",
         desc="Gillray, Swift and cartoons."),
    item("Music: Jazz | History in Photos", "Tue, 07 Apr", kind="bonus", desc="Photos."),
    item("Fashion: Dior | History in Photos (FULL EPISODE)", "Tue, 14 Apr", kind="bonus",
         desc="Photos."),
)


def by_name(groups):
    return {g.name: g for g in groups}


def test_parts_chain_into_series_and_bonus_attaches():
    groups, _ = group_episodes(parse_feed(XML))
    terror = by_name(groups)["The Terror"]
    assert [e.part for e in terror.main] == [1, 2]
    assert [e.title for e in terror.bonus] == ["The Man Who Painted Marat"]


def test_unrelated_bonus_stays_standalone():
    groups, _ = group_episodes(parse_feed(XML))
    assert by_name(groups)["Satire with Ian Hislop"].kind == "bonus"


def test_parts_without_shared_prefix_grouped_and_renamed():
    groups, _ = group_episodes(parse_feed(XML), Overrides(names={"USA: The Banner": "Anthems"}))
    assert len(by_name(groups)["Anthems"].main) == 2


def test_bonus_strand_from_title_suffix():
    groups, _ = group_episodes(parse_feed(XML))
    photos = by_name(groups)["History in Photos"]
    assert photos.kind == "bonus-series" and len(photos.bonus) == 2


def test_assign_override():
    groups, _ = group_episodes(parse_feed(XML), Overrides(assign={"Hislop": "The Terror"}))
    assert len(by_name(groups)["The Terror"].bonus) == 2


def test_clean_description_strips_promo():
    raw = "<p>What happened?</p>*The Rest Is History LIVE* tickets now"
    assert clean_description(raw) == "What happened?"
