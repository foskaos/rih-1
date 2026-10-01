# rest-is-hist

Reads my **Rest Is History Club** subscription feed and groups episodes into series:

- **Main series**: full episodes titled `... (Part N)` are chained together.
- **Members' mini-series**: bonus-only runs such as *History in Photos*, *Greatest Paintings*
  or *History of the World Cup* (found from title patterns or a shared "mini series" sentence).
- **Members' bonuses**: each remaining bonus is matched (TF-IDF text similarity) against the
  main series released in the ~3 weeks before it, and joins the best match if it's similar enough.
  Otherwise it stays a standalone bonus.

The feed doesn't say which series a bonus belongs to, so this is a heuristic.
`overrides.toml` fixes names (for series whose parts don't share a `Prefix:`) and forces
individual bonuses into a group.

## Usage

```sh
python -m rih --year 2026                     # markdown to stdout
python -m rih --year 2026 --format json -o reports/2026.json
python -m rih --year 2026 --explain           # show how each bonus was placed, with scores
python -m rih --feed-file feed.xml --year 2026   # work offline from a saved feed
```

Pure standard library, Python 3.11+. The feed URL defaults to my subscription; set
`RIH_FEED_URL` to use another.

Tests: `uv run --with pytest pytest`
