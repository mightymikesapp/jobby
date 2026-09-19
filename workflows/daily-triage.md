# Daily job triage

1. Read `jobby://profile` and `jobby://sources`.
2. Call `get_source_health` and `get_latest_scan`; report stale or partial sources.
3. Use `get_search_facets`, then `search_jobs` with known facets only.
4. Inspect shortlisted jobs with `get_job` and `get_market_fit`.
5. Use `preview_capture` for pasted or URL-only jobs. Ask for explicit confirmation before `save_captured_job`.
6. Create applications only after the user explicitly chooses a role; never submit externally.

Keep descriptions truncated unless the user asks for full content. Preserve IDs,
source URLs, timestamps, score explanations, and warnings in the summary.
