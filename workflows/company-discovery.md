# Company discovery

Use configured ATS sources and optional catalogs as observations. Apply role,
location, industry, and candidate constraints deterministically first. Store
the evidence and score with `discover_companies`; do not imply endorsement.

Only after the user approves a candidate should `watch_company` be called. A
watchlist is local and recurring; it does not contact a company or submit an
application.
