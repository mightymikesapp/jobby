# Jobby scale gates

The scale harness is entirely offline and always creates a new database in an
explicit or temporary directory. It does not read the configured Jobby home,
install a scheduler, call a job board, or enable an integration.

Run the small gate used by the normal test suite:

```console
uv run python scripts/scale_gate.py --profile smoke --output smoke-report.json
```

Run the exact release fixture—100,000 jobs, 10,000 applications, and exactly
1,000,000 in-place `JobSourceState` updates—with an explicit retained work
directory:

```console
uv run python scripts/scale_gate.py \
  --profile release \
  --confirm-exact-scale \
  --work-dir .scale-gate/release-0.6 \
  --output .scale-gate/release-0.6.json
```

The JSON report records stage latency and Python allocation peaks, checkpointed
database/WAL growth, first/tail SQL pagination latency, process peak RSS,
streaming CSV export latency and peak allocation, exact row/update counts, and
database integrity. Portable ceilings catch gross failures. To gate a later run
against the recorded macOS ARM64/Python 3.13 release baseline as well:

```console
uv run python scripts/scale_gate.py \
  --profile release \
  --confirm-exact-scale \
  --work-dir .scale-gate/candidate \
  --output .scale-gate/candidate.json \
  --baseline scale-baselines/release-0.6-macos-arm64-python313.json \
  --max-regression-percent 25
```

Recorded comparisons are environment-specific: use the matching OS,
architecture, Python, and SQLite baseline, or record a new baseline before
enforcing percentage regressions on another release builder. Absolute memory,
latency, and storage ceilings still apply on every environment.

The exact pytest gate is skipped by default. Enable it only in an isolated
release job:

```console
JOBBY_RUN_EXACT_SCALE=1 uv run pytest tests/jobby/test_scale_gate.py -m scale
```

The release workflow runs the exact command directly on its macOS 15 arm64,
Python 3.13 builder and enforces the checked-in
`release-0.6-macos-arm64-python313.json` baseline at 25%. Its report is retained
as a workflow artifact even when the comparison fails.

Every pytest process installs the repository's offline socket guard before test
collection. It blocks external DNS and non-loopback socket traffic, propagates
to child Python processes, and deliberately permits Unix sockets, IP loopback,
and in-memory transports such as HTTPX `MockTransport`. The exact CI scale
process loads the same guard through `sitecustomize`; release gates therefore
cannot accidentally query a live job board or billable API.
