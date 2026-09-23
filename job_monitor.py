#!/usr/bin/env python3
"""
Job Board Monitor — legacy compatibility helper
Monitors Greenhouse, Lever, Ashby, and USAJobs for new relevant postings.

Setup:
    pip install requests
    export USAJOBS_API_KEY="your_key_here"  # free at developer.usajobs.gov

Run:
    python3 job_monitor.py
    python3 job_monitor.py --reset     # clear seen jobs, re-scan everything
    python3 job_monitor.py --score 2   # only show jobs scoring 2+ keyword hits
"""

import argparse
import json
import os
import re
import sys
import requests
from datetime import datetime, date
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - exercised only in under-installed environments
    yaml = None

# ── CONFIG ────────────────────────────────────────────────────────────────────

# Greenhouse board tokens (confirmed working)
GREENHOUSE_COMPANIES = {
    "anthropic":    "Anthropic",
    "riotgames":    "Riot Games",
    "neumora":      "Neumora",
    "cloudflare":   "Cloudflare",
    "figma":        "Figma",
    "roblox":       "Roblox",
    "discord":      "Discord",
    "reddit":       "Reddit",
    "pinterest":    "Pinterest",
    "twitch":       "Twitch",
    "databricks":   "Databricks",
    "stabilityai":  "Stability AI",
}

# Lever company slugs — API: api.lever.co/v0/postings/{slug}
LEVER_COMPANIES = {
    "spotify":  "Spotify",
    "wmg":      "Warner Music Group",
    "palantir": "Palantir",
}

# Ashby company slugs — API: api.ashbyhq.com/posting-api/job-board/{slug}/jobs
# Note: some boards (e.g. OpenAI) are auth-restricted — those go to portals.yml.
# Note: openai and ironclad both return 401 — auth-restricted boards.
# Both kept in the manual portal config.
# NOTE: Legora, OpenAI, and Ironclad all run Ashby boards but return 401 from the
# public posting-api (they disabled the public JSON API) — they render only in the
# browser UI, so they live in the manual portal config. fetch_ashby remains ready for any
# future board that DOES expose the public API; add its slug here.
ASHBY_COMPANIES: dict = {}

# Workable company slugs — API: apply.workable.com/api/v1/widget/accounts/{slug}/vacancies
# Slug = subdomain visible at apply.workable.com/{slug}
# To add a company: confirm they use Workable, find their slug, add here.
WORKABLE_COMPANIES: dict = {}

# Workday company configs — uses the internal XHR API the public portal calls.
# Endpoint: POST https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
# To find values for a new company: look at their Workday URL:
#   https://{tenant}.{wd}.myworkdayjobs.com/{site}  →  extract tenant, wd (wd1/wd5), site.
WORKDAY_COMPANIES = {
    "nvidia":     {"tenant": "nvidia",     "wd": "wd5", "site": "NVIDIAExternalCareerSite", "name": "NVIDIA"},
    # AMD uses iCIMS (careers-amd.icims.com), not Workday — kept in portals.yml
    "intel":      {"tenant": "intel",      "wd": "wd1", "site": "External",                 "name": "Intel"},
    "adobe":      {"tenant": "adobe",      "wd": "wd5", "site": "external_experienced",     "name": "Adobe"},
    # Live Nation Workday instance returns 422 — non-standard config, kept in portals.yml
    "illumina":   {"tenant": "illumina",   "wd": "wd1", "site": "illumina-careers",         "name": "Illumina"},
    "disney":     {"tenant": "disney",     "wd": "wd5", "site": "disneycareer",             "name": "Disney"},
}

# Search terms sent to Workday's search API per company.
# Workday doesn't support OR syntax — each term is a separate request; results are deduped by job ID.
WORKDAY_SEARCH_TERMS = ["legal", "policy", "counsel", "patent", "copyright", "compliance"]

# Jobvite — requires internal alphanumeric company code (not URL slug), only
# discoverable via JS-rendered page. Not automatable without a real browser.
# Capcom USA kept in portals.yml instead.

# USAJobs location targets — None = national
USAJOBS_LOCATIONS = [
    "San Diego, California",
    None,
]

# ── MANUAL WATCHLIST ──────────────────────────────────────────────────────────
# Companies using Workday, Jobvite, or proprietary portals — check manually.
# Format: ("Company", "careers URL", "what to look for")

PORTALS_FILE = Path(__file__).parent / "portals.yml"

MANUAL_WATCHLIST_FALLBACK = [
    # ── San Diego — proprietary portals
    ("Qualcomm",               "https://careers.qualcomm.com",                          "IP legal, patent prosecution, policy"),
    # Illumina → moved to WORKDAY_COMPANIES (automated)
    ("Neurocrine Biosciences", "https://www.neurocrine.com/careers",                     "IP, regulatory, legal"),
    ("Dexcom",                 "https://careers.dexcom.com",                             "IP, legal, regulatory affairs"),
    ("Viasat",                 "https://careers.viasat.com/jobs",                        "IP, legal, tech policy"),
    ("SAIC",                   "https://jobs.saic.com",                                  "IP, legal, tech policy, AI"),
    ("Sony PlayStation",       "https://www.playstation.com/en-us/corporate/about/careers", "IP, licensing, entertainment law"),
    ("General Atomics",        "https://www.ga.com/careers",                             "tech policy, IP, legal"),
    # ── San Diego — IP/tech law firms (pre-bar clerk, patent agent, legal specialist)
    ("Knobbe Martens",         "https://www.knobbe.com/current-openings",                "3L Fall Associate (applied); watch for new law clerk or patent agent openings"),
    ("Fish & Richardson",      "https://www.fr.com/careers/",                            "technology specialist, patent agent, law student — SD office"),
    ("Procopio",               "https://www.procopio.com/careers/",                      "law clerk, legal specialist, IP, tech — SD-based AmLaw 200"),
    ("Sheppard Mullin",        "https://www.sheppardmullin.com/careers",                 "law clerk, legal specialist, entertainment IP, tech — SD office"),
    ("Cooley",                 "https://www.cooley.com/careers",                         "law clerk, legal specialist, IP, tech — SD office (Sorrento Valley)"),
    ("Wilson Sonsini",         "https://www.wsgr.com/en/careers.html",                   "law clerk, legal specialist, tech, IP — SD office"),
    ("DLA Piper",              "https://www.dlapiper.com/en-us/careers",                 "law clerk, IP, tech, policy — SD office"),
    ("Latham & Watkins",       "https://www.lw.com/en/careers",                          "law clerk, IP, tech, entertainment — SD office"),
    # ── San Diego — defense / policy
    ("Leidos",                 "https://careers.leidos.com",                             "policy analyst, legal, IP, tech — large SD presence"),
    ("Booz Allen Hamilton",    "https://careers.boozallen.com",                          "policy analyst, legal tech, AI policy — SD office"),
    ("Cubic Corporation",      "https://www.cubic.com/careers",                          "legal, policy, tech — SD-headquartered defense tech"),
    ("Kratos Defense",         "https://www.kratosdefense.com/about/careers",            "legal, policy, IP — SD-headquartered defense"),
    # ── San Diego — government / public interest
    ("City of San Diego",      "https://www.sandiego.gov/humanresources/jobseekers",     "city attorney, tech policy, legal"),
    ("County of San Diego",    "https://www.sandiegocounty.gov/content/sdc/hr/jobs",     "legal, policy, IP, consumer protection"),
    ("ACLU San Diego",         "https://www.aclusandiego.org/en/about/jobs-internships", "tech rights, AI, privacy"),
    # ── National — Workday / proprietary
    ("NBCUniversal",           "https://www.nbcunicareers.com",                          "IP, content licensing, entertainment"),
    # Disney → moved to WORKDAY_COMPANIES (automated)
    ("Live Nation",                 "https://careers.livenationentertainment.com/jobs",             "IP, legal, entertainment — Workday instance non-standard, check manually"),
    ("Capcom USA",             "https://jobs.jobvite.com/capcomusa",                     "IP, licensing, gaming legal"),
    ("TikTok / ByteDance",    "https://careers.tiktok.com",                             "trust & safety, content policy, legal, IP, AI policy"),
    # ── National — Ashby (auth-restricted boards — public API returns 401, check in browser)
    ("OpenAI",                 "https://jobs.ashbyhq.com/openai",                        "IP, policy, legal, developer relations"),
    ("Legora",                 "https://jobs.ashbyhq.com/legora",                        "Legal Engineer Associate, Legal Analyst, Legal Ops Specialist — top legal-tech target"),
    # ── AI Policy — direct sites
    ("GovAI (Oxford)",         "https://www.governance.ai/jobs",                         "research fellow, policy, AI governance"),
    ("Secure AI Project",      "https://www.secureaiproject.org/careers",                "policy analyst"),
    ("RAND Corporation",       "https://www.rand.org/jobs",                              "CAST fellowship, policy"),
    ("Brookings Institution",  "https://www.brookings.edu/careers",                      "AI policy, tech governance"),
    # ── Music / Copyright enforcement
    ("Universal Music Group",  "https://www.universalmusic.com/careers/",                "IP, music licensing, legal affairs, content policy, AI"),
    # Adobe → moved to WORKDAY_COMPANIES (automated)
    ("Getty Images",           "https://careers.gettyimages.com",                         "copyright enforcement, IP, licensing, AI content policy"),
    ("Meta",                   "https://metacareers.com",                                "AI policy, content policy, IP, trust & safety, legal"),
    ("Google",                 "https://careers.google.com",                             "AI policy, IP, legal, content policy, YouTube copyright"),
    ("Apple",                  "https://jobs.apple.com",                                 "privacy policy, IP, App Store policy, legal"),
    ("RIAA",                   "https://www.riaa.com/about-riaa/jobs/",                  "copyright enforcement, IP, policy, music licensing, AI"),
    # ── AI Policy Orgs (no bar, papers are the credential)
    ("IAPS",                        "https://iaps.ai/careers",                           "policy analyst, programs associate, AI governance"),
    ("CSET (Georgetown)",           "https://cset.georgetown.edu/careers/",               "research analyst, policy fellow, AI governance"),
    ("Center for Democracy & Tech", "https://cdt.org/about/employment/",                  "policy counsel, tech policy fellow, AI rights"),
    ("EFF",                         "https://www.eff.org/about/opportunities",            "legal fellow, policy analyst, Coase-Sandor fellow, IP"),
    ("Future of Privacy Forum",     "https://fpf.org/about/careers/",                    "policy counsel, research fellow, AI privacy"),
    ("Partnership on AI",           "https://partnershiponai.org/about/#careers",         "policy analyst, research associate, AI governance"),
    ("ITIF",                        "https://itif.org/about/employment/",                 "policy analyst, research fellow, tech policy"),
    ("Mozilla Foundation",          "https://www.mozilla.org/en-US/careers/",             "policy fellow, trustworthy AI, advocacy"),
    # ── Entertainment / Music Legal Affairs (no bar, coordinator/analyst level)
    ("Netflix",                     "https://jobs.netflix.com",                           "legal affairs, content policy, IP, licensing coordinator"),
    ("Sony Music",                  "https://www.sonybmusic.com/careers",                 "legal affairs, licensing, copyright, IP"),
    ("ASCAP",                       "https://www.ascap.com/about-ascap/careers",          "licensing, copyright, legal, royalties, music policy"),
    ("BMI",                         "https://www.bmi.com/about/careers",                  "licensing, copyright, legal, royalties, AI music"),
    ("Hulu",                        "https://jobs.hulu.com",                              "legal affairs, content policy, IP, licensing"),
    # ── Government (J.D.-advantage roles, no bar required at application)
    ("FTC",                         "https://www.ftc.gov/about-ftc/careers",              "law clerk, honors attorney, policy analyst, tech"),
    ("U.S. Copyright Office",       "https://www.loc.gov/careers/",                       "policy analyst, attorney advisor, AI copyright, Ringer Fellowship, Kaminstein Program"),
    ("NTIA",                        "https://www.ntia.gov/page/careers-ntia",             "policy analyst, tech policy, AI, spectrum"),
# ── Legal Tech and technology-policy roles
    # ── Dream employers / AI & semiconductor — high-match post-bar targets
    # NVIDIA → moved to WORKDAY_COMPANIES (automated)
    # Intel  → moved to WORKDAY_COMPANIES (automated)
    ("AMD",                         "https://careers-amd.icims.com/jobs/search",                    "IP counsel, legal, AI policy, patent — uses iCIMS not Workday"),
    ("IBM",                         "https://www.ibm.com/careers",                                   "AI governance, IP, legal, policy — Watson/AI ethics history"),
    ("Salesforce",                  "https://careers.salesforce.com",                                "AI policy, legal, IP, trust & safety, Einstein AI governance"),
    # Palantir — moved to LEVER_COMPANIES (automated)
    ("Hugging Face",                "https://apply.workable.com/huggingface/",                       "AI policy, legal, open source IP, model governance"),
    ("Cohere",                      "https://cohere.com/careers",                                    "AI governance, legal, IP, policy"),
    ("Mistral AI",                  "https://mistral.ai/careers",                                    "AI governance, legal, IP, policy — European AI Act angle"),
    ("Clio",                        "https://www.clio.com/about/careers/",                "legal tech, policy, compliance, product counsel"),
	("SpotDraft",                   "https://www.spotdraft.com/careers",                  "legal tech, AI legal, contracts, policy"),
	("Lexion",                      "https://www.lexion.ai/careers",                      "legal tech, AI contracts, legal operations"),
]


def load_manual_watchlist() -> list[tuple[str, str, str]]:
    """Load manual-check companies from portals.yml, falling back to the legacy list."""
    if yaml is None or not PORTALS_FILE.exists():
        return MANUAL_WATCHLIST_FALLBACK
    try:
        data = yaml.safe_load(PORTALS_FILE.read_text()) or {}
        watchlist = []
        for item in data.get("tracked_companies", []):
            if not item.get("enabled", True):
                continue
            name = str(item.get("name", "")).strip()
            url = str(item.get("careers_url", "")).strip()
            notes = str(item.get("notes", "")).strip()
            if name and url:
                watchlist.append((name, url, notes))
        return watchlist or MANUAL_WATCHLIST_FALLBACK
    except Exception as e:
        print(f"  ⚠ portals.yml: {e} — using hardcoded manual watchlist")
        return MANUAL_WATCHLIST_FALLBACK

# ── KEYWORDS ──────────────────────────────────────────────────────────────────
# Multi-word phrases checked first (substring), then single words with \b boundary.
# Keeps "IP" from matching "Principal", "principal", etc.

PHRASE_KEYWORDS = [
    "intellectual property",
    "developer relations",
    "AI policy",
    "legal specialist",
    "technical marketing",
    "product marketing",
    "solutions marketing",
    "legal counsel",
    "data privacy",
    "brand protection",
    "trust and safety",
    "trust & safety",
    "content policy",
    "business affairs",
    "legal affairs",
    "policy analyst",
    "policy manager",
    "policy fellow",
    "legal operations",
    "program manager",
    "research scholar",
    "research fellow",
]

WORD_KEYWORDS = [
    "patent", "trademark", "copyright", "counsel", "policy",
    "legal", "licensing", "governance", "MCP", "content",
    "fellow", "fellowship", "analyst", "compliance",
    "scholar", "researcher", "affairs", "coordinator",
    # Core pre-bar legal titles — previously scored 0 and were dropped by the score-0 gate
    "attorney", "paralegal", "clerk", "examiner",
]

# Subset of keywords signaling a core on-profile role. Used to rank these above
# broad matches like "content"/"coordinator" when sorting scan output.
HIGH_VALUE_KEYWORDS = {
    "ip", "patent", "trademark", "copyright", "intellectual property",
    "counsel", "legal counsel", "legal specialist", "licensing",
    "legal affairs", "business affairs", "policy analyst", "ai policy",
    "governance", "attorney", "paralegal", "examiner", "compliance",
}

# "IP" gets exact whole-word match (uppercase only) to avoid "principal" etc.
IP_PATTERN = re.compile(r'\bIP\b')


def score_title(title: str) -> tuple[int, list[str]]:
    if not title:
        return 0, []
    matched = []
    lower = title.lower()

    # Exact IP match (case-sensitive word boundary)
    if IP_PATTERN.search(title):
        matched.append("IP")

    # Multi-word phrases (case-insensitive substring)
    for phrase in PHRASE_KEYWORDS:
        if phrase.lower() in lower:
            matched.append(phrase)

    # Single words — skip any word already covered by a matched phrase or IP
    covered = {token for m in matched for token in m.lower().split()}
    for word in WORD_KEYWORDS:
        if word.lower() not in covered and re.search(rf'\b{re.escape(word)}\b', title, re.IGNORECASE):
            matched.append(word)

    return len(matched), matched


def priority_score(matched: list[str]) -> int:
    """Count of matched keywords that are core on-profile terms (for ranking)."""
    return sum(1 for m in matched if m.lower() in HIGH_VALUE_KEYWORDS)


# ── USAJobs ───────────────────────────────────────────────────────────────────

USAJOBS_EMAIL   = os.environ.get("USAJOBS_EMAIL", "jobby@example.invalid")
USAJOBS_API_KEY = os.environ.get("USAJOBS_API_KEY", "")
USAJOBS_SEARCHES = [
    "attorney advisor",            # GS-0905 series — standard federal attorney entry point
    "intellectual property",       # USPTO, Copyright Office, agency IP roles
    "technology policy analyst",   # NTIA, FCC, FTC, OSTP tech policy roles
    "AI policy",                   # Emerging AI-focused policy positions
    "copyright",                   # Copyright Office, LOC, agency copyright roles
    "legal policy",                # Policy-legal hybrid roles across agencies
    "policy analyst",              # FTC, FCC, NTIA, Copyright Office entry-level
    "law clerk",                   # Federal pre-bar clerk positions
    "legal intern",                # Summer/term legal positions
]

# Grade range to filter USAJobs results — avoids SES and GS-15 senior exec roles.
# GS-11 = entry attorney / policy analyst; GS-14 = senior but not exec.
USAJOBS_GRADE_MIN = 9
USAJOBS_GRADE_MAX = 13

STATE_FILE = Path(__file__).parent / "job_monitor_state.json"
OUTPUT_DIR = Path(__file__).parent


# ── STATE ─────────────────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"seen_jobs": [], "last_run": None}

def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ── JD FETCHERS ──────────────────────────────────────────────────────────────
# Fetch full job description text from various ATS APIs.
# Used with --fetch-jds flag. Saves to jds/ directory.

JDS_DIR = Path(__file__).parent / "jds"

def fetch_jd_greenhouse(token: str, job_id: str) -> str:
    """Fetch full JD from Greenhouse API. Returns HTML content or empty string."""
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs/{job_id}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        content = data.get("content", "")
        title = data.get("title", "")
        location = data.get("location", {}).get("name", "")
        # Combine metadata + content
        parts = [f"# {title}", f"**Location:** {location}", "", content]
        return "\n".join(parts)
    except Exception:
        return ""

def fetch_jd_lever(slug: str, job_id: str) -> str:
    """Fetch full JD from Lever API. Returns text or empty string."""
    url = f"https://api.lever.co/v0/postings/{slug}/{job_id}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        parts = [f"# {data.get('text', '')}"]
        desc = data.get("description", "")
        if desc:
            parts.append(desc)
        for lst in data.get("lists", []):
            parts.append(f"\n## {lst.get('text', '')}")
            parts.append("\n".join(f"- {item}" for item in lst.get("content", "").split("<li>") if item.strip()))
        return "\n".join(parts)
    except Exception:
        return ""

def fetch_jd_usajobs(item: dict) -> str:
    """Extract JD from USAJobs search result item."""
    desc = item.get("MatchedObjectDescriptor", {})
    parts = [
        f"# {desc.get('PositionTitle', '')}",
        f"**Organization:** {desc.get('OrganizationName', '')}",
        f"**Location:** {', '.join(loc.get('LocationName','') for loc in desc.get('PositionLocation', []))}",
        f"**Grade:** {desc.get('JobGrade', [{}])[0].get('Code', '')}",
        f"**Salary:** {desc.get('PositionRemuneration', [{}])[0].get('MinimumRange', '')} - {desc.get('PositionRemuneration', [{}])[0].get('MaximumRange', '')}",
        f"**Close Date:** {desc.get('ApplicationCloseDate', '')[:10]}",
        "",
    ]
    user_area = desc.get("UserArea", {}).get("Details", {})
    qual = desc.get("QualificationSummary", "")
    if qual:
        parts.append("## Qualifications")
        parts.append(qual)
    duties = user_area.get("MajorDuties", [])
    if duties:
        parts.append("\n## Major Duties")
        if isinstance(duties, list):
            parts.extend(f"- {d}" for d in duties)
        else:
            parts.append(str(duties))
    return "\n".join(parts)

def fetch_jd_workday(job_url: str) -> str:
    """Best-effort Workday JD fetch from a public job URL. Returns text or ''."""
    m = re.match(r'https://([^.]+)\.(wd\d+)\.myworkdayjobs\.com(/.*)', job_url)
    if not m:
        return ""
    tenant, wd, path = m.group(1), m.group(2), m.group(3)
    segs = [s for s in path.split("/") if s]   # e.g. ['en-US', '{site}', 'job', ..., 'JR123']
    if len(segs) < 3:
        return ""
    site = segs[1]
    rest = "/" + "/".join(segs[2:])
    api  = f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}{rest}"
    try:
        r = requests.get(api, headers={"Accept": "application/json"}, timeout=10)
        r.raise_for_status()
        info = r.json().get("jobPostingInfo", {})
        body = info.get("jobDescription", "")
        if not body:
            return ""
        parts = [f"# {info.get('title', '')}", f"**Location:** {info.get('location', '')}", "", body]
        return "\n".join(parts)
    except Exception:
        return ""

def save_jd(company: str, title: str, content: str):
    """Save JD text to jds/ directory."""
    if not content.strip():
        return
    JDS_DIR.mkdir(exist_ok=True)
    slug = re.sub(r'[^a-z0-9]+', '-', f"{company}-{title}".lower()).strip('-')[:80]
    path = JDS_DIR / f"{slug}.md"
    path.write_text(content)

def append_to_pipeline(jobs: list[dict]):
    """Append new jobs to data/pipeline.md."""
    pipeline_path = Path(__file__).parent / "data" / "pipeline.md"
    lines = []
    for job in jobs:
        score = job.get("score", 0)
        company = job.get("company", "")
        title = job.get("title", "")
        url = job.get("url", "")
        warn = " (LIKELY SENIOR)" if job.get("seniority_warning") else ""
        lines.append(f"- [{score}] {company} — {title}{warn} | Discovered | {url}")
    if lines:
        with open(pipeline_path, "a") as f:
            f.write("\n".join(lines) + "\n")


# ── FETCHERS ──────────────────────────────────────────────────────────────────

def fetch_greenhouse(token: str) -> list[dict]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jobs = r.json().get("jobs", [])
        return [{"title": j.get("title",""), "url": j.get("absolute_url",""), "id": str(j.get("id",""))} for j in jobs]
    except requests.HTTPError:
        if r.status_code == 404:
            print(f"  ⚠  greenhouse/{token}: board not found")
        else:
            print(f"  ⚠  greenhouse/{token}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠  greenhouse/{token}: {e}")
        return []

def fetch_lever(slug: str) -> list[dict]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jobs = r.json()
        return [{"title": j.get("text",""), "url": j.get("hostedUrl",""), "id": j.get("id","")} for j in jobs]
    except requests.HTTPError:
        if r.status_code == 404:
            print(f"  ⚠  lever/{slug}: board not found (slug may have changed) — SOURCE FAILED, not 'nothing new'")
        else:
            print(f"  ⚠  lever/{slug}: HTTP {r.status_code} — SOURCE FAILED")
        return []
    except Exception as e:
        print(f"  ⚠  lever/{slug}: {e} — SOURCE FAILED")
        return []

def fetch_ashby(slug: str) -> list[dict]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}/jobs"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jobs = r.json().get("jobs", [])
        return [{"title": j.get("title",""), "url": j.get("jobUrl",""), "id": str(j.get("id",""))} for j in jobs]
    except requests.HTTPError:
        if r.status_code == 404:
            print(f"  ⚠  ashby/{slug}: board not found (slug may have changed) — SOURCE FAILED, not 'nothing new'")
        elif r.status_code == 401:
            print(f"  ⚠  ashby/{slug}: auth-restricted (401) — add to portals.yml")
        else:
            print(f"  ⚠  ashby/{slug}: HTTP {r.status_code} — SOURCE FAILED")
        return []
    except Exception as e:
        print(f"  ⚠  ashby/{slug}: {e} — SOURCE FAILED")
        return []

def fetch_workable(slug: str) -> list[dict]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}/vacancies"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        jobs = r.json().get("results", [])
        return [{"title": j.get("title", ""), "url": f"https://apply.workable.com/{slug}/j/{j.get('shortcode', '')}/", "id": j.get("shortcode", "")} for j in jobs]
    except requests.HTTPError:
        if r.status_code == 404:
            print(f"  ⚠  workable/{slug}: board not found")
        else:
            print(f"  ⚠  workable/{slug}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠  workable/{slug}: {e}")
        return []

def fetch_workday(tenant: str, wd: str, site: str) -> list[dict]:
    base_url = f"https://{tenant}.{wd}.myworkdayjobs.com"
    api_url  = f"{base_url}/wday/cxs/{tenant}/{site}/jobs"
    headers  = {"Content-Type": "application/json", "Accept": "application/json"}
    seen_ids: set[str] = set()
    all_jobs: list[dict] = []
    for term in WORKDAY_SEARCH_TERMS:
        payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": term}
        try:
            r = requests.post(api_url, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            for job in r.json().get("jobPostings", []):
                path   = job.get("externalPath", "")
                # Job ID is the segment after the last underscore in the path
                job_id = path.rsplit("_", 1)[-1] if "_" in path else path
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)
                all_jobs.append({
                    "title": job.get("title", ""),
                    "url":   f"{base_url}{path}" if path else "",
                    "id":    job_id,
                })
        except requests.HTTPError:
            code = r.status_code
            if code == 404:
                print(f"  ⚠  workday/{tenant}: board not found")
                break   # Board does not exist — no point trying other terms
            else:
                print(f"  ⚠  workday/{tenant} [{term}]: HTTP {code}")
                continue  # Transient error — try remaining terms
        except Exception as e:
            print(f"  ⚠  workday/{tenant} [{term}]: {e}")
            continue  # Transient error — try remaining terms
    return all_jobs


def fetch_jobvite(slug: str) -> list[dict]:
    url = f"https://jobs.jobvite.com/api/jobs?c={slug}&d=json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        jobs = data.get("jobs", data) if isinstance(data, dict) else data
        if not isinstance(jobs, list):
            print(f"  ⚠  jobvite/{slug}: unexpected response format")
            return []
        result = []
        for i, j in enumerate(jobs):
            job_url = j.get("jobUrl", j.get("url", ""))
            # Extract ID from last URL path segment as a stable fallback
            url_id = job_url.rstrip("/").rsplit("/", 1)[-1] if "/" in job_url else ""
            job_id = j.get("jobId", j.get("id", url_id or f"idx_{i}"))
            result.append({"title": j.get("title", ""), "url": job_url, "id": job_id})
        return result
    except requests.HTTPError:
        if r.status_code == 404:
            print(f"  ⚠  jobvite/{slug}: board not found")
        else:
            print(f"  ⚠  jobvite/{slug}: HTTP {r.status_code}")
        return []
    except Exception as e:
        print(f"  ⚠  jobvite/{slug}: {e}")
        return []

def fetch_usajobs(keyword: str, days: int = 14, location: str = None) -> list[dict]:
    if not USAJOBS_API_KEY:
        return []
    headers = {
        "Authorization-Key": USAJOBS_API_KEY,
        "User-Agent":        USAJOBS_EMAIL,
        "Host":              "data.usajobs.gov",
    }
    params = {
        "Keyword": keyword,
        "DatePosted": days,
        "ResultsPerPage": 25,
        "GradeMin": str(USAJOBS_GRADE_MIN),
        "GradeMax": str(USAJOBS_GRADE_MAX),
    }
    if location:
        params["LocationName"] = location
    try:
        r = requests.get("https://data.usajobs.gov/api/search", headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json().get("SearchResult", {}).get("SearchResultItems", [])
    except Exception as e:
        print(f"  ⚠  USAJobs '{keyword}': {e}")
        return []


# ── OUTPUT ────────────────────────────────────────────────────────────────────

def print_job(job: dict):
    star  = "★" if job["score"] > 0 else "·"
    warn  = " ⚠ LIKELY SENIOR" if job.get("seniority_warning") else ""
    kws   = f"  [{', '.join(job['matched_keywords'])}]" if job.get("matched_keywords") else ""
    close = f"  closes {job['close_date']}" if job.get("close_date") else ""
    print(f"    {star} {job['title']}{warn}{kws}{close}")
    print(f"      {job['url']}")

def save_markdown(jobs: list[dict]):
    if not jobs:
        return
    path = OUTPUT_DIR / f"new_jobs_{date.today()}.md"
    with open(path, "w") as f:
        f.write(f"# New Job Postings — {date.today()}\n\n")
        for job in sorted(jobs, key=lambda x: (-x.get("priority", 0), -x["score"], x.get("seniority_warning", False))):
            warn = " ⚠ LIKELY SENIOR" if job.get("seniority_warning") else ""
            f.write(f"## {job['title']}{warn}\n")
            f.write(f"**{job['company']}** | {job['source'].upper()}")
            if job.get("close_date"):
                f.write(f" | Closes: {job['close_date']}")
            f.write("\n\n")
            if job.get("matched_keywords"):
                f.write(f"Keywords matched: {', '.join(job['matched_keywords'])}\n\n")
            f.write(f"{job['url']}\n\n---\n\n")
    print(f"\nSaved → {path.name}")


# ── HELPERS ───────────────────────────────────────────────────────────────────

EXCLUDE_PATTERNS = [
    re.compile(r'\bsenior\b',         re.IGNORECASE),
    re.compile(r'\bdirector\b',       re.IGNORECASE),
    re.compile(r'\bsr\.',             re.IGNORECASE),
    re.compile(r'\bvp\b',            re.IGNORECASE),
    re.compile(r'\bvice president\b', re.IGNORECASE),
    re.compile(r'\bhead of\b',        re.IGNORECASE),
    re.compile(r'\bprincipal\b',      re.IGNORECASE),
    # Exclude standalone title-start Advisor roles, while keeping federal
    # "Attorney Advisor" and policy-adjacent "Policy Advisor" roles visible.
    re.compile(r'^advisor\b',        re.IGNORECASE),
    re.compile(r'\bchief\b.*\bofficer\b', re.IGNORECASE),  # C-suite only — NOT entry "Compliance/Privacy/T&S Officer"
    # NOTE: bare "intern"/"internship" removed — retain focused paid roles
    # (e.g. Hulu Business Affairs & Legal JD Intern). Unpaid ones are filtered at evaluation.
    re.compile(r'\bstaff\b',          re.IGNORECASE),  # "Staff Engineer", "Staff PM" = senior IC
    re.compile(r'\b(?:TPM|Program Manager)\s*III\b', re.IGNORECASE),  # level III+ = senior
    re.compile(r'\bsupervisory\b',    re.IGNORECASE),  # federal senior/management
]

# Titles matching these get a ⚠ flag in output — likely mid-career but not auto-excluded.
# Kept visible so edge cases (solo IC "manager" at a startup) aren't lost.
SENIORITY_WARN_PATTERNS = [
    re.compile(r'\blead\b',           re.IGNORECASE),
    re.compile(r'\bmanager\b',        re.IGNORECASE),
    re.compile(r'\b(?:III|IV)\b'),                      # level III+ only — "II" is an early rung, not senior
]

def is_excluded(title: str) -> bool:
    return any(p.search(title) for p in EXCLUDE_PATTERNS)

def has_seniority_warning(title: str) -> bool:
    return any(p.search(title) for p in SENIORITY_WARN_PATTERNS)

def process_jobs(raw_jobs, source_prefix, company_name, seen, new_jobs, min_score):
    company_new = []
    for job in raw_jobs:
        uid   = f"{source_prefix}_{job['id']}"
        title = job["title"]
        if is_excluded(title):
            continue
        score, matched = score_title(title)
        if uid not in seen and score >= min_score:
            seen.add(uid)
            entry = {
                "uid":              uid,
                "company":          company_name,
                "title":            title,
                "url":              job["url"],
                "score":            score,
                "priority":         priority_score(matched),
                "matched_keywords": matched,
                "seniority_warning": has_seniority_warning(title),
                "source":           source_prefix.split("_")[0],
                "found":            str(date.today()),
            }
            if job.get("close_date"):
                entry["close_date"] = job["close_date"]
            company_new.append(entry)
            new_jobs.append(entry)
    return company_new


# ── MAIN ──────────────────────────────────────────────────────────────────────

def run(min_score: int = 0, reset: bool = False, pipeline: bool = False, fetch_jds: bool = False):
    state = load_state()
    if reset:
        state["seen_jobs"] = []
        print("State reset — rescanning all postings.\n")
    seen     = set(state["seen_jobs"])
    new_jobs: list[dict] = []

    print(f"\n{'='*60}")
    print(f"  Job Monitor — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if state["last_run"]:
        print(f"  Last run: {state['last_run']}")
    print(f"{'='*60}\n")

    # ── Greenhouse
    print("GREENHOUSE")
    print("-" * 40)
    for token, name in GREENHOUSE_COMPANIES.items():
        raw  = fetch_greenhouse(token)
        hits = process_jobs(raw, f"gh_{token}", name, seen, new_jobs, min_score)
        if hits:
            print(f"\n  {name} — {len(hits)} new:")
            for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                print_job(j)
        else:
            print(f"  {name} — nothing new")

    # ── Lever
    print("\n\nLEVER")
    print("-" * 40)
    for slug, name in LEVER_COMPANIES.items():
        raw  = fetch_lever(slug)
        hits = process_jobs(raw, f"lever_{slug}", name, seen, new_jobs, min_score)
        if hits:
            print(f"\n  {name} — {len(hits)} new:")
            for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                print_job(j)
        else:
            print(f"  {name} — nothing new")

    # ── Ashby
    print("\n\nASHBY")
    print("-" * 40)
    for slug, name in ASHBY_COMPANIES.items():
        raw  = fetch_ashby(slug)
        hits = process_jobs(raw, f"ashby_{slug}", name, seen, new_jobs, min_score)
        if hits:
            print(f"\n  {name} — {len(hits)} new:")
            for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                print_job(j)
        else:
            print(f"  {name} — nothing new")

    # ── Workable
    print("\n\nWORKABLE")
    print("-" * 40)
    for slug, name in WORKABLE_COMPANIES.items():
        raw  = fetch_workable(slug)
        hits = process_jobs(raw, f"workable_{slug}", name, seen, new_jobs, min_score)
        if hits:
            print(f"\n  {name} — {len(hits)} new:")
            for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                print_job(j)
        else:
            print(f"  {name} — nothing new")

    # ── Workday
    print("\n\nWORKDAY")
    print("-" * 40)
    for slug, cfg in WORKDAY_COMPANIES.items():
        raw  = fetch_workday(cfg["tenant"], cfg["wd"], cfg["site"])
        hits = process_jobs(raw, f"workday_{slug}", cfg["name"], seen, new_jobs, min_score)
        if hits:
            print(f"\n  {cfg['name']} — {len(hits)} new:")
            for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                print_job(j)
        else:
            print(f"  {cfg['name']} — nothing new")

    # ── USAJobs
    if USAJOBS_API_KEY:
        print("\n\nUSAJOBS")
        print("-" * 40)
        seen_usa: set[str] = set()
        for location in USAJOBS_LOCATIONS:
            print(f"\n  [{location or 'National'}]")
            for keyword in USAJOBS_SEARCHES:
                for item in fetch_usajobs(keyword, location=location):
                    desc   = item.get("MatchedObjectDescriptor", {})
                    job_id = desc.get("PositionID", "")
                    if not job_id:
                        continue
                    uid = f"usa_{job_id}"
                    if uid in seen_usa or uid in seen:
                        continue
                    seen_usa.add(uid)
                    close = desc.get("ApplicationCloseDate", "")
                    raw = {
                        "title":      desc.get("PositionTitle", ""),
                        "url":        desc.get("PositionURI", ""),
                        "id":         job_id,
                        "close_date": close[:10] if close else "",
                    }
                    company = desc.get("OrganizationName", "Federal Government")
                    hits = process_jobs([raw], "usa", company, seen, new_jobs, min_score)
                    if fetch_jds and hits:
                        save_jd(company, raw["title"], fetch_jd_usajobs(item))
                    for j in sorted(hits, key=lambda x: x["score"], reverse=True):
                        print_job(j)
    else:
        print("\n(USAJobs skipped — set USAJOBS_API_KEY to enable)")

    # ── Manual watchlist
    manual_watchlist = load_manual_watchlist()
    print(f"\n\nMANUAL CHECKS (proprietary portals — iCIMS, Jobvite, custom)")
    print("-" * 40)
    for company, url, notes in manual_watchlist:
        print(f"  {company}")
        print(f"    {url}")
        print(f"    → {notes}")

    # ── Summary
    print(f"\n{'='*60}")
    print(f"  {len(new_jobs)} new automated posting(s) found")
    print(f"  {len(manual_watchlist)} sites require manual check")
    print(f"{'='*60}\n")

    save_markdown(new_jobs)

    # ── Pipeline mode: append scored jobs to data/pipeline.md
    if pipeline and new_jobs:
        scored = [j for j in new_jobs if j["score"] >= min_score]
        if scored:
            append_to_pipeline(scored)
            print(f"\n  → Appended {len(scored)} job(s) to data/pipeline.md")

    # ── Fetch JDs for keyword-matched jobs
    if fetch_jds and new_jobs:
        scored = [j for j in new_jobs if j["score"] >= min_score]
        fetched = 0
        for job in scored:
            source = job.get("source", "")
            uid = job.get("uid", "")
            jd_text = ""
            if source == "gh":
                # Greenhouse: extract token and job_id from uid
                parts = uid.split("_", 2)  # gh_token_id
                if len(parts) >= 3:
                    jd_text = fetch_jd_greenhouse(parts[1], parts[2])
            elif source == "lever":
                parts = uid.split("_", 2)
                if len(parts) >= 3:
                    jd_text = fetch_jd_lever(parts[1], parts[2])
            elif source == "workday":
                jd_text = fetch_jd_workday(job.get("url", ""))
            # USAJobs JDs are saved inline during the USAJobs scan above
            # (its search descriptor isn't stored on the job entry).
            if jd_text:
                save_jd(job["company"], job["title"], jd_text)
                fetched += 1
        if fetched:
            print(f"  → Saved {fetched} job description(s) to jds/")

    state["seen_jobs"] = list(seen)
    state["last_run"]  = str(datetime.now())
    save_state(state)


def compatibility_main(argv: list[str] | None = None) -> int:
    """Delegate the historical command to SQLite-backed Jobby.

    The legacy implementation remains importable so old deterministic unit
    tests and one-off readers do not break, but command execution must never
    create a second mutable job state beside Jobby's database.
    """

    parser = argparse.ArgumentParser(description="Job board monitor")
    parser.add_argument("--reset", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--score", type=int, default=0, help=argparse.SUPPRESS)
    parser.add_argument("--pipeline", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--fetch-jds", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.reset or args.score or args.pipeline or args.fetch_jds:
        print(
            "DEPRECATED: legacy flags no longer write JSON, reports, JDs, or pipeline Markdown; "
            "Jobby will run one SQLite-backed scan.",
            file=sys.stderr,
        )
    else:
        print(
            "DEPRECATED: job_monitor.py now delegates to 'jobby scan'.",
            file=sys.stderr,
        )
    from jobby.cli import main as jobby_main

    return jobby_main(["scan"])


if __name__ == "__main__":
    raise SystemExit(compatibility_main())
