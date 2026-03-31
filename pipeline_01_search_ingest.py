#!/usr/bin/env python3
"""
pipeline_01_search_ingest.py
────────────────────────────
Searches ProPublica Nonprofit Explorer for healthcare 501(c)(3) orgs
and upserts them into the `organizations` table.

Usage:
    python pipeline_01_search_ingest.py                  # all 50 states
    python pipeline_01_search_ingest.py --state IL       # single state (good for testing)
    python pipeline_01_search_ingest.py --dry-run        # print counts, write nothing

Requirements:
    pip install psycopg2-binary requests
"""

import argparse
import logging
import os
import time

import psycopg2
import psycopg2.extras
import requests

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"
NTEE_HEALTH_CODE = "4"      # NTEE major group 4 = Health
SUBSECCD = "3"              # 501(c)(3)
REQUESTS_PER_SECOND = 2     # ProPublica is generous; stay polite
PAGE_SIZE = 25              # max records per page

ALL_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC","PR",
]

# ── ProPublica API ────────────────────────────────────────────────────────────

def search_orgs(state: str, ntee: str, page: int) -> dict:
    """Call ProPublica search endpoint and return the JSON response."""
    url = f"{PROPUBLICA_BASE}/search.json"
    params = {
        "state[id]": state,
        "ntee[id]": ntee,
        "c_code[id]": SUBSECCD,
        "page": page,
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_for_state(state: str) -> list[dict]:
    """Page through all results for a given state. Returns list of org dicts."""
    orgs = []
    page = 0
    while True:
        try:
            data = search_orgs(state, NTEE_HEALTH_CODE, page)
        except requests.HTTPError as e:
            log.warning(f"  HTTP error on {state} page {page}: {e}")
            break

        organizations = data.get("organizations", [])
        if not organizations:
            break

        orgs.extend(organizations)
        total_results = data.get("total_results", 0)
        log.debug(f"  {state} page {page}: got {len(organizations)} orgs (total={total_results})")

        # If we got fewer than PAGE_SIZE, we've hit the last page
        if len(organizations) < PAGE_SIZE:
            break

        page += 1
        time.sleep(1.0 / REQUESTS_PER_SECOND)

    return orgs


# ── Database ──────────────────────────────────────────────────────────────────

UPSERT_SQL = """
INSERT INTO organizations (
    ein, name, city, state, zipcode,
    ntee_code, ntee_major, subseccd,
    ruling_date, asset_cd, income_cd,
    affiliation, classification, deductibility,
    foundation, activity, organization, status,
    tax_period, acct_pd, filing_req_cd, sort_name,
    last_refreshed_at
) VALUES (
    %(ein)s, %(name)s, %(city)s, %(state)s, %(zipcode)s,
    %(ntee_code)s, %(ntee_major)s, %(subseccd)s,
    %(ruling_date)s, %(asset_cd)s, %(income_cd)s,
    %(affiliation)s, %(classification)s, %(deductibility)s,
    %(foundation)s, %(activity)s, %(organization)s, %(status)s,
    %(tax_period)s, %(acct_pd)s, %(filing_req_cd)s, %(sort_name)s,
    now()
)
ON CONFLICT (ein) DO UPDATE SET
    name             = EXCLUDED.name,
    city             = EXCLUDED.city,
    state            = EXCLUDED.state,
    zipcode          = EXCLUDED.zipcode,
    ntee_code        = EXCLUDED.ntee_code,
    asset_cd         = EXCLUDED.asset_cd,
    income_cd        = EXCLUDED.income_cd,
    tax_period       = EXCLUDED.tax_period,
    last_refreshed_at = now()
;
"""


def org_to_row(o: dict) -> dict:
    """Map a ProPublica API org object to our DB column names."""
    return {
        "ein":           str(o.get("ein", "")).replace("-", "").strip(),
        "name":          (o.get("name") or o.get("strname") or "")[:500],
        "city":          o.get("city") or "",
        "state":         o.get("state") or "",
        "zipcode":       o.get("zipcode") or "",
        "ntee_code":     o.get("ntee_code") or "",
        "ntee_major":    int(o.get("ntee_major") or 0) or None,
        "subseccd":      str(o.get("subseccd") or ""),
        "ruling_date":   str(o.get("ruling_date") or ""),
        "asset_cd":      _int(o.get("asset_cd")),
        "income_cd":     _int(o.get("income_cd")),
        "affiliation":   _int(o.get("affiliation")),
        "classification":str(o.get("classification") or ""),
        "deductibility": _int(o.get("deductibility")),
        "foundation":    _int(o.get("foundation")),
        "activity":      str(o.get("activity") or ""),
        "organization":  _int(o.get("organization")),
        "status":        _int(o.get("status")),
        "tax_period":    str(o.get("tax_period") or ""),
        "acct_pd":       _int(o.get("acct_pd")),
        "filing_req_cd": _int(o.get("filing_req_cd")),
        "sort_name":     str(o.get("sort_name") or ""),
    }


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def upsert_batch(cur, rows: list[dict]):
    psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=200)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(database_url: str, states: list[str], dry_run: bool):
    conn = psycopg2.connect(database_url)
    conn.autocommit = False

    # Log this run
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (pipeline, notes) VALUES (%s, %s) RETURNING id",
            ("01_search", f"states={','.join(states)} dry_run={dry_run}"),
        )
        run_id = cur.fetchone()[0]
        conn.commit()

    total_ok = 0
    total_err = 0

    for state in states:
        log.info(f"Fetching {state} …")
        orgs = fetch_all_for_state(state)
        log.info(f"  {state}: {len(orgs)} orgs returned from API")

        if dry_run:
            total_ok += len(orgs)
            continue

        rows = []
        for o in orgs:
            try:
                rows.append(org_to_row(o))
            except Exception as e:
                log.warning(f"  Row build failed for {o.get('ein')}: {e}")
                total_err += 1

        try:
            with conn.cursor() as cur:
                upsert_batch(cur, rows)
            conn.commit()
            total_ok += len(rows)
            log.info(f"  {state}: upserted {len(rows)} orgs  (running total: {total_ok:,})")
        except Exception as e:
            conn.rollback()
            log.error(f"  {state}: DB write failed — {e}")
            total_err += len(rows)

        time.sleep(0.5)  # brief pause between states

    # Close pipeline run record
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status=%s, orgs_processed=%s, orgs_errored=%s, finished_at=now() WHERE id=%s",
            ("done" if not dry_run else "dry_run", total_ok, total_err, run_id),
        )
        conn.commit()

    conn.close()
    log.info(f"Done. Upserted: {total_ok:,}  Errors: {total_err:,}")
    if dry_run:
        log.info("(DRY RUN — nothing written to DB)")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest healthcare nonprofits from ProPublica")
    parser.add_argument("--state", default=None, help="Single state code, e.g. IL (omit for all states)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch from API but don't write to DB")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url and not args.dry_run:
        raise SystemExit("ERROR: Set DATABASE_URL environment variable before running.\n"
                         "Example: export DATABASE_URL='postgresql://nonprofit_user:yourpassword@localhost:5432/nonprofits'")

    states = [args.state.upper()] if args.state else ALL_STATES
    run(db_url, states, args.dry_run)
