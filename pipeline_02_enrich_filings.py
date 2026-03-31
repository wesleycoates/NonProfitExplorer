#!/usr/bin/env python3
"""
pipeline_02_enrich_filings.py
──────────────────────────────
For each EIN in the organizations table, fetches all available 990 filings
from ProPublica and upserts them into the `filings` table.

Designed to be safely re-run (idempotent). Already-enriched EINs are skipped
unless --force is passed.

Usage:
    python pipeline_02_enrich_filings.py                 # all un-enriched EINs
    python pipeline_02_enrich_filings.py --limit 100     # first 100 (good for testing)
    python pipeline_02_enrich_filings.py --state IL      # only Illinois orgs
    python pipeline_02_enrich_filings.py --force         # re-fetch even if already done

Requirements:
    pip install psycopg2-binary requests
"""

import argparse
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
import psycopg2.extras
import requests
from psycopg2.pool import ThreadedConnectionPool

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────
PROPUBLICA_BASE = "https://projects.propublica.org/nonprofits/api/v2"
REQUESTS_PER_SECOND = 3     # ProPublica's rate limit is generous; be polite
REQUEST_TIMEOUT = 30


# ── ProPublica API ────────────────────────────────────────────────────────────

def fetch_filings_for_ein(ein: str) -> list[dict]:
    """
    Fetch all filings for an EIN from ProPublica.
    Returns a list of filing dicts (may be empty if org has no electronic filings).
    """
    url = f"{PROPUBLICA_BASE}/organizations/{ein}.json"
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 404:
            log.debug(f"  {ein}: 404 (no ProPublica record)")
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("filings_with_data", [])
    except requests.RequestException as e:
        log.warning(f"  {ein}: request failed — {e}")
        return []


# ── Data mapping ──────────────────────────────────────────────────────────────

def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _big(v):
    """Parse a potentially large integer (revenue, assets, etc.)"""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def filing_to_row(ein: str, f: dict, source_url: str) -> dict:
    """Map a ProPublica filing object to our DB column schema."""
    return {
        "ein":              ein,
        "tax_prd":          str(f.get("tax_prd") or ""),
        "form_type":        str(f.get("formtype") or f.get("form_type") or ""),
        "org_name":         str(f.get("orgs", [{}])[0].get("name", "") if f.get("orgs") else ""),
        "address":          str(f.get("address") or ""),
        "city":             str(f.get("city") or ""),
        "state":            str(f.get("state") or ""),
        "zipcode":          str(f.get("zipcode") or ""),
        # Revenue
        "totrevenue":       _big(f.get("totrevenue")),
        "totfuncexpns":     _big(f.get("totfuncexpns")),
        "totassetsend":     _big(f.get("totassetsend")),
        "totliabend":       _big(f.get("totliabend")),
        "netassetsend":     _big(f.get("netassetsend")),
        "prgmservrev":      _big(f.get("prgmservrev")),
        "investinc":        _big(f.get("investinc")),
        "othrevnue":        _big(f.get("othrevnue")),
        "grscontribs":      _big(f.get("grscontribs")),
        "fedgrnts":         _big(f.get("fedgrnts")),
        # Expenses
        "totprgmexpns":     _big(f.get("totprgmexpns")),
        "totgrants":        _big(f.get("totgrants")),
        "totmgmtexpns":     _big(f.get("totmgmtexpns")),
        "totfndrsng":       _big(f.get("totfndrsng")),
        "compnsatncurrofcr":_big(f.get("compnsatncurrofcr")),
        "othrsalwages":     _big(f.get("othrsalwages")),
        "payrolltx":        _big(f.get("payrolltx")),
        # Headcount
        "noemployees":      _int(f.get("noemployees")),
        "noemplyeesw2":     _int(f.get("noemplyeesw2")),
        "noindcontractor":  _int(f.get("noindcontractor")),
        # Metadata
        "pdf_url":          str(f.get("pdf_url") or ""),
        "source_url":       source_url,
        "raw_json":         json.dumps(f),
    }


# ── Database ──────────────────────────────────────────────────────────────────

UPSERT_SQL = """
INSERT INTO filings (
    ein, tax_prd, form_type,
    org_name, address, city, state, zipcode,
    totrevenue, totfuncexpns, totassetsend, totliabend, netassetsend,
    prgmservrev, investinc, othrevnue, grscontribs, fedgrnts,
    totprgmexpns, totgrants, totmgmtexpns, totfndrsng,
    compnsatncurrofcr, othrsalwages, payrolltx,
    noemployees, noemplyeesw2, noindcontractor,
    pdf_url, source_url, raw_json
) VALUES (
    %(ein)s, %(tax_prd)s, %(form_type)s,
    %(org_name)s, %(address)s, %(city)s, %(state)s, %(zipcode)s,
    %(totrevenue)s, %(totfuncexpns)s, %(totassetsend)s, %(totliabend)s, %(netassetsend)s,
    %(prgmservrev)s, %(investinc)s, %(othrevnue)s, %(grscontribs)s, %(fedgrnts)s,
    %(totprgmexpns)s, %(totgrants)s, %(totmgmtexpns)s, %(totfndrsng)s,
    %(compnsatncurrofcr)s, %(othrsalwages)s, %(payrolltx)s,
    %(noemployees)s, %(noemplyeesw2)s, %(noindcontractor)s,
    %(pdf_url)s, %(source_url)s, %(raw_json)s::jsonb
)
ON CONFLICT (ein, tax_prd) DO UPDATE SET
    form_type           = EXCLUDED.form_type,
    totrevenue          = EXCLUDED.totrevenue,
    totfuncexpns        = EXCLUDED.totfuncexpns,
    totassetsend        = EXCLUDED.totassetsend,
    totliabend          = EXCLUDED.totliabend,
    netassetsend        = EXCLUDED.netassetsend,
    prgmservrev         = EXCLUDED.prgmservrev,
    noemployees         = EXCLUDED.noemployees,
    pdf_url             = EXCLUDED.pdf_url,
    raw_json            = EXCLUDED.raw_json,
    ingested_at         = now()
;
"""

MARK_ENRICHED_SQL = """
UPDATE organizations
SET last_refreshed_at = now()
WHERE ein = %s
"""


def get_unenriched_eins(conn, state_filter: str | None, limit: int | None, force: bool) -> list[str]:
    """
    Return EINs that haven't been enriched yet (or all EINs if --force).
    'Enriched' means at least one row exists in the filings table.
    """
    where_clauses = []
    params = []

    if not force:
        where_clauses.append(
            "ein NOT IN (SELECT DISTINCT ein FROM filings)"
        )
    if state_filter:
        where_clauses.append("state = %s")
        params.append(state_filter.upper())

    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    limit_clause = f"LIMIT {limit}" if limit else ""

    sql = f"SELECT ein FROM organizations {where} ORDER BY ein {limit_clause}"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


# ── Worker ────────────────────────────────────────────────────────────────────

def process_ein(ein: str, pool: ThreadedConnectionPool) -> tuple[str, int, int]:
    """
    Fetch filings for an EIN, upsert them, and return (ein, n_inserted, n_errors).
    Designed to run in a thread pool.
    """
    source_url = f"{PROPUBLICA_BASE}/organizations/{ein}.json"
    raw_filings = fetch_filings_for_ein(ein)

    if not raw_filings:
        return ein, 0, 0

    rows = []
    for f in raw_filings:
        try:
            row = filing_to_row(ein, f, source_url)
            # Skip filings with no tax period (malformed)
            if not row["tax_prd"]:
                continue
            rows.append(row)
        except Exception as e:
            log.warning(f"  {ein}: row build failed — {e}")

    if not rows:
        return ein, 0, 0

    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows, page_size=50)
            cur.execute(MARK_ENRICHED_SQL, (ein,))
        conn.commit()
        return ein, len(rows), 0
    except Exception as e:
        conn.rollback()
        log.error(f"  {ein}: DB write failed — {e}")
        return ein, 0, len(rows)
    finally:
        pool.putconn(conn)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(database_url: str, state: str | None, limit: int | None, force: bool, workers: int):
    # Connection pool (one extra connection for the coordinator)
    pool = ThreadedConnectionPool(minconn=1, maxconn=workers + 2, dsn=database_url)
    coord_conn = pool.getconn()

    # Log run
    with coord_conn.cursor() as cur:
        cur.execute(
            "INSERT INTO pipeline_runs (pipeline, notes) VALUES (%s, %s) RETURNING id",
            ("02_enrich", f"state={state} limit={limit} force={force} workers={workers}"),
        )
        run_id = cur.fetchone()[0]
        coord_conn.commit()

    eins = get_unenriched_eins(coord_conn, state, limit, force)
    pool.putconn(coord_conn)

    log.info(f"EINs to enrich: {len(eins):,} | workers: {workers}")

    total_ok = 0
    total_err = 0
    completed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(process_ein, ein, pool): ein for ein in eins}
        for future in as_completed(futures):
            ein, n_ok, n_err = future.result()
            total_ok += n_ok
            total_err += n_err
            completed += 1
            if completed % 500 == 0 or completed == len(eins):
                log.info(
                    f"  Progress: {completed:,}/{len(eins):,} EINs | "
                    f"filings inserted: {total_ok:,} | errors: {total_err}"
                )
            time.sleep(1.0 / REQUESTS_PER_SECOND / workers)

    # Update run record
    update_conn = pool.getconn()
    with update_conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET status=%s, orgs_processed=%s, orgs_errored=%s, finished_at=now() WHERE id=%s",
            ("done", total_ok, total_err, run_id),
        )
        update_conn.commit()
    pool.putconn(update_conn)

    pool.closeall()
    log.info(f"Done. Filings inserted/updated: {total_ok:,}  Errors: {total_err}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich org filings from ProPublica")
    parser.add_argument("--state",   default=None, help="Filter to single state, e.g. IL")
    parser.add_argument("--limit",   default=None, type=int, help="Max EINs to process")
    parser.add_argument("--workers", default=3, type=int, help="Parallel threads (default: 3)")
    parser.add_argument("--force",   action="store_true", help="Re-enrich already-processed EINs")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise SystemExit("ERROR: Set DATABASE_URL environment variable.\n"
                         "Example: export DATABASE_URL='postgresql://nonprofit_user:yourpassword@localhost:5432/nonprofits'")

    run(db_url, args.state, args.limit, args.force, args.workers)
