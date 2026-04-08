
import os
from dotenv import load_dotenv
load_dotenv(os.path.expanduser("~/.env"))
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import psycopg2
import psycopg2.extras

app = Flask(__name__)
CORS(app)

def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])

# ── /api/meta ─────────────────────────────────────────────────────────────────
# Returns dropdown options: states, NTEE types, years, revenue bounds
@app.route("/api/meta")
def meta():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT o.state
        FROM organizations o
        JOIN filings f ON f.ein = o.ein
        JOIN personnel p ON p.filing_id = f.id
        WHERE f.totrevenue >= 10000000
          AND f.compnsatncurrofcr >= 50000
          AND o.state IS NOT NULL
        ORDER BY o.state
    """)
    states = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT LEFT(o.ntee_code, 1) AS ntee_major
        FROM organizations o
        JOIN filings f ON f.ein = o.ein
        WHERE f.totrevenue >= 10000000
          AND f.compnsatncurrofcr >= 50000
          AND o.ntee_code IS NOT NULL
        ORDER BY ntee_major
    """)
    ntees = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT f.tax_prd_yr
        FROM filings f
        JOIN personnel p ON p.filing_id = f.id
        WHERE f.totrevenue >= 10000000
          AND f.compnsatncurrofcr >= 50000
          AND f.tax_prd_yr IS NOT NULL
        ORDER BY f.tax_prd_yr DESC
    """)
    years = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT MIN(totrevenue), MAX(totrevenue)
        FROM filings
        WHERE totrevenue >= 10000000
          AND compnsatncurrofcr >= 50000
    """)
    rev_range = cur.fetchone()

    cur.close()
    conn.close()
    return jsonify({
        "states": states,
        "ntees": ntees,
        "years": years,
        "rev_min": rev_range[0],
        "rev_max": rev_range[1],
    })


# ── /api/orgs ─────────────────────────────────────────────────────────────────
# Returns filtered org list with latest-year summary per org
@app.route("/api/orgs")
def orgs():
    state    = request.args.get("state", "")
    ntee     = request.args.get("ntee", "")
    year     = request.args.get("year", "")
    search   = request.args.get("search", "")
    rev_min  = request.args.get("rev_min", 10000000, type=float)
    rev_max  = request.args.get("rev_max", None, type=float)

    conditions = [
        "f.totrevenue >= %(rev_min)s",
        "f.compnsatncurrofcr >= 50000",
        "f.tax_prd_yr = sub.max_yr",
    ]
    params = {"rev_min": rev_min}

    if rev_max:
        conditions.append("f.totrevenue <= %(rev_max)s")
        params["rev_max"] = rev_max
    if state:
        conditions.append("o.state = %(state)s")
        params["state"] = state
    if ntee:
        conditions.append("LEFT(o.ntee_code, 1) = %(ntee)s")
        params["ntee"] = ntee
    if year:
        conditions.append("f.tax_prd_yr = %(year)s")
        params["year"] = int(year)
    if search:
        conditions.append("o.name ILIKE %(search)s")
        params["search"] = f"%{search}%"

    where = " AND ".join(conditions)

    query = f"""
        SELECT
            o.ein,
            o.name                          AS org_name,
            o.city                          AS org_city,
            o.state                         AS org_state,
            o.ntee_code,
            f.tax_prd_yr                    AS latest_year,
            f.totrevenue,
            f.margin_pct,
            f.noemployees,
            f.compnsatncurrofcr             AS total_officer_comp
        FROM organizations o
        JOIN (
            SELECT ein, MAX(tax_prd_yr) AS max_yr
            FROM filings
            WHERE totrevenue >= %(rev_min)s
              AND compnsatncurrofcr >= 50000
            GROUP BY ein
        ) sub ON sub.ein = o.ein
        JOIN filings f ON f.ein = o.ein
        WHERE {where}
        ORDER BY f.totrevenue DESC
        LIMIT 2000
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([dict(r) for r in rows])


# ── /api/org/<ein> ────────────────────────────────────────────────────────────
# Returns full detail for one org: all filing years + personnel for selected year
@app.route("/api/org/<ein>")
def org_detail(ein):
    year = request.args.get("year", None, type=int)

    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # All filing summaries for this org (for year tabs + financial trend cards)
    cur.execute("""
        SELECT
            f.id                            AS filing_id,
            f.tax_prd_yr,
            f.form_type,
            f.totrevenue,
            f.totfuncexpns,
            f.surplus,
            f.margin_pct,
            f.totassetsend,
            f.netassetsend,
            f.noemployees,
            f.prgmservrev,
            f.program_rev_pct,
            f.compnsatncurrofcr             AS total_officer_comp_filing
        FROM filings f
        WHERE f.ein = %(ein)s
        ORDER BY f.tax_prd_yr DESC
    """, {"ein": ein})
    filings = [dict(r) for r in cur.fetchall()]

    if not filings:
        cur.close(); conn.close()
        return jsonify({"error": "Not found"}), 404

    # Use requested year or latest
    active_year = year if year else filings[0]["tax_prd_yr"]
    active_filing = next((f for f in filings if f["tax_prd_yr"] == active_year), filings[0])

    # Personnel for the active year with YoY comp calculations
    cur.execute("""
        WITH comp_with_lag AS (
            SELECT
                p.person_name,
                p.title,
                p.hours_per_week,
                p.is_officer,
                p.is_key_employee,
                p.compensation                                          AS base_comp,
                p.related_comp,
                p.other_comp,
                (p.compensation + COALESCE(p.related_comp, 0) + COALESCE(p.other_comp, 0))        AS total_comp,
                LAG(p.compensation + COALESCE(p.related_comp, 0) + COALESCE(p.other_comp, 0))
                    OVER (PARTITION BY p.ein, p.person_name
                          ORDER BY p.tax_prd_yr)                        AS prior_yr_total_comp,
                p.tax_prd_yr
            FROM personnel p
            WHERE p.ein = %(ein)s
        )
        SELECT
            person_name, title, hours_per_week, is_officer, is_key_employee,
            base_comp, related_comp, other_comp, total_comp, prior_yr_total_comp,
            (total_comp - prior_yr_total_comp)                          AS comp_change_dollars,
            CASE
                WHEN prior_yr_total_comp > 0
                THEN ROUND(((total_comp - prior_yr_total_comp)::numeric
                     / prior_yr_total_comp) * 100, 2)
                ELSE NULL
            END                                                         AS comp_change_pct
        FROM comp_with_lag
        WHERE tax_prd_yr = %(year)s
        ORDER BY total_comp DESC NULLS LAST
    """, {"ein": ein, "year": active_year})
    personnel = [dict(r) for r in cur.fetchall()]

    # Org info
    cur.execute("""
        SELECT ein, name AS org_name, city AS org_city, state AS org_state, ntee_code
        FROM organizations WHERE ein = %(ein)s
    """, {"ein": ein})
    org = dict(cur.fetchone())

    cur.close(); conn.close()
    return jsonify({
        "org": org,
        "filings": filings,
        "active_year": active_year,
        "active_filing": active_filing,
        "personnel": personnel,
    })


@app.route("/")
def index():
    return send_from_directory("/home/matthewroberts79", "nonprofit_explorer.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
