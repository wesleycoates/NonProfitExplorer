-- =============================================================================
-- 990 Healthcare Nonprofit Database Schema
-- Target: PostgreSQL 15 (self-hosted on GCP e2-micro VM)
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- enables fuzzy/partial name search

-- =============================================================================
-- 1. ORGANIZATIONS
--    One row per EIN. Populated by pipeline_01_search_ingest.py
-- =============================================================================
CREATE TABLE IF NOT EXISTS organizations (
    ein                 VARCHAR(10)     PRIMARY KEY,
    name                TEXT            NOT NULL,
    name_normalized     TEXT            GENERATED ALWAYS AS (lower(trim(name))) STORED,
    city                VARCHAR(100),
    state               CHAR(2),
    zipcode             VARCHAR(10),
    ntee_code           VARCHAR(10),         -- e.g. 'E22' (Community Health Centers)
    ntee_major          SMALLINT,            -- 4 = Health
    subseccd            VARCHAR(5),          -- 501(c) type code, typically '3'
    ruling_date         VARCHAR(6),          -- YYYYMM format
    asset_cd            SMALLINT,            -- IRS asset size bucket (1–9)
    income_cd           SMALLINT,            -- IRS income size bucket (1–9)
    affiliation         SMALLINT,
    classification      VARCHAR(10),
    deductibility       SMALLINT,
    foundation          SMALLINT,
    activity            VARCHAR(10),
    organization        SMALLINT,
    status              SMALLINT,
    tax_period          VARCHAR(6),          -- most recent tax period on file
    acct_pd             SMALLINT,            -- accounting period month
    filing_req_cd       SMALLINT,
    sort_name           TEXT,
    first_seen_at       TIMESTAMPTZ     DEFAULT now(),
    last_refreshed_at   TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_org_state       ON organizations(state);
CREATE INDEX IF NOT EXISTS idx_org_ntee_major  ON organizations(ntee_major);
CREATE INDEX IF NOT EXISTS idx_org_subseccd    ON organizations(subseccd);
CREATE INDEX IF NOT EXISTS idx_org_name_trgm   ON organizations USING GIN (name_normalized gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_org_asset_cd    ON organizations(asset_cd);


-- =============================================================================
-- 2. FILINGS
--    One row per EIN × tax_period. Populated by pipeline_02_enrich_filings.py
--    Financial fields sourced from ProPublica's /organizations/{ein}/filings.json
-- =============================================================================
CREATE TABLE IF NOT EXISTS filings (
    id                      BIGSERIAL       PRIMARY KEY,
    ein                     VARCHAR(10)     NOT NULL REFERENCES organizations(ein) ON DELETE CASCADE,
    tax_prd                 VARCHAR(6)      NOT NULL,   -- YYYYMM end of tax period
    tax_prd_yr              SMALLINT        GENERATED ALWAYS AS (LEFT(tax_prd, 4)::SMALLINT) STORED,
    form_type               VARCHAR(10),                -- '990', '990EZ', '990PF'

    -- === Organization info (from filing, may differ from IRS master) ===
    org_name                TEXT,
    address                 TEXT,
    city                    VARCHAR(100),
    state                   CHAR(2),
    zipcode                 VARCHAR(10),

    -- === Revenue ===
    totrevenue              BIGINT,
    totfuncexpns            BIGINT,
    totassetsend            BIGINT,
    totliabend              BIGINT,
    netassetsend            BIGINT,
    prgmservrev             BIGINT,     -- program service revenue
    investinc               BIGINT,     -- investment income
    othrevnue               BIGINT,
    grscontribs             BIGINT,     -- gross contributions/grants
    fedgrnts                BIGINT,     -- federal grants

    -- === Expenses ===
    totprgmexpns            BIGINT,     -- total program service expenses
    totgrants               BIGINT,     -- total grants paid out
    totmgmtexpns            BIGINT,     -- management & general expenses
    totfndrsng              BIGINT,     -- fundraising expenses
    compnsatncurrofcr       BIGINT,     -- officer/director compensation
    othrsalwages            BIGINT,     -- other salaries & wages
    payrolltx               BIGINT,

    -- === Headcount ===
    noemployees             INTEGER,    -- total employees
    noemplyeesw2            INTEGER,    -- W-2 employees
    noindcontractor         INTEGER,    -- independent contractors

    -- === Computed columns (auto-calculated, read-only) ===
    surplus                 BIGINT      GENERATED ALWAYS AS (totrevenue - totfuncexpns) STORED,
    margin_pct              NUMERIC(6,2) GENERATED ALWAYS AS (
                                CASE WHEN totrevenue IS NOT NULL AND totrevenue <> 0
                                     THEN ROUND((totrevenue - totfuncexpns)::NUMERIC / totrevenue * 100, 2)
                                     ELSE NULL END
                            ) STORED,
    program_rev_pct         NUMERIC(6,2) GENERATED ALWAYS AS (
                                CASE WHEN totrevenue IS NOT NULL AND totrevenue <> 0
                                     THEN ROUND(COALESCE(prgmservrev, 0)::NUMERIC / totrevenue * 100, 2)
                                     ELSE NULL END
                            ) STORED,

    -- === Source metadata ===
    pdf_url                 TEXT,
    source_url              TEXT,       -- ProPublica API URL used
    raw_json                JSONB,      -- full ProPublica response for this filing
    ingested_at             TIMESTAMPTZ DEFAULT now(),

    UNIQUE (ein, tax_prd)
);

CREATE INDEX IF NOT EXISTS idx_filings_ein         ON filings(ein);
CREATE INDEX IF NOT EXISTS idx_filings_tax_prd_yr  ON filings(tax_prd_yr);
CREATE INDEX IF NOT EXISTS idx_filings_state       ON filings(state);
CREATE INDEX IF NOT EXISTS idx_filings_totrevenue  ON filings(totrevenue);
CREATE INDEX IF NOT EXISTS idx_filings_margin_pct  ON filings(margin_pct);
CREATE INDEX IF NOT EXISTS idx_filings_form_type   ON filings(form_type);


-- =============================================================================
-- 3. AI ANALYSES
--    One row per filing (or per EIN for summary). Populated by pipeline_03_ai_analyze.py
--    Store Claude's structured output so you never pay to re-analyze the same filing.
-- =============================================================================
CREATE TABLE IF NOT EXISTS ai_analyses (
    id                  BIGSERIAL       PRIMARY KEY,
    filing_id           BIGINT          UNIQUE REFERENCES filings(id) ON DELETE CASCADE,
    ein                 VARCHAR(10)     NOT NULL,
    tax_prd_yr          SMALLINT,
    model               VARCHAR(60)     NOT NULL,    -- e.g. 'claude-sonnet-4-20250514'
    insights            JSONB,          -- array of {tag, text} objects
    outlook             TEXT,           -- strategic outlook paragraph
    flags               JSONB,          -- array of unusual pattern strings
    raw_response        TEXT,           -- full model response (for debugging)
    tokens_in           INTEGER,
    tokens_out          INTEGER,
    cost_usd            NUMERIC(10,6),  -- computed from token usage
    analyzed_at         TIMESTAMPTZ     DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ai_ein         ON ai_analyses(ein);
CREATE INDEX IF NOT EXISTS idx_ai_tax_prd_yr  ON ai_analyses(tax_prd_yr);


-- =============================================================================
-- 4. PIPELINE RUNS
--    Audit log for each pipeline execution. Useful for resuming interrupted runs.
-- =============================================================================
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id              SERIAL          PRIMARY KEY,
    pipeline        VARCHAR(30)     NOT NULL,   -- '01_search', '02_enrich', '03_analyze'
    status          VARCHAR(20)     NOT NULL DEFAULT 'running',
    started_at      TIMESTAMPTZ     DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    orgs_processed  INTEGER         DEFAULT 0,
    orgs_errored    INTEGER         DEFAULT 0,
    notes           TEXT
);


-- =============================================================================
-- 5. CONVENIENCE VIEWS
-- =============================================================================

-- Latest filing per org with financial snapshot
CREATE OR REPLACE VIEW v_latest_filings AS
SELECT DISTINCT ON (f.ein)
    o.name,
    o.city,
    o.state,
    o.ntee_code,
    f.ein,
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
    a.insights,
    a.outlook,
    a.flags
FROM filings f
JOIN organizations o ON o.ein = f.ein
LEFT JOIN ai_analyses a ON a.filing_id = f.id
ORDER BY f.ein, f.tax_prd_yr DESC;

-- Year-over-year revenue trend (last 5 years per org)
CREATE OR REPLACE VIEW v_revenue_trend AS
SELECT
    f.ein,
    o.name,
    o.state,
    f.tax_prd_yr,
    f.totrevenue,
    f.totfuncexpns,
    f.surplus,
    f.margin_pct,
    LAG(f.totrevenue) OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr) AS prev_yr_revenue,
    CASE
        WHEN LAG(f.totrevenue) OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr) > 0
        THEN ROUND(
            (f.totrevenue - LAG(f.totrevenue) OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr))::NUMERIC
            / LAG(f.totrevenue) OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr) * 100, 1
        )
        ELSE NULL
    END AS revenue_growth_pct
FROM filings f
JOIN organizations o ON o.ein = f.ein
WHERE f.tax_prd_yr >= EXTRACT(YEAR FROM now())::SMALLINT - 5;
