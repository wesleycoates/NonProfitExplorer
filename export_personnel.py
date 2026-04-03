
import os, csv, psycopg2

conn = psycopg2.connect(os.environ['DATABASE_URL'])
cur = conn.cursor()

query = '''
WITH
comp_with_lag AS (
  SELECT
    p.id AS personnel_id,
    p.filing_id,
    p.ein,
    p.tax_prd_yr,
    p.person_name,
    p.title,
    p.hours_per_week,
    p.is_officer,
    p.is_key_employee,
    p.compensation,
    p.related_comp,
    p.other_comp,
    (p.compensation + p.related_comp + p.other_comp) AS total_comp,
    CASE WHEN (p.compensation + p.related_comp + p.other_comp) > 0
         THEN true ELSE false END AS is_paid,
    LAG(p.compensation + p.related_comp + p.other_comp)
      OVER (PARTITION BY p.ein, p.person_name ORDER BY p.tax_prd_yr) AS prior_yr_total_comp
  FROM personnel p
),
filings_with_lag AS (
  SELECT
    f.*,
    LAG(f.margin_pct)
      OVER (PARTITION BY f.ein ORDER BY f.tax_prd_yr) AS prior_yr_margin
  FROM filings f
)
SELECT
  o.ein,
  o.name AS org_name,
  o.city AS org_city,
  o.state AS org_state,
  o.ntee_code,
  f.id AS filing_id,
  f.tax_prd_yr,
  f.form_type,
  f.totrevenue,
  f.totfuncexpns,
  f.surplus,
  f.margin_pct,
  f.prior_yr_margin,
  f.totassetsend,
  f.netassetsend,
  f.noemployees,
  f.prgmservrev,
  f.program_rev_pct,
  f.compnsatncurrofcr AS total_officer_comp_filing,
  c.person_name,
  c.title,
  c.hours_per_week,
  c.is_officer,
  c.is_key_employee,
  c.is_paid,
  c.compensation AS base_comp,
  c.related_comp,
  c.other_comp,
  c.total_comp,
  c.prior_yr_total_comp,
  (c.total_comp - c.prior_yr_total_comp) AS comp_change_dollars,
  CASE
    WHEN c.prior_yr_total_comp > 0
    THEN ROUND(((c.total_comp - c.prior_yr_total_comp)::numeric / c.prior_yr_total_comp) * 100, 2)
    ELSE NULL
  END AS comp_change_pct
FROM comp_with_lag c
JOIN filings_with_lag f ON f.id = c.filing_id
JOIN organizations o ON o.ein = f.ein
WHERE
  f.totrevenue >= 10000000
  AND f.compnsatncurrofcr >= 50000
ORDER BY o.ein, f.tax_prd_yr DESC, c.total_comp DESC NULLS LAST
'''

print("Running query...")
cur.execute(query)
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

outfile = '/home/matthewroberts79/personnel_filtered.csv'
with open(outfile, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(cols)
    writer.writerows(rows)

print(f'Done. {len(rows)} rows written to {outfile}')
cur.close()
conn.close()
