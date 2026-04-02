# NonProfitExplorer Changelog

## 2026-04-01

### Schema Changes
- Added 11 new columns to `filings` table extracted from existing `raw_json` blob:
  - `royaltsinc`, `miscrevtot`, `grsincfndrsng`, `netincfndrsng`
  - `profndraising`, `grsincgaming`, `netincgaming`, `netrntlinc`
  - `txexmptbndsend`, `unrelbusinc`, `nonpfrea`
- Updated `v_latest_filings` view to filter `totrevenue >= 10000000`
  and `margin_pct BETWEEN -100 AND 100` to exclude noise rows

### Data
- 462,846 filings updated with extracted JSON fields
- Working dataset: ~8,366 orgs with 10M+ revenue and valid margins

## 2026-04-02

### Pipeline 05 — Personnel / Officer Compensation
- Added `pipeline_05_personnel.py` — downloads IRS 990 XML zip files,
  parses Part VII officer/director compensation, inserts to `personnel` table
- Streams zips to disk (not RAM) to handle memory constraints on e2-micro
- Skips zips over 400MB, commits every 500 matches for crash resilience
- Result: 81,901 personnel records across 6,862 orgs from 2023 IRS filings
