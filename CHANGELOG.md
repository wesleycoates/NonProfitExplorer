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
