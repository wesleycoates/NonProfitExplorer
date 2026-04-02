import psycopg2
import requests
import zipfile
import os
import logging
from xml.etree import ElementTree as ET

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline_05.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL  = os.environ.get("DATABASE_URL")
IRS_BASE      = "https://apps.irs.gov/pub/epostcard/990/xml"
NS            = {'irs': 'http://www.irs.gov/efile'}
TEST_MODE     = False
YEARS         = [2023]
ZIP_SUFFIXES  = ['01A','02A','03A','04A','05A','06A','07A','08A','09A','10A']
TMP_ZIP_PATH  = '/tmp/irs_990_current.zip'
MAX_ZIP_MB    = 400   # skip zips larger than this
COMMIT_EVERY  = 500   # commit to DB after this many matched orgs

# ── Helpers ───────────────────────────────────────────────────────────────────
def normalize_ein(raw):
    try:
        return str(int(str(raw).replace('-', ''))).zfill(9)
    except:
        return None

def download_zip(url, dest_path):
    """Stream zip to disk in chunks to avoid RAM exhaustion."""
    with requests.get(url, timeout=180, stream=True) as r:
        if r.status_code != 200:
            return False
        with open(dest_path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    size_mb = os.path.getsize(dest_path) / 1024 / 1024
    log.info(f"  Saved {size_mb:.1f} MB to disk")
    return True

def get_return_ein(root):
    el = root.find('.//irs:Filer/irs:EIN', NS)
    return normalize_ein(el.text) if el is not None else None

def get_tax_year(root):
    el = root.find('.//irs:TaxPeriodEndDt', NS)
    return int(el.text[:4]) if (el is not None and el.text) else None

def parse_officers(root):
    people = []
    for grp in root.findall('.//irs:Form990PartVIISectionAGrp', NS):
        def val(tag):
            el = grp.find(f'irs:{tag}', NS)
            return el.text if el is not None else None
        name = val('PersonNm')
        if not name:
            continue
        people.append({
            'person_name':  name.strip(),
            'title':        (val('TitleTxt') or '').strip(),
            'hours':        float(val('AverageHoursPerWeekRt') or 0),
            'compensation': int(val('ReportableCompFromOrgAmt') or 0),
            'related_comp': int(val('ReportableCompFromRltdOrgAmt') or 0),
            'other_comp':   int(val('OtherCompensationAmt') or 0),
            'is_officer':   val('OfficerInd') == 'X',
            'is_key_emp':   val('KeyEmployeeInd') == 'X',
        })
    return people

def get_filing_id(cur, ein, tax_year):
    cur.execute("""
        SELECT id FROM filings
        WHERE ein = %s AND tax_prd_yr = %s
        LIMIT 1
    """, (ein, tax_year))
    row = cur.fetchone()
    return row[0] if row else None

def insert_people(cur, filing_id, ein, tax_year, people):
    inserted = 0
    for p in people:
        cur.execute("""
            INSERT INTO personnel
              (filing_id, ein, tax_prd_yr, person_name, title,
               compensation, related_comp, other_comp,
               hours_per_week, is_officer, is_key_employee)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            filing_id, ein, tax_year,
            p['person_name'], p['title'],
            p['compensation'], p['related_comp'], p['other_comp'],
            p['hours'], p['is_officer'], p['is_key_emp']
        ))
        inserted += 1
    return inserted

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur  = conn.cursor()

    # Load target EINs
    cur.execute("SELECT DISTINCT ein FROM organizations")
    target_eins = set()
    for (e,) in cur.fetchall():
        n = normalize_ein(e)
        if n:
            target_eins.add(n)
    log.info(f"Target EINs loaded: {len(target_eins)}")

    # Track already-processed EINs for resume support
    cur.execute("SELECT DISTINCT ein FROM personnel")
    done_eins = set()
    for (e,) in cur.fetchall():
        n = normalize_ein(e)
        if n:
            done_eins.add(n)
    log.info(f"Already have personnel for {len(done_eins)} EINs — will skip these")

    total_inserted = 0
    total_matched  = 0

    for year in YEARS:
        log.info(f"=== Processing year {year} ===")

        for suffix in ZIP_SUFFIXES:
            url = f"{IRS_BASE}/{year}/{year}_TEOS_XML_{suffix}.zip"

            # Check zip exists
            head = requests.head(url, timeout=15)
            if head.status_code != 200:
                log.info(f"  Zip {suffix} not found, skipping")
                continue

            # Stream to disk
            log.info(f"Downloading {url} ...")
            ok = download_zip(url, TMP_ZIP_PATH)
            if not ok:
                log.warning(f"  Failed to download {suffix}, skipping")
                continue

            # Skip oversized zips
            size_mb = os.path.getsize(TMP_ZIP_PATH) / 1024 / 1024
            if size_mb > MAX_ZIP_MB:
                log.warning(f"  Zip too large ({size_mb:.0f} MB > {MAX_ZIP_MB} MB limit), skipping")
                os.remove(TMP_ZIP_PATH)
                continue

            matched = skipped = inserted = commit_count = 0

            with zipfile.ZipFile(TMP_ZIP_PATH, 'r') as z:
                files = z.namelist()
                log.info(f"  {len(files)} XML files in zip")

                for fname in files:
                    try:
                        xml  = z.read(fname).decode('utf-8')
                        root = ET.fromstring(xml)

                        ein = get_return_ein(root)
                        if not ein or ein not in target_eins:
                            skipped += 1
                            xml = root = None
                            continue

                        # Skip if we already have this EIN's personnel
                        if ein in done_eins:
                            skipped += 1
                            xml = root = None
                            continue

                        tax_year = get_tax_year(root)
                        if not tax_year:
                            skipped += 1
                            xml = root = None
                            continue

                        people = parse_officers(root)
                        xml = root = None  # free memory immediately

                        if not people:
                            skipped += 1
                            continue

                        filing_id = get_filing_id(cur, ein, tax_year)
                        if not filing_id:
                            skipped += 1
                            continue

                        n = insert_people(cur, filing_id, ein, tax_year, people)
                        inserted += n
                        matched  += 1
                        commit_count += 1

                        # Periodic commit to avoid large transactions
                        if commit_count >= COMMIT_EVERY:
                            conn.commit()
                            log.info(f"  Checkpoint commit: {inserted} inserted so far")
                            commit_count = 0

                    except Exception as e:
                        log.warning(f"Error in {fname}: {e}")
                        continue

            # Clean up zip immediately
            os.remove(TMP_ZIP_PATH)
            conn.commit()
            total_inserted += inserted
            total_matched  += matched
            log.info(f"  Done. Matched: {matched} | Inserted: {inserted} | Skipped: {skipped}")

    cur.close()
    conn.close()
    log.info(f"=== DONE === Total matched: {total_matched} | Total inserted: {total_inserted}")

if __name__ == "__main__":
    main()
