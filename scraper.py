import requests
from bs4 import BeautifulSoup
import re, csv, json, sys, time, os
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
BASE = 'https://usppa.org'
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public', 'data')

COLS = ['Entry_ID','Incident_Date','Incident_DateTime','Location','PPG_Type','Type_of_Incident',
        'Description','Type_of_Injury','Phase_of_Flight','Flight_Window','Wind_Speed',
        'Age','Weight','Gender','Highest_Rating','Pilot_Experience',
        'Wing_Brand','Wing_Model','Wing_Size','Paramotor_Frame',
        'Collateral_Damage','Analysis','Video_URL','URL']


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    return r.text


def get_entry_urls():
    urls = []
    for page in range(1, 30):
        print(f'Listing page {page}…', file=sys.stderr)
        html = fetch(f'{BASE}/incidents/?frm-page-7672={page}')
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('a[href*="/incidents/entry/"]')
        if not links:
            break
        for a in links:
            href = a.get('href', '')
            full = href if href.startswith('http') else BASE + href
            if full not in urls:
                urls.append(full)
        print(f'  +{len(links)} entries, total: {len(urls)}', file=sys.stderr)
        time.sleep(0.3)
    return urls


def parse_entry(url):
    try:
        html = fetch(url)
    except Exception as e:
        return {'URL': url, 'Entry_ID': url.split('/entry/')[-1].rstrip('/'), 'Error': str(e)}

    soup = BeautifulSoup(html, 'html.parser')
    content = soup.select_one('.zn_text_box')
    if not content:
        return {'URL': url, 'Entry_ID': url.split('/entry/')[-1].rstrip('/'), 'Error': 'No content'}

    text = content.get_text(separator='\n', strip=True)
    rec = {'URL': url, 'Entry_ID': url.split('/entry/')[-1].rstrip('/')}

    patterns = [
        ('PPG_Type', r'PPG Type\s*:\s*(.+?)(?=\n|Type of Injury|$)'),
        ('Type_of_Injury', r'Type of Injury\s*:\s*(.+?)(?=\n|Age|$)'),
        ('Age', r'Age\s*:\s*(\d+)'),
        ('Weight', r'Weight\s*:\s*(\d+)'),
        ('Gender', r'Gender\s*:\s*(\w+)'),
        ('Highest_Rating', r'Highest rating held.*?:\s*(.+?)(?=\n|Pilot experience|$)'),
        ('Pilot_Experience', r'Pilot experience level\s*:\s*(.+?)(?=\n|Wing Brand|$)'),
        ('Wing_Brand', r'Wing Brand\s*:\s*(.+?)(?=\n|Model|$)'),
        ('Wing_Model', r'(?<!\w)Model\s*:\s*(.+?)(?=\n|Size|$)'),
        ('Wing_Size', r'Size\s*:\s*(.+?)(?=\n|Paramotor|$)'),
        ('Paramotor_Frame', r'Paramotor Frame\s*:\s*(.+?)(?=\n|$)'),
        ('Incident_DateTime', r'(\w+ \d{1,2}, \d{4}\s+\d{1,2}:\d{2}\s*[APM]{2})'),
        ('Incident_Date', r'(\w+ \d{1,2}, \d{4})'),
        ('Location', r'Location of the incident:\s*(.+?)(?=\n|Type of Incident|$)'),
        ('Type_of_Incident', r'Type of Incident\s*:\s*(.+?)(?=\n|$)'),
        ('Flight_Window', r'Flight Window\s*:\s*(.+?)(?=\n|Wind Speed|$)'),
        ('Wind_Speed', r'Wind Speed\s*:\s*(.+?)(?=\n|Type|$)'),
        ('Phase_of_Flight', r'Phase of Flight\s*:\s*(.+?)(?=\n|Type of Injury|Collateral|$)'),
        ('Collateral_Damage', r'Collateral Damage\s*:\s*(.+?)(?=\n|Analysis|$)'),
        ('Analysis', r'Analysis of the incident.*?:\s*(.+?)(?=\n|Photos|Video|Other|$)'),
    ]

    for name, pat in patterns:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            val = next((g for g in m.groups() if g is not None), '')
            rec[name] = val.strip()
        else:
            rec[name] = ''

    # Description
    bolds = content.select('strong')
    for b in bolds:
        t = b.get_text(strip=True)
        if len(t) > 30 and not any(k in t for k in ['PPG Type', 'Type of Injury', 'Age', 'Weight', 'Gender', 'Wing Brand', 'Flight Window', 'Wind Speed', 'Phase of Flight', 'Collateral', 'Analysis', 'Highest rating', 'Pilot experience', 'Location of']):
            rec['Description'] = t
            break
    if 'Description' not in rec:
        rec['Description'] = ''

    # Video
    vm = re.search(r'Video.*?:\s*(https?://\S+)', text)
    rec['Video_URL'] = vm.group(1) if vm else ''

    return rec


def progress_bar(current, total, width=40):
    pct = current / total if total else 0
    filled = int(width * pct)
    bar = '\u2588' * filled + '\u2591' * (width - filled)
    return f'\r  [{bar}] {current}/{total} ({pct:.0%})'


def save_batch_csv(records, batch_num):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f'incidents_batch_{batch_num:03d}.csv'
    filepath = os.path.join(OUTPUT_DIR, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(records)
    return filename


def save_combined(records):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    records.sort(key=lambda r: int(r.get('Entry_ID', '0') or '0'), reverse=True)

    csv_path = os.path.join(OUTPUT_DIR, 'usppa_incidents_all.csv')
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(records)

    json_path = os.path.join(OUTPUT_DIR, 'usppa_incidents_all.json')
    with open(json_path, 'w') as f:
        json.dump(records, f, indent=2)

    return csv_path, json_path


def save_status(phase, done=0, total=0, message='', elapsed_seconds=0, started_at=''):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    status = {
        'phase': phase,
        'done': done,
        'total': total,
        'message': message,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'started_at': started_at,
        'elapsed_seconds': elapsed_seconds,
    }
    with open(os.path.join(OUTPUT_DIR, 'status.json'), 'w') as f:
        json.dump(status, f, indent=2)


def save_manifest(batch_count):
    files = []
    for f in sorted(os.listdir(OUTPUT_DIR)):
        if f in ('manifest.json', 'status.json'):
            continue
        filepath = os.path.join(OUTPUT_DIR, f)
        if os.path.isfile(filepath):
            files.append({
                'name': f,
                'size': os.path.getsize(filepath),
                'type': 'combined' if f.startswith('usppa_incidents_all') else 'batch',
            })
    manifest = {'files': files, 'batch_count': batch_count}
    with open(os.path.join(OUTPUT_DIR, 'manifest.json'), 'w') as mf:
        json.dump(manifest, mf, indent=2)


def run_scrape():
    """Run the full scrape. Writes status.json throughout for live progress."""
    start = time.time()
    started_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())

    save_status('listing', message='Discovering incident pages...', started_at=started_at)

    entry_urls = get_entry_urls()
    total = len(entry_urls)
    print(f'\nFound {total} entries. Scraping with 10 threads...\n', file=sys.stderr)

    save_status('scraping', done=0, total=total,
                message=f'Scraping {total} incidents...', started_at=started_at)

    records = []
    all_records = []
    done = 0
    batch_num = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(parse_entry, url): url for url in entry_urls}
        for f in as_completed(futures):
            done += 1
            rec = f.result()
            if rec:
                records.append(rec)
                all_records.append(rec)

            # Terminal progress bar
            sys.stderr.write(progress_bar(done, total))
            sys.stderr.flush()

            # Update status.json every entry (instant local reads)
            elapsed = int(time.time() - start)
            pct = (done * 100) // total if total else 0
            save_status('scraping', done=done, total=total,
                        message=f'Scraped {done}/{total} incidents ({pct}%)',
                        elapsed_seconds=elapsed, started_at=started_at)

            # Save batch CSV every 100
            if len(records) >= 100:
                batch_num += 1
                fname = save_batch_csv(records, batch_num)
                save_manifest(batch_num)
                sys.stderr.write(f'\n  Saved batch {batch_num}: {fname}\n')
                records = []

    # Final batch
    if records:
        batch_num += 1
        save_batch_csv(records, batch_num)

    # Combined files
    save_combined(all_records)
    save_manifest(batch_num)

    elapsed = int(time.time() - start)
    save_status('complete', done=len(all_records), total=len(all_records),
                message=f'Done! {len(all_records)} incidents in {batch_num} batches.',
                elapsed_seconds=elapsed, started_at=started_at)

    print(f'\n\nDone! {len(all_records)} records in {batch_num} batches. ({elapsed}s)', file=sys.stderr)


if __name__ == '__main__':
    run_scrape()
