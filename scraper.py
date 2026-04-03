import requests
from bs4 import BeautifulSoup
import re, csv, json, sys, time, os, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
BASE = 'https://usppa.org'
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(ROOT_DIR, 'public', 'data')

# Known field labels mapped to clean column names
FIELD_MAP = {
    'ppg type': 'PPG_Type',
    'type of injury': 'Type_of_Injury',
    'age': 'Age',
    'weight': 'Weight',
    'gender': 'Gender',
    'highest rating held': 'Highest_Rating',
    'highest rating': 'Highest_Rating',
    'pilot experience level': 'Pilot_Experience',
    'pilot experience': 'Pilot_Experience',
    'experience level': 'Pilot_Experience',
    'wing brand': 'Wing_Brand',
    'wing make': 'Wing_Brand',
    'model': 'Wing_Model',
    'wing model': 'Wing_Model',
    'size': 'Wing_Size',
    'wing size': 'Wing_Size',
    'paramotor frame': 'Paramotor_Frame',
    'paramotor': 'Paramotor_Frame',
    'frame': 'Paramotor_Frame',
    'motor': 'Paramotor_Frame',
    'location of the incident': 'Location',
    'location': 'Location',
    'incident location': 'Location',
    'type of incident': 'Type_of_Incident',
    'incident type': 'Type_of_Incident',
    'flight window': 'Flight_Window',
    'wind speed': 'Wind_Speed',
    'wind': 'Wind_Speed',
    'phase of flight': 'Phase_of_Flight',
    'flight phase': 'Phase_of_Flight',
    'collateral damage': 'Collateral_Damage',
    'analysis of the incident': 'Analysis',
    'analysis': 'Analysis',
    'incident analysis': 'Analysis',
    'description': 'Description',
    'incident description': 'Description',
    'narrative': 'Description',
    'video': 'Video_URL',
    'video url': 'Video_URL',
    'photos': 'Photos_URL',
    'photo': 'Photos_URL',
    'other': 'Other',
}

COLS = ['Entry_ID', 'Incident_Date', 'Incident_DateTime', 'Location', 'PPG_Type',
        'Type_of_Incident', 'Description', 'Type_of_Injury', 'Phase_of_Flight',
        'Flight_Window', 'Wind_Speed', 'Age', 'Weight', 'Gender',
        'Highest_Rating', 'Pilot_Experience', 'Wing_Brand', 'Wing_Model',
        'Wing_Size', 'Paramotor_Frame', 'Collateral_Damage', 'Analysis',
        'Video_URL', 'Photos_URL', 'Other', 'Raw_Text', 'URL']


def fetch(url):
    r = requests.get(url, headers=HEADERS, timeout=30)
    return r.text


def get_entry_urls():
    urls = []
    for page in range(1, 100):
        print(f'Listing page {page}...', file=sys.stderr)
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
        return {'URL': url, 'Entry_ID': url.split('/entry/')[-1].rstrip('/'),
                'Raw_Text': '', 'Error': str(e)}

    soup = BeautifulSoup(html, 'html.parser')

    # Try multiple content selectors
    content = None
    for sel in ['.zn_text_box', '.entry-content', '.post-content', 'article', 'main']:
        content = soup.select_one(sel)
        if content:
            break

    if not content:
        return {'URL': url, 'Entry_ID': url.split('/entry/')[-1].rstrip('/'),
                'Raw_Text': '', 'Error': 'No content'}

    text = content.get_text(separator='\n', strip=True)
    rec = {col: '' for col in COLS}
    rec['URL'] = url
    rec['Entry_ID'] = url.split('/entry/')[-1].rstrip('/')
    rec['Raw_Text'] = text

    # Extract date/time
    dt_match = re.search(r'(\w+ \d{1,2},?\s*\d{4}\s+\d{1,2}:\d{2}\s*[APMapm]{2})', text)
    if dt_match:
        rec['Incident_DateTime'] = dt_match.group(1).strip()
    d_match = re.search(r'(\w+ \d{1,2},?\s*\d{4})', text)
    if d_match:
        rec['Incident_Date'] = d_match.group(1).strip()

    # Generic key:value extraction — handles any "Label : Value" format
    # Split text into lines and look for "label : value" patterns
    lines = text.split('\n')
    for i, line in enumerate(lines):
        # Match "Some Label : some value" or "Some Label: some value"
        m = re.match(r'^(.+?)\s*:\s*(.+)$', line.strip())
        if m:
            label = m.group(1).strip().rstrip('.')
            value = m.group(2).strip()
            # Look up the label in our field map (case-insensitive)
            label_lower = label.lower()
            for known_label, col_name in FIELD_MAP.items():
                if known_label in label_lower or label_lower in known_label:
                    if not rec[col_name]:  # don't overwrite if already found
                        rec[col_name] = value
                    break

    # Also try multi-line: label on one line, value on next
    for i, line in enumerate(lines):
        label_lower = line.strip().rstrip(':').lower()
        for known_label, col_name in FIELD_MAP.items():
            if label_lower == known_label and i + 1 < len(lines):
                value = lines[i + 1].strip()
                if value and not rec[col_name]:
                    rec[col_name] = value
                break

    # Description from bold text if not already found
    if not rec['Description']:
        for b in content.select('strong, b'):
            t = b.get_text(strip=True)
            if len(t) > 30:
                label_like = False
                for known in FIELD_MAP:
                    if known in t.lower():
                        label_like = True
                        break
                if not label_like:
                    rec['Description'] = t
                    break

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
        'phase': phase, 'done': done, 'total': total, 'message': message,
        'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'started_at': started_at, 'elapsed_seconds': elapsed_seconds,
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
            if f.startswith('acro_'):
                ftype = 'acro'
            elif f.startswith('usppa_incidents_all'):
                ftype = 'combined'
            else:
                ftype = 'batch'
            files.append({
                'name': f,
                'size': os.path.getsize(filepath),
                'type': ftype,
            })
    manifest = {'files': files, 'batch_count': batch_count}
    with open(os.path.join(OUTPUT_DIR, 'manifest.json'), 'w') as mf:
        json.dump(manifest, mf, indent=2)


def git_push_data():
    """Commit and push all data files to the repo when scrape is complete."""
    print('\nPushing data to repo...', file=sys.stderr)
    try:
        subprocess.run(['git', 'config', 'user.name', 'usppa-scraper'], capture_output=True, cwd=ROOT_DIR)
        subprocess.run(['git', 'config', 'user.email', 'scraper@localhost'], capture_output=True, cwd=ROOT_DIR)
        subprocess.run(['git', 'add', '-f', 'public/data/'], check=True, capture_output=True, cwd=ROOT_DIR)
        result = subprocess.run(['git', 'diff', '--staged', '--quiet'], capture_output=True, cwd=ROOT_DIR)
        if result.returncode != 0:
            subprocess.run(['git', 'commit', '-m', 'Update incident data [automated]'],
                           check=True, capture_output=True, cwd=ROOT_DIR)
            subprocess.run(['git', 'push'], check=True, capture_output=True, cwd=ROOT_DIR)
            print('Data pushed to repo successfully.', file=sys.stderr)
        else:
            print('No changes to push.', file=sys.stderr)
    except Exception as e:
        print(f'Git push failed: {e}', file=sys.stderr)
        print('Data is still saved locally in public/data/', file=sys.stderr)


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

            sys.stderr.write(progress_bar(done, total))
            sys.stderr.flush()

            elapsed = int(time.time() - start)
            pct = (done * 100) // total if total else 0
            save_status('scraping', done=done, total=total,
                        message=f'Scraped {done}/{total} incidents ({pct}%)',
                        elapsed_seconds=elapsed, started_at=started_at)

            if len(records) >= 100:
                batch_num += 1
                fname = save_batch_csv(records, batch_num)
                save_manifest(batch_num)
                sys.stderr.write(f'\n  Saved batch {batch_num}: {fname}\n')
                records = []

    if records:
        batch_num += 1
        save_batch_csv(records, batch_num)

    save_combined(all_records)
    save_manifest(batch_num)

    elapsed = int(time.time() - start)
    save_status('complete', done=len(all_records), total=len(all_records),
                message=f'Done! {len(all_records)} incidents in {batch_num} batches.',
                elapsed_seconds=elapsed, started_at=started_at)

    print(f'\n\nDone! {len(all_records)} records in {batch_num} batches. ({elapsed}s)', file=sys.stderr)

    # Run acro filter
    print('\nFiltering acro incidents...', file=sys.stderr)
    try:
        from acro_filter import main as run_acro_filter
        run_acro_filter()
    except Exception as e:
        print(f'Acro filter error: {e}', file=sys.stderr)

    # Rebuild manifest to include acro CSV
    save_manifest(batch_num)

    # Auto-push to repo
    git_push_data()


if __name__ == '__main__':
    run_scrape()
