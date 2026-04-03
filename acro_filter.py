"""
Post-scrape filter: extract acro-related incidents from scraped data
and output in Google Forms format with USPPA incident link.

Run after scraper: python acro_filter.py
Or auto-runs after scraper completes.
"""
import csv, json, re, os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'public', 'data')
# Check both locations for the JSON
INPUT_JSON_DATA = os.path.join(DATA_DIR, 'usppa_incidents_all.json')
INPUT_JSON_ROOT = os.path.join(ROOT_DIR, 'usppa_incidents_all.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'acro_incidents.csv')

# --- Acro keywords found in actual USPPA reports ---
# These are the real terms used by pilots in incident reports
ACRO_KEYWORDS = [
    # Specific acro maneuver names
    r'\bsat\b',
    r'\bwing\s*over', r'\bwingover',
    r'\bbarrel\s*roll',
    r'\bjoker\b',
    r'\bspiral\s*dive',
    r'\bloop(?:ing|s)?\b',
    r'\brocket\s*loop',
    r'\btumbl',
    r'\bhelico',
    r'\bmisty',
    r'\bmctwist', r'\bmc\s*twist',
    r'\besfera',
    r'\bcorkscrew',
    r'\brhythmic',
    r'\binfinite',
    r'\bflat\s*spin',
    r'\bfull\s*stall',
    r'\bdeep\s*stall',
    r'\bsuper\s*stall',
    r'\basymmetric\s*spiral',
    r'\bdynamic\s*(?:full\s*)?stall',
    # Acro / aerobatics / trick terms
    r'\bacro\b', r'\bacrobat',
    r'\baerobat',
    r'\btrick(?:s|ing)?\b',
    r'\bstunt(?:s|ing)?\b',
    # Aggressive / show-off flying
    r'\blow\s+(?:maneuver|aerobat|pass|flyby|fly[\s-]*by)',
    r'\blow\s+maneuvering',
    r'\bshow\s*(?:ing)?\s*off',
    r'\bhot\s*dog',
    r'\bhotdog',
    r'\bdisplay\s+(?:the\s+)?trick',
    r'\bperform(?:ing|ed)?\s+(?:a\s+)?(?:trick|maneuver|stunt)',
    # Smoke + maneuver context (airshow display)
    r'\bsmoke\b.*\bwingover', r'\bwingover.*\bsmoke\b',
    r'\bsmoke\b.*\blow\b', r'\blow\b.*\bsmoke\b.*\bmaneuver',
    # Intentional stall/spin (not accidental)
    r'\bstall(?:ing|ed)?\s+the\s+(?:glider|wing|canopy)',
    r'\bintentional(?:ly)?\s+(?:stall|spin|spiral)',
    r'\bpracticing\b.*\b(?:stall|spin|spiral|wingover|maneuver)',
    r'\battempt(?:ing|ed)?\b.*\b(?:stall|spin|spiral|wingover|maneuver|trick|acro)',
    # Reserve during maneuver
    r'\breserve\b.*\b(?:maneuver|acro|trick|stunt|wingover|spiral|sat\b)',
    r'\b(?:maneuver|acro|trick|stunt|wingover|spiral|sat)\b.*\breserve\b',
    # Collapse during intentional maneuver
    r'\bcollapse\b.*\b(?:wingover|maneuver|trick|acro)',
    r'\b(?:wingover|maneuver|trick|acro).*\bcollapse\b',
]

# --- Maneuver classification based on real USPPA terminology ---
MANEUVERS = {
    'wing overs': [r'\bwing\s*over', r'\bwingover'],
    'barrel roll': [r'\bbarrel\s*roll'],
    'spiral': [r'\bspiral'],
    'stall': [r'\bstall(?:ing|ed)?\b', r'\bfull\s*stall', r'\bdeep\s*stall'],
    'spin': [r'\bspin\b', r'\bflat\s*spin'],
    'infinite': [r'\binfinite', r'\btumbl'],
    'SAT': [r'\bsat\b'],
    'joker': [r'\bjoker\b'],
    'helico': [r'\bhelico'],
    'misty flip': [r'\bmisty'],
    'looping': [r'\bloop', r'\brocket\s*loop'],
    'single turn into ground': [r'\bsingle\s*turn', r'\bturn.*ground'],
}

# --- Low vs High acro detection based on real report language ---
LOW_INDICATORS = [
    r'\blow\s+(?:aerobat|maneuver|altitude|pass|fly)',
    r'\blow\s+maneuvering',
    r'\blow\b.*\bwingover', r'\bwingover.*\blow\b',
    r'\btreetop', r'\btree\s*top',
    r'\b(?:50|100|150|200)\s*(?:feet|foot|ft)\b',
    r'\b\d+\s*(?:feet|foot|ft)\s*AGL\b',
    r'\bnear\s*(?:the\s*)?ground',
    r'\bclose\s*to\s*(?:the\s*)?ground',
    r'\btoo\s+low',
    r'\binsufficient\s+alt',
    r'\blow\b.*\bsmoke\b', r'\bsmoke\b.*\blow\b',
    r'\blow\b.*\bcollapse', r'\bcollapse.*\blow\b',
]

HIGH_INDICATORS = [
    r'\bhigh\s+(?:alt|altitude)\b',
    r'\bover\s*water\b',
    r'\b(?:1000|1500|2000|3000)\s*(?:feet|foot|ft)',
    r'\bplenty\s+of\s+(?:alt|height)',
    r'\bsafe\s+alt',
]

ACRO_FORM_COLS = [
    'Date', 'Location', 'Pilot_Name', 'Glider_Make_Model', 'Injury',
    'Fatal', 'Experience_Level', 'Description', 'Low_or_High',
    'Maneuver', 'Lesson_Learned', 'Motor', 'USPPA_Link',
]


def get_all_text(rec):
    """Combine all text fields for searching."""
    fields = ['Raw_Text', 'Description', 'Type_of_Incident', 'Analysis',
              'Phase_of_Flight', 'Collateral_Damage', 'Other']
    return ' '.join(rec.get(f, '') or '' for f in fields)


def is_acro_incident(rec):
    """Check if incident matches any acro keyword."""
    text = get_all_text(rec)
    matched_terms = []
    for pattern in ACRO_KEYWORDS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            matched_terms.append(m.group(0))
    return bool(matched_terms), matched_terms


def detect_maneuver(text):
    found = []
    for name, patterns in MANEUVERS.items():
        for p in patterns:
            if re.search(p, text, re.IGNORECASE):
                found.append(name)
                break
    return ', '.join(found) if found else ''


def detect_low_or_high(text):
    low = any(re.search(p, text, re.IGNORECASE) for p in LOW_INDICATORS)
    high = any(re.search(p, text, re.IGNORECASE) for p in HIGH_INDICATORS)
    if low and not high:
        return 'Low'
    if high and not low:
        return 'High'
    if low and high:
        return 'Low'  # flag conservatively
    return ''


def detect_injury(rec):
    text = (rec.get('Type_of_Injury', '') + ' ' + rec.get('Raw_Text', '')).lower()
    if any(w in text for w in ['fatal', 'death', 'died', 'killed', 'deceased', 'perished']):
        return 'Yes', 'Yes'
    if any(w in text for w in ['no injury', 'none', 'no injuries', 'uninjured', 'walked away']):
        return 'No', 'No'
    if any(w in text for w in ['injury', 'injured', 'broken', 'broke', 'fracture', 'hospital',
                                'concussion', 'laceration', 'sprain', 'burn', 'surgery']):
        return 'Yes', 'No'
    return '', 'No'


def detect_motor(rec):
    text = get_all_text(rec).lower()
    if any(w in text for w in ['paramotor', 'ppg', 'powered paraglid', 'motor', 'prop',
                                'engine', 'throttle', 'frame']):
        return 'Motor'
    if any(w in text for w in ['free flight', 'free fly', 'paraglid', ' pg ']):
        return 'No Motor'
    return 'Motor'  # USPPA default


def convert_to_form(rec, matched_terms):
    text = get_all_text(rec)
    injury, fatal = detect_injury(rec)

    glider_parts = [rec.get('Wing_Brand', ''), rec.get('Wing_Model', '')]
    glider = ' '.join(p for p in glider_parts if p).strip()

    desc = rec.get('Analysis', '') or rec.get('Description', '') or rec.get('Raw_Text', '')[:500]

    return {
        'Date': rec.get('Incident_Date', '') or rec.get('Incident_DateTime', ''),
        'Location': rec.get('Location', ''),
        'Pilot_Name': '',
        'Glider_Make_Model': glider,
        'Injury': injury,
        'Fatal': fatal,
        'Experience_Level': rec.get('Pilot_Experience', '') or rec.get('Highest_Rating', ''),
        'Description': desc,
        'Low_or_High': detect_low_or_high(text),
        'Maneuver': detect_maneuver(text),
        'Lesson_Learned': '',
        'Motor': detect_motor(rec),
        'USPPA_Link': rec.get('URL', ''),
    }


def main():
    # Find the JSON file
    input_path = None
    for path in [INPUT_JSON_DATA, INPUT_JSON_ROOT]:
        if os.path.exists(path):
            input_path = path
            break

    if not input_path:
        print(f'No data found. Checked:')
        print(f'  {INPUT_JSON_DATA}')
        print(f'  {INPUT_JSON_ROOT}')
        print('Run the scraper first: python server.py')
        return

    with open(input_path, 'r') as f:
        records = json.load(f)

    print(f'Loaded {len(records)} total incidents from {input_path}')

    acro_records = []
    for rec in records:
        is_acro, matched_terms = is_acro_incident(rec)
        if is_acro:
            form_rec = convert_to_form(rec, matched_terms)
            form_rec['_matched_terms'] = ', '.join(set(matched_terms))
            form_rec['_entry_id'] = rec.get('Entry_ID', '')
            acro_records.append(form_rec)
            print(f'  MATCH Entry {rec.get("Entry_ID","?")}: {", ".join(set(matched_terms))}')

    # Sort: low acro first, then by date
    acro_records.sort(key=lambda r: (0 if r['Low_or_High'] == 'Low' else 1, r['Date']),
                      reverse=True)

    low_count = sum(1 for r in acro_records if r['Low_or_High'] == 'Low')
    high_count = sum(1 for r in acro_records if r['Low_or_High'] == 'High')
    unknown_count = len(acro_records) - low_count - high_count

    print(f'\nFound {len(acro_records)} acro-related incidents:')
    print(f'  Low acro:     {low_count}')
    print(f'  High acro:    {high_count}')
    print(f'  Unclassified: {unknown_count}')

    # Save CSV
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=ACRO_FORM_COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(acro_records)

    print(f'\nSaved to {OUTPUT_CSV}')


if __name__ == '__main__':
    main()
