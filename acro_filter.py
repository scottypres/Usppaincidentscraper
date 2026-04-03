"""
Post-scrape filter: extract acro-related incidents from scraped data
and output in Google Forms format with USPPA incident link.

Run after scraper: python acro_filter.py
"""
import csv, json, re, os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'public', 'data')
INPUT_JSON = os.path.join(DATA_DIR, 'usppa_incidents_all.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'acro_incidents.csv')

# --- Acro detection keywords ---
# Primary: strong indicators of acro flying
ACRO_PRIMARY = [
    r'\bacro\b', r'\bacrobat', r'\bwing\s*over', r'\bwingover',
    r'\bbarrel\s*roll', r'\bspiral\s*dive', r'\bsat\b', r'\bfull\s*stall',
    r'\bspin\b', r'\bspinn', r'\binfinite\s*tumbl', r'\btumbl',
    r'\bhelico', r'\bhelicopter\b', r'\bmisty\s*flip', r'\brhythmic',
    r'\bmctwist', r'\bmc\s*twist', r'\besfera', r'\bcorkscrew',
    r'\bjoker\b', r'\banti.?rhythm', r'\basymmetric\s*spiral',
    r'\bsuperstall', r'\bsuper\s*stall', r'\bdeep\s*stall',
    r'\bstall\s*(?:to\s*)?spin', r'\bflat\s*spin',
]

# Secondary: need context (e.g. combined with maneuver language)
ACRO_SECONDARY = [
    r'\bstall\b', r'\bspiral\b', r'\bcollapse', r'\bcravat',
    r'\brecovery\b', r'\breserve\b', r'\btwist(?:ed|ing)?\s*(?:line|riser)',
    r'\bcascade', r'\bnegative\b', r'\bG.?force', r'\blooping',
    r'\btrick', r'\bmaneuver', r'\bmanoeuvre',
]

# Context words that make secondary matches more likely acro
ACRO_CONTEXT = [
    r'\bpracticing\b', r'\battempt', r'\btrying\b', r'\bperform',
    r'\blearning\b', r'\btraining\b', r'\bover\s*water', r'\blake\b',
    r'\blow\b.*\bpass\b', r'\blow\s+alt', r'\blow\b',
    r'\baggressive', r'\bradical', r'\bextreme',
]

# --- Maneuver classification ---
MANEUVERS = {
    'wing overs': [r'\bwing\s*over', r'\bwingover'],
    'barrel roll': [r'\bbarrel\s*roll'],
    'spiral': [r'\bspiral', r'\basymmetric\s*spiral'],
    'stall': [r'\bstall', r'\bfull\s*stall', r'\bdeep\s*stall', r'\bsuper\s*stall'],
    'spin': [r'\bspin\b', r'\bspinn', r'\bflat\s*spin'],
    'infinite': [r'\binfinite', r'\btumbl'],
    'single turn into ground': [r'\bsingle\s*turn', r'\bturn.*ground', r'\bground\s*spiral'],
    'SAT': [r'\bsat\b', r'\bhelico'],
    'misty flip': [r'\bmisty'],
    'mctwist': [r'\bmctwist', r'\bmc\s*twist'],
    'looping': [r'\bloop'],
}

# --- Low vs High detection ---
LOW_PATTERNS = [
    r'\blow\b.*\b(?:acro|wing\s*over|spiral|stall|spin|maneuver|trick|pass|turn|altitude)',
    r'\b(?:acro|wing\s*over|spiral|stall|spin|maneuver|trick).*\blow\b',
    r'\blow\s+alt', r'\blow\s+height', r'\blow\s+level',
    r'\b(?:50|100|150|200)\s*(?:feet|ft|meters|m)\b',
    r'\bnear\s*(?:the\s*)?ground', r'\bclose\s*to\s*(?:the\s*)?ground',
    r'\bground\s*level', r'\blow\s+pass', r'\blow\s+approach',
    r'\btoo\s+low', r'\binsufficient\s+alt', r'\bnot\s+enough\s+(?:alt|height)',
    r'\b(?:launch|takeoff|take.?off)\b.*\b(?:stall|spin|spiral|turn)',
]

HIGH_PATTERNS = [
    r'\bhigh\b.*\b(?:acro|altitude|alt)\b',
    r'\b(?:acro|wing\s*over|spiral).*\bhigh\b',
    r'\bover\s*water', r'\bhigh\s+alt',
    r'\b(?:1000|2000|3000|[1-9]\d{3})\s*(?:feet|ft|agl)',
]

ACRO_FORM_COLS = [
    'Date', 'Location', 'Pilot_Name', 'Glider_Make_Model', 'Injury',
    'Fatal', 'Experience_Level', 'Description', 'Low_or_High',
    'Maneuver', 'Lesson_Learned', 'Motor', 'USPPA_Link',
]


def matches_any(text, patterns):
    for p in patterns:
        if re.search(p, text, re.IGNORECASE):
            return True
    return False


def is_acro_incident(rec):
    """Determine if an incident is acro-related. Returns (is_acro, confidence)."""
    text = (rec.get('Raw_Text', '') + ' ' + rec.get('Description', '') + ' ' +
            rec.get('Type_of_Incident', '') + ' ' + rec.get('Analysis', '') + ' ' +
            rec.get('Phase_of_Flight', ''))

    # Primary keyword = definitely acro
    if matches_any(text, ACRO_PRIMARY):
        return True, 'high'

    # Secondary keyword + context = likely acro
    if matches_any(text, ACRO_SECONDARY) and matches_any(text, ACRO_CONTEXT):
        return True, 'medium'

    return False, None


def detect_maneuver(text):
    """Detect which maneuver(s) were involved."""
    found = []
    for name, patterns in MANEUVERS.items():
        for p in patterns:
            if re.search(p, text, re.IGNORECASE):
                found.append(name)
                break
    return ', '.join(found) if found else ''


def detect_low_or_high(text):
    """Determine if acro started low or high."""
    low = matches_any(text, LOW_PATTERNS)
    high = matches_any(text, HIGH_PATTERNS)
    if low and not high:
        return 'Low'
    if high and not low:
        return 'High'
    if low and high:
        return 'Low'  # err on the side of flagging low acro
    return ''


def detect_injury(rec):
    text = (rec.get('Type_of_Injury', '') + ' ' + rec.get('Raw_Text', '')).lower()
    if any(w in text for w in ['fatal', 'death', 'died', 'killed', 'deceased']):
        return 'Yes', 'Yes'
    if any(w in text for w in ['no injury', 'none', 'no injuries', 'uninjured']):
        return 'No', 'No'
    if any(w in text for w in ['injury', 'injured', 'broken', 'fracture', 'hospital',
                                'concussion', 'laceration', 'sprain', 'burn']):
        return 'Yes', 'No'
    return '', 'No'


def detect_motor(rec):
    text = (rec.get('PPG_Type', '') + ' ' + rec.get('Paramotor_Frame', '') + ' ' +
            rec.get('Raw_Text', '')).lower()
    if any(w in text for w in ['paramotor', 'ppg', 'motor', 'powered paraglid']):
        return 'Motor'
    if any(w in text for w in ['free flight', 'paraglid', 'pg ', 'free fly']):
        return 'No Motor'
    # Default for USPPA data (mostly powered)
    return 'Motor'


def convert_to_form(rec, confidence):
    """Convert a scraped record to Google Form format."""
    text = (rec.get('Raw_Text', '') + ' ' + rec.get('Description', '') + ' ' +
            rec.get('Analysis', ''))

    injury, fatal = detect_injury(rec)

    # Build glider make/model
    glider_parts = [rec.get('Wing_Brand', ''), rec.get('Wing_Model', '')]
    glider = ' '.join(p for p in glider_parts if p).strip()

    # Description: prefer Analysis > Description > Raw_Text snippet
    desc = rec.get('Analysis', '') or rec.get('Description', '')
    if not desc and rec.get('Raw_Text', ''):
        desc = rec['Raw_Text'][:500]

    return {
        'Date': rec.get('Incident_Date', '') or rec.get('Incident_DateTime', ''),
        'Location': rec.get('Location', ''),
        'Pilot_Name': '',  # not in USPPA data
        'Glider_Make_Model': glider,
        'Injury': injury,
        'Fatal': fatal,
        'Experience_Level': rec.get('Pilot_Experience', '') or rec.get('Highest_Rating', ''),
        'Description': desc,
        'Low_or_High': detect_low_or_high(text),
        'Maneuver': detect_maneuver(text),
        'Lesson_Learned': '',  # not typically in USPPA data
        'Motor': detect_motor(rec),
        'USPPA_Link': rec.get('URL', ''),
    }


def main():
    if not os.path.exists(INPUT_JSON):
        print(f'No data found at {INPUT_JSON}')
        print('Run the scraper first: python server.py')
        return

    with open(INPUT_JSON, 'r') as f:
        records = json.load(f)

    print(f'Loaded {len(records)} total incidents.')

    acro_records = []
    for rec in records:
        is_acro, confidence = is_acro_incident(rec)
        if is_acro:
            form_rec = convert_to_form(rec, confidence)
            form_rec['_confidence'] = confidence
            form_rec['_entry_id'] = rec.get('Entry_ID', '')
            acro_records.append(form_rec)

    # Sort: low acro first, then by date
    acro_records.sort(key=lambda r: (0 if r['Low_or_High'] == 'Low' else 1, r['Date']),
                      reverse=True)

    print(f'\nFound {len(acro_records)} acro-related incidents:')
    low_count = sum(1 for r in acro_records if r['Low_or_High'] == 'Low')
    high_count = sum(1 for r in acro_records if r['Low_or_High'] == 'High')
    unknown_count = len(acro_records) - low_count - high_count
    print(f'  Low acro:  {low_count}')
    print(f'  High acro: {high_count}')
    print(f'  Unknown:   {unknown_count}')

    # Save CSV
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=ACRO_FORM_COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(acro_records)

    print(f'\nSaved to {OUTPUT_CSV}')

    # Print summary
    print(f'\n--- LOW ACRO INCIDENTS ---')
    for r in acro_records:
        if r['Low_or_High'] == 'Low':
            print(f"  [{r['Date']}] {r['Maneuver'] or 'unknown maneuver'} - {r['Location']}")
            print(f"    {r['USPPA_Link']}")


if __name__ == '__main__':
    main()
