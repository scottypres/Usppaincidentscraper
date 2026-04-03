"""
Post-scrape filter: extract acro-related incidents from scraped USPPA data
and output in Google Forms format with USPPA incident link.

Keywords and patterns derived from reading all 542 actual USPPA incident reports.
"""
import csv, json, re, os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT_DIR, 'public', 'data')
INPUT_JSON_DATA = os.path.join(DATA_DIR, 'usppa_incidents_all.json')
INPUT_JSON_ROOT = os.path.join(ROOT_DIR, 'usppa_incidents_all.json')
OUTPUT_CSV = os.path.join(DATA_DIR, 'acro_incidents.csv')

# ============================================================================
# ACRO DETECTION - based on actual USPPA report language
# ============================================================================

# TIER 1: Definitely acro - any single match = acro incident
DEFINITE_ACRO = [
    r'\bacro\b',                          # "doing acro", "acro at 50-200 feet"
    r'\bacrobat',                         # "acrobatics", "low acrobatics", "low-altitude acrobatics"
    r'\baerobat',                         # "aerobatics", "low aerobatics"
    r'\bwing\s*overs?\b',                  # "wingovers", "wing overs", "wingover maneuver"
    r'\bwingovers?\b',
    r'\bbarrel\s*roll',                   # "barrel rolls"
    r'\brocket\s*loop',                   # "triple rocket loop"
    r'\bsat\s+maneuver',                  # "SAT maneuver" (avoids matching "sat down")
    r'\bperforming\s+a\s+sat\b',         # "performing a SAT"
    r'\bpractice\s+a\s+sat\b',           # "practice a SAT"
    r'\btrick(?:s)?\s+and\s+stunt',      # "tricks and stunts"
    r'\binvented\s+new\s+tricks',        # from entry 2883
    r'\blow\s+(?:level\s+)?aerobat',     # "low aerobatics", "low level aerobatics"
    r'\blow[\s-]+altitude\s+acrobat',    # "low-altitude acrobatics"
    r'\blow\s+maneuvering',              # "low maneuvering" (entry 2487)
    r'\baggressive\s+low\s+altitude\s+wingover',  # entry 1462
    r'\blow\s+acro',                      # "low acro"
    r'\btrike\s+acrobat',                # "trike acrobatics" (entry 2435)
    r'\bshow\s*(?:ing)?\s*off',          # not yet seen but plausible
    r'\bhot\s*dog',
    r'\battempting\s+stalls',            # "attempting stalls" (entry 1409)
    r'\bpre[\s-]?stall',                 # "pre stall" practice (entry 1409)
    r'\bfull\s+stall\b(?!.*\bparachut)', # "full stall" but not "full stall parachute"
    r'\binfinite\s*tumbl',
    r'\bflat\s*spin',
    r'\bmisty\s*flip',
    r'\bmctwist',
    r'\besfera',
    r'\bcorkscrew',
    r'\bdynamic\s+(?:full\s+)?stall',
]

# TIER 2: Likely acro — intentional aggressive maneuver
TIER2_PATTERNS = [
    # Intentional spiral/steep turn that went wrong
    r'\binitiat(?:ing|ed)\s+a\s+(?:steep|tight|deep)?\s*(?:spiral|turn)',
    r'\bsteep\s+turn\b.*\b(?:spiral|aggressiv|water|crash)',
    r'\bdeep\s+spiral\b',
    r'\bspiral\s+dive\s+(?:loss|into|crash)',
    r'\bdeath\s+spiral',
    # Practicing spirals/stalls intentionally
    r'\bpracticing\s+(?:death\s+)?spiral',
    # Aggressive low flying
    r'\blow[\s-]+level\s+maneuver',
    r'\blow[\s-]+level\s+manouver',
    r'\baggressiv(?:e|ely)\s+(?:turn|scalloped|wingover|low)',
    r'\bact\s+of\s+showmanship',
    r'\bbuzz(?:ing|ed)?\s+(?:the\s+)?(?:beach|field|crowd|people)',
    r'\bgoofing\s+off',
    r'\bfigure\s*8.*\b(?:low|aggressiv)',
    r'\baggressiv.*\bfigure\s*8',
    r'\broller\s*coaster\b.*\bmaneuver',
    # Intentionally stalling at low altitude
    r'\bstall(?:ing|ed)\s+the\s+(?:glider|wing)\s+(?:from|at)\s+\d+',
    r'\bpilot\s+stalled\s+(?:the\s+)?wing\s+while',
]

# ============================================================================
# MANEUVER CLASSIFICATION
# ============================================================================
MANEUVERS = {
    'wing overs': [r'\bwing\s*overs?\b', r'\bwingovers?\b'],
    'barrel roll': [r'\bbarrel\s*roll'],
    'spiral': [r'\bspiral'],
    'stall': [r'\bstall(?:ing|ed)?\b', r'\bfull\s*stall', r'\bpre[\s-]?stall'],
    'spin': [r'\bspin\b(?!ning\s+propell)'],  # exclude "spinning propeller"
    'infinite': [r'\binfinite', r'\btumbl(?!eweed)'],  # exclude "tumbleweed"
    'SAT': [r'\bsat\s+maneuver', r'\bperforming\s+a\s+sat', r'\bpractice\s+a\s+sat'],
    'looping': [r'\brocket\s*loop', r'\bloop(?!.*brake)'],  # exclude "brake loop"
    'single turn into ground': [r'\bsingle\s*turn'],
}

# ============================================================================
# LOW vs HIGH classification
# ============================================================================
LOW_PATTERNS = [
    r'\blow\s+(?:level\s+)?aerobat',
    r'\blow[\s-]+altitude\s+acrobat',
    r'\blow\s+acro\b',
    r'\blow\s+maneuvering',
    r'\blow\b.*\bwingover', r'\bwingover.*\blow\b',
    r'\blow\b.*\bwing\s*over', r'\bwing\s*over.*\blow\b',
    r'\b(?:30|40|50|75|100|120)\s*(?:feet|foot|ft|\')\s*(?:AGL)?\b',
    r'\btreetop',
    r'\bnear\s*(?:the\s*)?ground',
    r'\btoo\s+low',
    r'\bless\s+than\s+50\s*ft',
    r'\blow\s+pass',
    r'\bbuzz(?:ing|ed)?.*(?:beach|ground|field)',
    r'\blow\b.*\bsmoke\b',
    r'\blow\s+alt',
    r'\b50-200\s*f(?:ee)?t',                   # "50-200 ft" from entry 1955
    r'\b35-50\s*ft',                            # from entry 1462
    r'\bscalloped\s+wingovers',                 # always low (entry 1462)
    r'low\s+acro(?:batics)?\s+kills',          # "Low acro kills" safety phrase
]

HIGH_PATTERNS = [
    r'\bhigh\s+(?:alt|altitude)\b',
    r'\bover\s+water\b.*\bacro',
    r'\b(?:1000|1500|2000|3000)\s*(?:feet|foot|ft)',
    r'\bplenty\s+of\s+alt',
    r'\bsafe\s+altitude',
    r'\balt(?:itude)?\s+was\s+my\s+(?:biggest\s+)?ally',  # entry 2967
]

ACRO_FORM_COLS = [
    'Date', 'Location', 'Pilot_Name', 'Glider_Make_Model', 'Injury',
    'Fatal', 'Experience_Level', 'Description', 'Low_or_High',
    'Maneuver', 'Lesson_Learned', 'Motor', 'USPPA_Link',
]


def get_text(rec):
    return ' '.join(str(rec.get(k, '') or '') for k in [
        'Raw_Text', 'Description', 'Type_of_Incident', 'Analysis',
        'Phase_of_Flight', 'Collateral_Damage', 'Other'
    ])


def matches_any(text, patterns):
    for p in patterns:
        if re.search(p, text, re.IGNORECASE):
            return True
    return False


def find_matches(text, patterns):
    found = []
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            found.append(m.group(0).strip())
    return found


def is_acro_incident(rec):
    text = get_text(rec)

    # Tier 1: definite acro
    matches = find_matches(text, DEFINITE_ACRO)
    if matches:
        return True, 'definite', matches

    # Tier 2: specific aggressive/intentional maneuver patterns
    tier2_matches = find_matches(text, TIER2_PATTERNS)
    if tier2_matches:
        return True, 'likely', tier2_matches

    return False, None, []


def detect_maneuver(text):
    found = []
    for name, patterns in MANEUVERS.items():
        for p in patterns:
            if re.search(p, text, re.IGNORECASE):
                found.append(name)
                break
    return ', '.join(found) if found else ''


def detect_low_or_high(text):
    low = matches_any(text, LOW_PATTERNS)
    high = matches_any(text, HIGH_PATTERNS)
    if low and not high:
        return 'Low'
    if high and not low:
        return 'High'
    if low and high:
        return 'Low'
    return ''


def detect_injury(rec):
    text = get_text(rec).lower()
    typ = (rec.get('Type_of_Injury', '') or '').lower()
    if 'fatal' in typ or any(w in text for w in ['fatal', 'death', 'died', 'killed', 'perished']):
        return 'Yes', 'Yes'
    if 'no injury' in typ or 'no injury' in text:
        return 'No', 'No'
    if 'major' in typ or any(w in text for w in ['broken', 'broke', 'fracture', 'hospital',
                                                   'surgery', 'trauma center', 'airlifted']):
        return 'Yes', 'No'
    if 'minor' in typ:
        return 'Yes', 'No'
    return '', 'No'


def detect_motor(rec):
    text = get_text(rec).lower()
    ppg = (rec.get('PPG_Type', '') or '').lower()
    frame = (rec.get('Paramotor_Frame', '') or '').lower()
    if any(w in ppg + frame for w in ['paramotor', 'wheel launch', 'foot launch', 'trike']):
        return 'Motor'
    if any(w in text for w in ['paramotor', 'ppg', 'motor', 'throttle', 'prop']):
        return 'Motor'
    if any(w in text for w in ['free flight', 'paraglider only']):
        return 'No Motor'
    return 'Motor'


def convert_to_form(rec):
    text = get_text(rec)
    injury, fatal = detect_injury(rec)

    glider_parts = [rec.get('Wing_Brand', ''), rec.get('Wing_Model', '')]
    glider = ' '.join(p for p in glider_parts if p).strip()

    desc = rec.get('Description', '') or ''
    if not desc:
        raw = rec.get('Raw_Text', '') or ''
        # Skip the header boilerplate, grab meaningful content
        for line in raw.split('\n'):
            line = line.strip()
            if len(line) > 50 and 'Incident List' not in line and 'Return to' not in line:
                desc = line[:500]
                break

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
    input_path = None
    for path in [INPUT_JSON_DATA, INPUT_JSON_ROOT]:
        if os.path.exists(path):
            input_path = path
            break

    if not input_path:
        print('No data found. Run the scraper first.')
        return

    with open(input_path, 'r') as f:
        records = json.load(f)

    print(f'Loaded {len(records)} total incidents from {input_path}')

    acro_records = []
    for rec in records:
        is_acro, confidence, matched = is_acro_incident(rec)
        if is_acro:
            form_rec = convert_to_form(rec)
            entry_id = rec.get('Entry_ID', '?')
            desc_short = (rec.get('Description', '') or '')[:60]
            print(f'  [{confidence:8s}] Entry {entry_id}: {", ".join(set(matched))[:60]}')
            print(f'             {desc_short}')
            acro_records.append(form_rec)

    acro_records.sort(key=lambda r: (
        0 if r['Low_or_High'] == 'Low' else (1 if r['Low_or_High'] == 'High' else 2),
        r['Date']
    ), reverse=True)

    low = sum(1 for r in acro_records if r['Low_or_High'] == 'Low')
    high = sum(1 for r in acro_records if r['Low_or_High'] == 'High')
    fatal = sum(1 for r in acro_records if r['Fatal'] == 'Yes')

    print(f'\n{"="*50}')
    print(f'RESULTS: {len(acro_records)} acro-related incidents')
    print(f'  Low acro:     {low}')
    print(f'  High acro:    {high}')
    print(f'  Unclassified: {len(acro_records) - low - high}')
    print(f'  Fatal:        {fatal}')
    print(f'{"="*50}')

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=ACRO_FORM_COLS, extrasaction='ignore')
        w.writeheader()
        w.writerows(acro_records)

    print(f'\nSaved to {OUTPUT_CSV}')


if __name__ == '__main__':
    main()
