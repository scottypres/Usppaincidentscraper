"""
Run on your SSH server to sample incident formats across different pages.
Usage: python sample_field_formats.py > field_samples.txt
Then share field_samples.txt so I can fix the scraper patterns.
"""
import requests
from bs4 import BeautifulSoup
import re, time

HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
BASE = 'https://usppa.org'

def fetch(url):
    return requests.get(url, headers=HEADERS, timeout=30).text

pages_to_check = [1, 3, 5, 8, 10, 13, 15, 18, 20, 25]

for page in pages_to_check:
    print(f'\n{"="*80}')
    print(f'LISTING PAGE {page}')
    print(f'{"="*80}')
    try:
        html = fetch(f'{BASE}/incidents/?frm-page-7672={page}')
        soup = BeautifulSoup(html, 'html.parser')
        links = soup.select('a[href*="/incidents/entry/"]')
        if not links:
            print('  No entries found on this page.')
            continue

        href = links[0].get('href', '')
        url = href if href.startswith('http') else BASE + href
        print(f'ENTRY URL: {url}\n')

        html2 = fetch(url)
        soup2 = BeautifulSoup(html2, 'html.parser')
        content = soup2.select_one('.zn_text_box')
        if not content:
            for sel in ['.entry-content', '.post-content', 'article', '.content-area', 'main']:
                content = soup2.select_one(sel)
                if content:
                    print(f'  (Found content with selector: {sel})')
                    break

        if content:
            text = content.get_text(separator='\n', strip=True)
            print(text)

            print(f'\n--- BOLD/STRONG LABELS ---')
            for b in content.select('strong, b'):
                t = b.get_text(strip=True)
                if t:
                    print(f'  [{t}]')

            print(f'\n--- ALL FIELD LABELS (text before colons) ---')
            for m in re.finditer(r'([A-Za-z][A-Za-z /()]+?)\s*:', text):
                label = m.group(1).strip()
                if 2 < len(label) < 60:
                    print(f'  [{label}]')
        else:
            print('  No content found.')

        time.sleep(0.5)
    except Exception as e:
        print(f'  Error: {e}')

print(f'\n{"="*80}')
print('DONE - paste this output back to me')
