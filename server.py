"""
USPPA Incident Scraper + Download Server
Run: python server.py
Opens http://localhost:8000 with live progress bar and file downloads.
"""
import os
import sys
import json
import threading
import webbrowser
import functools
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = 8000
PUBLIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public')


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=PUBLIC_DIR, **kwargs)

    def end_headers(self):
        # No caching so status.json always returns fresh data
        self.send_header('Cache-Control', 'no-store')
        super().end_headers()

    def log_message(self, format, *args):
        pass  # Suppress request logs to keep terminal clean for scraper output


def main():
    os.makedirs(os.path.join(PUBLIC_DIR, 'data'), exist_ok=True)

    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f'Server running at http://localhost:{PORT}')
    print('Starting scraper...\n')

    # Open browser
    webbrowser.open(f'http://localhost:{PORT}')

    # Run scraper in background thread
    from scraper import run_scrape
    scrape_thread = threading.Thread(target=run_scrape, daemon=True)
    scrape_thread.start()

    # Serve until scraper finishes, then keep serving for downloads
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped.')
        server.server_close()


if __name__ == '__main__':
    main()
