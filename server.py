"""
Simple download server for scraped USPPA incident CSV/JSON files.
Run after scraper.py to serve files at http://localhost:8000
"""
from http.server import HTTPServer, SimpleHTTPRequestHandler
import os
import urllib.parse

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
PORT = 8000


class DownloadHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/' or self.path == '':
            self.serve_index()
        elif self.path.startswith('/download/'):
            self.serve_file()
        else:
            self.send_error(404)

    def serve_index(self):
        files = sorted(os.listdir(OUTPUT_DIR)) if os.path.isdir(OUTPUT_DIR) else []
        batch_files = [f for f in files if f.startswith('incidents_batch_')]
        combined_files = [f for f in files if f.startswith('usppa_incidents_all')]

        rows = ''
        for f in combined_files:
            size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
            size_str = f'{size / 1024:.1f} KB' if size < 1048576 else f'{size / 1048576:.1f} MB'
            rows += f'<tr><td>📦 {f}</td><td>{size_str}</td><td><a href="/download/{f}" class="btn combined">Download</a></td></tr>\n'

        for f in batch_files:
            size = os.path.getsize(os.path.join(OUTPUT_DIR, f))
            size_str = f'{size / 1024:.1f} KB' if size < 1048576 else f'{size / 1048576:.1f} MB'
            rows += f'<tr><td>📄 {f}</td><td>{size_str}</td><td><a href="/download/{f}" class="btn">Download</a></td></tr>\n'

        html = f"""<!DOCTYPE html>
<html>
<head>
<title>USPPA Incident Data Downloads</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; background: #f5f5f5; }}
  h1 {{ color: #333; }}
  table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
  th {{ background: #2c3e50; color: white; padding: 12px 16px; text-align: left; }}
  td {{ padding: 10px 16px; border-bottom: 1px solid #eee; }}
  tr:hover {{ background: #f8f9fa; }}
  .btn {{ display: inline-block; padding: 6px 16px; background: #3498db; color: white; text-decoration: none; border-radius: 4px; font-size: 14px; }}
  .btn:hover {{ background: #2980b9; }}
  .btn.combined {{ background: #27ae60; }}
  .btn.combined:hover {{ background: #219a52; }}
  .info {{ color: #666; margin-bottom: 20px; }}
</style>
</head>
<body>
<h1>USPPA Incident Data</h1>
<p class="info">Download individual batch files (100 incidents each) or the combined dataset.</p>
<table>
<tr><th>File</th><th>Size</th><th>Action</th></tr>
{rows if rows else '<tr><td colspan="3" style="text-align:center;padding:20px;">No files yet. Run scraper.py first.</td></tr>'}
</table>
</body>
</html>"""

        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        self.wfile.write(html.encode())

    def serve_file(self):
        filename = urllib.parse.unquote(self.path.split('/download/')[-1])
        # Prevent directory traversal
        filename = os.path.basename(filename)
        filepath = os.path.join(OUTPUT_DIR, filename)

        if not os.path.isfile(filepath):
            self.send_error(404, 'File not found')
            return

        self.send_response(200)
        if filename.endswith('.csv'):
            self.send_header('Content-Type', 'text/csv')
        elif filename.endswith('.json'):
            self.send_header('Content-Type', 'application/json')
        else:
            self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
        self.send_header('Content-Length', str(os.path.getsize(filepath)))
        self.end_headers()

        with open(filepath, 'rb') as f:
            self.wfile.write(f.read())


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    server = HTTPServer(('0.0.0.0', PORT), DownloadHandler)
    print(f'Serving USPPA incident files at http://localhost:{PORT}')
    print(f'Files directory: {OUTPUT_DIR}')
    print('Press Ctrl+C to stop.')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nShutting down.')
        server.server_close()


if __name__ == '__main__':
    main()
