"""
Local dev server - serves the public/ directory at http://localhost:8000
Same content that Vercel serves in production.
"""
import os
import functools
from http.server import HTTPServer, SimpleHTTPRequestHandler

PORT = 8000
PUBLIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'public')

Handler = functools.partial(SimpleHTTPRequestHandler, directory=PUBLIC_DIR)

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    print(f'Serving public/ at http://localhost:{PORT}')
    print('Press Ctrl+C to stop.')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped.')
        server.server_close()
