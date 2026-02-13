import os
from http.server import HTTPServer, BaseHTTPRequestHandler


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format, *args):
        # Silence default HTTP logs
        return


def main():
    host = "0.0.0.0"
    port = int(os.getenv("PORT", "8000"))
    server = HTTPServer((host, port), HealthHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
