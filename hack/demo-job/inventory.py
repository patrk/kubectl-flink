# The inventory service the checkout job asks for stock: a real HTTP server whose latency and error rate come from
# the environment, so that a demo can slow it down or break it with kubectl set env, as a database under load or a
# bad release would. Threaded and keep-alive, so that the latency is per request and not a queue.
import os
import random
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LATENCY_MS = float(os.environ.get("LATENCY_MS", "20"))
ERROR_PERCENT = float(os.environ.get("ERROR_PERCENT", "0"))


class Handler(BaseHTTPRequestHandler):
    # Keep-alive, as the job's HTTP client reuses its connections; HTTP/1.0 would close each one under it.
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        time.sleep(LATENCY_MS / 1000)
        if random.uniform(0, 100) < ERROR_PERCENT:
            body, code = b"connection pool exhausted", 503
        else:
            body, code = str(random.randint(0, 500)).encode(), 200
        self.send_response(code)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


ThreadingHTTPServer.daemon_threads = True
# Room for the connections of every request in flight, not the default 5.
ThreadingHTTPServer.request_queue_size = 256
ThreadingHTTPServer(("", 8080), Handler).serve_forever()
