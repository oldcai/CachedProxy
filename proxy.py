import logging
import os
import sys
import socket
import yaml
import json
import base64
from urlparse import urlparse

import tornado.httpserver
import tornado.ioloop
import tornado.iostream
import tornado.web
import tornado.httpclient
import tornadoredis

__all__ = ['ProxyHandler', 'run_proxy']


def get_config(path="config.yaml"):
    return yaml.load(open("config.yaml").read())

config = get_config()
redis = config.get(
    "redis", {
        "host":
        "localhost",
        "port": 6379
    }
)

CONNECTION_POOL = tornadoredis.ConnectionPool(max_connections=500,
                                              wait_for_available=True,
                                              host=redis["host"],
                                              port=redis["port"])


def get_logger():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='/tmp/cache_proxy.log',
                        filemode='w')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    logging.getLogger('cache_proxy').addHandler(console)
    return logging.getLogger('cache_proxy')
logger = get_logger()


def get_proxy(url):
    url_parsed = urlparse(url, scheme='http')
    proxy_key = '%s_proxy' % url_parsed.scheme
    return os.environ.get(proxy_key)


def parse_proxy(proxy):
    proxy_parsed = urlparse(proxy, scheme='http')
    return proxy_parsed.hostname, proxy_parsed.port


def get_key(url, **kwargs):
    return "|-|".join([
        url,
        kwargs.get("method", "GET"),
        str(kwargs.get("body", "BODY")),
        str(kwargs.get("headers", "HEADERS")),
    ])


def serialize_response(response):
    if (response.error and not
            isinstance(response.error, tornado.httpclient.HTTPError)):
        response = {
            "status": response.code,
            "body": 'Internal server error:\n' + str(response.error),
        }
    else:
        headers = []
        for header in ('Date', 'Cache-Control', 'Server',
                       'Content-Type', 'Location', 'Set-Cookie'):
            v = response.headers.get(header)
            if v:
                headers.append([header, v])
        response = {
            "status": response.code,
            "headers": headers,
            "body": response.body,
        }
    return response


def encode_response(response):
    response = response.copy()
    response["body"] = base64.encodestring(response["body"])
    return json.dumps(response)


def decode_response(response):
    response = json.loads(response)
    response["body"] = base64.decodestring(response["body"])
    return response


class ProxyHandler(tornado.web.RequestHandler):
    SUPPORTED_METHODS = ['GET', 'POST', 'CONNECT']

    @tornado.web.asynchronous
    @tornado.gen.engine
    def fetch_request(self, url, callback, **kwargs):
        cache = tornadoredis.Client(connection_pool=CONNECTION_POOL)
        response = yield tornado.gen.Task(cache.get, get_key(url, **kwargs))
        if not response:
            proxy = get_proxy(url)
            if proxy:
                logger.debug('Forward request via upstream proxy %s', proxy)
                tornado.httpclient.AsyncHTTPClient.configure(
                    'tornado.curl_httpclient.CurlAsyncHTTPClient')
                host, port = parse_proxy(proxy)
                kwargs['proxy_host'] = host
                kwargs['proxy_port'] = port

            req = tornado.httpclient.HTTPRequest(url, **kwargs)
            client = tornado.httpclient.AsyncHTTPClient()
            response = yield tornado.gen.Task(client.fetch, req)
            response = serialize_response(response)
            yield tornado.gen.Task(cache.set, get_key(url, **kwargs), encode_response(response))
        else:
            response = decode_response(response)
        yield tornado.gen.Task(cache.disconnect)
        callback(response)

    @tornado.web.asynchronous
    def get(self):
        logger.debug('Handle %s request to %s', self.request.method,
                     self.request.uri)

        def handle_response(response):
            # response = json.loads(response)
            self.set_status(response["status"])
            headers = response.get("headers")
            if headers:
                for header in headers:
                    if len(header) >= 2:
                        self.set_header(header[0], header[1])
            body = response["body"]
            if body:
                self.write(body)
            self.finish()

        body = self.request.body
        if not body:
            body = None
        try:
            self.fetch_request(
                self.request.uri, handle_response,
                method=self.request.method, body=body,
                headers=self.request.headers, follow_redirects=False,
                allow_nonstandard_methods=True)
        except tornado.httpclient.HTTPError as e:
            self.set_status(500)
            self.write('Internal server error:\n' + str(e))
            self.finish()

    @tornado.web.asynchronous
    def post(self):
        return self.get()

    @tornado.web.asynchronous
    def connect(self):
        logger.debug('Start CONNECT to %s', self.request.uri)
        host, port = self.request.uri.split(':')
        client = self.request.connection.stream

        def read_from_client(data):
            upstream.write(data)

        def read_from_upstream(data):
            client.write(data)

        def client_close(data=None):
            if upstream.closed():
                return
            if data:
                upstream.write(data)
            upstream.close()

        def upstream_close(data=None):
            if client.closed():
                return
            if data:
                client.write(data)
            client.close()

        def start_tunnel():
            logger.debug('CONNECT tunnel established to %s', self.request.uri)
            client.read_until_close(client_close, read_from_client)
            upstream.read_until_close(upstream_close, read_from_upstream)
            client.write(b'HTTP/1.0 200 Connection established\r\n\r\n')

        def on_proxy_response(data=None):
            if data:
                first_line = data.splitlines()[0]
                http_v, status, text = first_line.split(None, 2)
                if int(status) == 200:
                    logger.debug('Connected to upstream proxy %s', proxy)
                    start_tunnel()
                    return

            self.set_status(500)
            self.finish()

        def start_proxy_tunnel():
            upstream.write('CONNECT %s HTTP/1.1\r\n' % self.request.uri)
            upstream.write('Host: %s\r\n' % self.request.uri)
            upstream.write('Proxy-Connection: Keep-Alive\r\n\r\n')
            upstream.read_until('\r\n\r\n', on_proxy_response)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        upstream = tornado.iostream.IOStream(s)

        proxy = get_proxy(self.request.uri)
        if proxy:
            proxy_host, proxy_port = parse_proxy(proxy)
            upstream.connect((proxy_host, proxy_port), start_proxy_tunnel)
        else:
            upstream.connect((host, int(port)), start_tunnel)


def run_proxy(port, start_ioloop=True):
    """
    Run proxy on the specified port. If start_ioloop is True (default),
    the tornado IOLoop will be started immediately.
    """
    app = tornado.web.Application([
        (r'.*', ProxyHandler),
    ])
    app.listen(port)
    ioloop = tornado.ioloop.IOLoop.instance()
    if start_ioloop:
        ioloop.start()

if __name__ == '__main__':
    port = 8888
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    print ("Starting HTTP proxy on port %d" % port)
    run_proxy(port)