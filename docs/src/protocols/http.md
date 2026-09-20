# HTTP

The HTTP client is built directly on hyper with a custom connection layer so
**every phase of every request is measured** — no averaged guesses:

| Metric | Phase |
|---|---|
| `http_req_blocked` | waiting for a connection (dns + connect + tls on cold connections; ~0 on reuse) |
| `http_req_connecting` | TCP connect |
| `http_req_tls_handshaking` | TLS handshake |
| `http_req_sending` | writing the request |
| `http_req_waiting` | time to first byte (TTFB) |
| `http_req_receiving` | reading the body |
| `http_req_duration` | sending + waiting + receiving |

Plus `http_reqs`, `http_req_failed` (transport error or status ≥ 400),
`data_sent`, `data_received`. Samples carry `name`, `method`, `status`,
`scenario`, `group`, `proto` tags.

## Versions

`defaults.http.version`:

- `auto` (default) — ALPN negotiation; HTTP/2 when the server offers it.
- `http1` — force HTTP/1.1.
- `http2` — offer only h2 over TLS.
- `http2-prior-knowledge` — HTTP/2 without negotiation, including plaintext.

HTTP/2 connections are multiplexed; HTTP/1.1 connections are kept alive and
reused **per VU** (a VU models one user agent: its own connections and cookie
jar). `keep_alive: false` closes after each request.

## DNS and connection recovery

HTTP shares in-progress DNS lookups between VUs using the same handler and
hostname/port. Successful results are cached for one second (a local reuse
window, not the authoritative DNS TTL). Up to 1,024 host/port entries are
retained. IP literals bypass DNS.

A failed or empty lookup is retried up to twice, after 100 ms and 200 ms;
each lookup has a two-second deadline. A failed lookup sequence is shared
with waiting VUs, with a one-second cooldown to avoid a tight failure loop.
The overall request timeout still bounds lookup waits, retries and connection
setup. The operating system may finish a blocking DNS lookup after its caller
times out.

TCP connections try all resolved addresses, interleaving IPv6 and IPv4.
Another address is attempted after 250 ms if a connection is still pending,
or immediately after an attempt fails. This lets IPv4-only agents recover
when DNS lists IPv6 first.

These retries happen before sending HTTP request bytes. Transport failures
after sending and HTTP error responses are returned to the caller, without
automatic HTTP replay. DNS lookup, shared-lookup wait and backoff time are
included in connection-blocked time, including returned DNS failures; returned
TCP failures also retain connection time. `http_req_duration` still measures
sending + waiting + receiving, so use iteration duration to assess the full
cost of connection setup. Overall request timeouts retain the existing timeout
timing behavior.

## TLS & mTLS

```yaml
defaults:
  http:
    tls:
      ca_file: ./internal-ca.pem        # extra trust roots (PEM, may contain several)
      cert_file: ./client.pem           # client certificate (mTLS)
      key_file: ./client-key.pem
      server_name: api.internal         # SNI override
      insecure_skip_verify: false       # accept any cert (testing only!)
      min_version: "1.2"                # pin the lowest TLS version offered
      max_version: "1.3"                # pin the highest TLS version offered
```

Roots default to the bundled Mozilla store (webpki-roots). Everything is
rustls — no OpenSSL dependency.

### TLS version pinning

`tls.min_version` and `tls.max_version` constrain which TLS versions the
handshake may negotiate. Both are strings and accept only `"1.2"` or `"1.3"`
(the `1.` prefix and a `TLSv1.` prefix are both tolerated, so `"TLSv1.3"`
works too). When neither is set the client offers TLS 1.2 and 1.3 and lets the
server pick the highest.

```yaml
defaults:
  http:
    tls:
      min_version: "1.3"     # refuse anything older than TLS 1.3
```

Pinning is useful for proving a server has dropped legacy TLS, or for forcing a
specific version while profiling. A configuration whose `min_version` is higher
than its `max_version` (so no version remains) is rejected at startup.

## Redirects, compression, proxies

- Redirects followed by default (`max_redirects: 10`); 301/302/303 switch to
  GET, 307/308 preserve method and body. Timings accumulate across hops; the
  reported `url` is the final one.
- `compression: true` sends `Accept-Encoding: gzip, deflate, br` and
  transparently decompresses. `data_received` counts wire (compressed) bytes.
- `proxy: http://host:3128` routes plaintext requests via absolute-form and
  HTTPS via `CONNECT`.

## Cookies

Automatic per-VU jars (RFC 6265 domain/path/secure/expiry matching) — see
[Requests](../yaml/requests.md#cookies).

## Response caching

`cache: true` gives each VU a browser-style HTTP cache, modelled on JMeter's
HTTP Cache Manager. Only `GET` requests are cached, and only when the response
says so:

```yaml
defaults:
  http:
    cache: true
```

The cache key is the full request URL. Behaviour per GET:

- **Fresh hit** — if a stored entry is still within its `max-age`, it is served
  straight from cache with **no network round trip**. Timings are zero and
  `bytes_sent` is `0`.
- **Revalidation** — if an entry has expired but carries a validator (`ETag`
  and/or `Last-Modified`), loadr re-requests it with `If-None-Match` /
  `If-Modified-Since`. A **`304 Not Modified`** serves the cached body and
  refreshes its freshness window; the response timings/bytes reflect the
  conditional request.
- **Store** — a `200 OK` whose `Cache-Control` allows caching (a `max-age=N`
  and no `no-store`/`private`) is stored for next time.

`Cache-Control: no-store` or `private` are never cached. Responses without a
`max-age` are not stored. The cache lives in the VU and is not shared between
VUs, so the first iteration of each VU populates it.

Each served response carries a `cache` field in its `extras` set to `hit`,
`revalidated`, or `miss`, which is handy when inspecting traffic with
`--http-debug`.

## Per-host connection overrides

`hosts` pins one or more hostnames to fixed addresses, bypassing DNS — the
equivalent of curl's `--resolve`. Use it to send
traffic at a specific node behind a load balancer, to test before DNS has
propagated, or to hit a staging box while keeping the real `Host` header.

```yaml
defaults:
  http:
    hosts:
      api.example.com: 10.0.0.42          # host          -> ip
      api.example.com:443: 10.0.0.42:8443 # host:port      -> ip:port
      cdn.example.com: 10.0.0.43:8080     # host          -> ip:port
```

Keys are matched case-insensitively. A `host:port` key matches only requests to
that exact port; a bare `host` key matches any port. When the mapped value omits
a port, the request's original port is kept. Only connection routing changes —
the URL, `Host` header, SNI and certificate validation all still use the
original hostname.

## Discarding response bodies

`discard_response_bodies: true` drops each response body as soon as it has been
read and measured. This keeps memory flat during high-throughput or long
soak runs where bodies would otherwise pile up.

```yaml
defaults:
  http:
    discard_response_bodies: true
```

Discarding happens **after** the body is fully received and decompressed, so
`data_received` and all phase timings stay accurate. Extractors and body
assertions that run on a discarded response see an empty body, so only enable
this when you are asserting on status/headers/timings rather than body content.

## Distributed tracing

`tracing: true` injects a W3C Trace Context `traceparent` header on every
request, so spans generated by loadr correlate with traces in
your backend (Jaeger, Tempo, Honeycomb, ...).

```yaml
defaults:
  http:
    tracing: true
```

A fresh `traceparent` (`00-<32-hex trace-id>-<16-hex span-id>-01`) is generated
**per request**. The trace ids only need to be unique, not cryptographically
random, so they are produced from a fast per-VU PRNG. If a request already
carries a `traceparent` header (set on the request or in `defaults.http.headers`),
loadr leaves it untouched.

## Wire-level debugging

For a verbose dump of every HTTP request and response — request line, all
headers, and a preview of the response body (first 2000 chars) — enable HTTP
debug. This is for diagnosing a single test interactively, not for load runs.

```bash
loadr run test.yaml --http-debug
```

The `--http-debug` flag sets the `LOADR_HTTP_DEBUG` environment variable, which
the HTTP handler reads on startup; setting `LOADR_HTTP_DEBUG` directly has the
same effect:

```bash
LOADR_HTTP_DEBUG=1 loadr run test.yaml
```

Output is logged under the `loadr::http_debug` target. Combined with
`cache: true`, the logged responses also show the `cache` state (`hit` /
`revalidated` / `miss`) for each GET.
