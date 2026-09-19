# Requests

The `flow:` of a scenario is a list of steps, each a single-key mapping:
`request`, `think_time`, `js`, or `group`.

```yaml
flow:
  - request:
      name: create order          # metric tag (defaults to the URL string)
      protocol: http              # inferred from URL scheme when omitted
      method: POST                # default GET (POST when a body is present)
      url: /orders                # absolute, or relative to defaults.http.base_url
      params: { source: loadtest }    # query string parameters
      headers:
        X-Idempotency-Key: "${js: crypto.uuidv4()}"
      body: ...                   # see below
      timeout: 10s                # per-request override
      follow_redirects: false     # per-request override
      tags: { endpoint: orders }  # extra metric tags
      extract: [ ... ]            # see Extraction
      assert: [ ... ]             # failures mark the request failed
      checks: [ ... ]             # recorded only
  - think_time: { type: uniform, min: 1s, max: 3s }
  - js: "session.counterAdd('orders_created', 1)"
  - group:
      name: checkout              # nested samples get group="::checkout"
      steps: [ ... ]
```

## Bodies

```yaml
body: 'raw string with ${interpolation}'
# or structured (exactly one key):
body: { json: { sku: "W-1", qty: 2, note: "${vars.note}" } }   # sets Content-Type
body: { file: ./payload.bin }                                  # loaded at start
body: { binary_base64: "${data.tx.body}" }                     # raw bytes, decoded per request
body: { form: { user: alice, pass: "${secrets.pw}" } }         # urlencoded
body:
  multipart:
    - { name: meta, value: '{"kind":"avatar"}', content_type: application/json }
    - { name: file, file: ./avatar.png, filename: avatar.png }
```

JSON bodies interpolate every string leaf; a leaf that is *only* `${expr}`
keeps its JSON type when the value parses as JSON (`"${count}"` → `7`, not
`"7"`).

## Protocol-specific blocks

Non-HTTP requests use the same step with an extra options block — see the
[protocol chapters](../protocols/http.md):

```yaml
- request: { url: wss://x/ws, ws: { send: ["hi"], receive_count: 1 } }
- request: { url: grpc://x:50051, grpc: { service: pkg.Svc, method: M, reflection: true, message: {...} } }
- request: { url: /graphql, protocol: graphql, graphql: { query: "...", variables: {...} } }
- request: { url: tcp://x:7000, socket: { send_text: "PING\n", read_bytes: 64 } }
- request: { url: postgres://u:p@db/app, sql: { query: "SELECT * FROM t WHERE id=$1", params: ["1"] } }  # needs the PostgreSQL plugin
```

> SQL is delivered as native protocol plugins, not built in: install the
> [PostgreSQL plugin](../plugins/postgres.md) (`loadr plugin install postgres`,
> advisory-clean) or the [MySQL plugin](../plugins/mysql.md)
> (`loadr plugin install mysql`) and list it under `plugins:`. The `sql:` block
> above is the same once the relevant plugin is installed (`$1, $2, …`
> placeholders for PostgreSQL, `?` for MySQL).

## Cookies

With `defaults.http.cookies: true` (the default) every VU has its own cookie
jar: `Set-Cookie` responses are stored (RFC 6265 domain/path/secure/expiry
matching) and sent automatically. Manual control is available from JS:
`session.cookieSet(url, name, value)`, `session.cookieGet(url, name)`,
`session.cookiesClear()`.
