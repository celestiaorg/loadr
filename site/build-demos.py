#!/usr/bin/env python3
"""Generate the /demos index + a detail page per demo from the DEMOS catalog.

Single source of truth for the demos section. Writes:
  site/demos/index.html            — categorised, 3-column tiled card index
  site/demos/<slug>/index.html     — full detail page per demo

Each demo maps to a real file under examples/ (the same tree browsable at
/examples/). The example is READ at build time and embedded verbatim, so the
detail page can never drift from the runnable example.

Adding a demo = append one dict to DEMOS below and re-run:
    python3 site/build-demos.py

Output is committed (so Tailwind's content scan sees the classes) and copied
to dist/demos/ by deploy.sh. The shared nav is injected later via the
<!-- INCLUDE:NAV --> marker by build-nav.py.
"""
import html
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"

# ---------------------------------------------------------------------------
# Data — one record per demo. Keys:
#   slug        URL slug under /demos/<slug>/
#   title       card + hero heading
#   category    grouping (see CATEGORY_ORDER)
#   tagline     one-liner on the card
#   description longer intro (may contain simple HTML)
#   example     path under examples/ — read & embedded verbatim at build time
#   lang        code fence language for the embedded example (yaml/js/bash…)
#   highlights  bullet list: "what to look for"
#   metrics     key metrics this demo surfaces (optional)
#   curve       load-profile shape for a small inline chart (optional):
#               flat | ramp | arrival | spike | soak | impulse
#   docs        docs link (optional)
#   video       recording basename under /videos/ (optional)
#   also        extra example files worth linking (optional list)
# ---------------------------------------------------------------------------
DEMOS = [
    # ---- Load profiles -----------------------------------------------------
    {
        "slug": "quickstart",
        "title": "The 30-second quickstart",
        "category": "Load profiles",
        "tagline": "10 VUs against one endpoint for 30s, with checks and a pass/fail threshold.",
        "description": "The smallest useful loadr test. A constant pool of virtual users hammers a single endpoint, every response is checked, and a threshold turns the run red or green — the shape every other test builds on.",
        "example": "01-quickstart.yaml",
        "curve": "flat",
        "highlights": [
            "<code class=\"text-flare\">constant-vus</code> — a fixed pool of virtual users",
            "Inline <code class=\"text-flare\">checks</code> on status and duration",
            "<code class=\"text-flare\">thresholds</code> set the process exit code for CI",
        ],
        "metrics": ["http_req_duration", "http_req_failed", "checks"],
        "docs": "/docs/getting-started/first-test.html",
        "video": "01-quickstart",
    },
    {
        "slug": "ramping-load",
        "title": "Ramping load",
        "category": "Load profiles",
        "tagline": "Climb to 50 VUs, hold, ramp down — with human-like think time.",
        "description": "The classic ramp: stage the VU count up, hold at the peak, then ease back down while think time between requests makes each VU behave like a real user. The trapezoid you'll run most often.",
        "example": "02-ramping-load.yaml",
        "curve": "ramp",
        "highlights": [
            "<code class=\"text-flare\">ramping-vus</code> with staged targets",
            "<code class=\"text-flare\">think_time</code> spaces requests like a human",
            "<code class=\"text-flare\">graceful_ramp_down</code> lets VUs finish cleanly",
        ],
        "metrics": ["http_req_duration", "http_req_failed", "vus"],
        "docs": "/docs/yaml/executors.html",
    },
    {
        "slug": "arrival-rate",
        "title": "Open-model arrival rate",
        "category": "Load profiles",
        "tagline": "Hold exactly 100 req/s regardless of how slowly the target responds.",
        "description": "Open-model load decouples throughput from response time: loadr starts a fixed number of iterations per second and adds VUs as needed to keep the rate. Watch <code class=\"text-flare\">dropped_iterations</code> to catch the moment the target can't keep up.",
        "example": "03-arrival-rate.yaml",
        "curve": "arrival",
        "highlights": [
            "<code class=\"text-flare\">constant-arrival-rate</code> — throughput, not concurrency",
            "<code class=\"text-flare\">pre_allocated_vus</code> / <code class=\"text-flare\">max_vus</code> bound the pool",
            "<code class=\"text-flare\">dropped_iterations</code> flags saturation",
        ],
        "metrics": ["http_req_duration", "dropped_iterations"],
        "docs": "/docs/yaml/executors.html",
    },
    {
        "slug": "spike-test",
        "title": "Spike test",
        "category": "Load profiles",
        "tagline": "Calm baseline, sudden 10× spike, recovery — abort if errors explode.",
        "description": "A ramping arrival rate slams the target from a quiet baseline to a 10× spike and back, then verifies recovery. An <code class=\"text-flare\">abort_on_fail</code> threshold kills the run early if the error rate blows past your limit.",
        "example": "04-spike-test.yaml",
        "curve": "spike",
        "highlights": [
            "<code class=\"text-flare\">ramping-arrival-rate</code> for a sharp spike",
            "<code class=\"text-flare\">abort_on_fail</code> + <code class=\"text-flare\">delay_abort_eval</code> stop a doomed run",
            "Recovery stages prove the service comes back",
        ],
        "metrics": ["http_req_failed", "http_req_duration"],
        "docs": "/docs/yaml/thresholds.html",
    },
    {
        "slug": "soak",
        "title": "Soak & outputs",
        "category": "Load profiles",
        "tagline": "Hold steady for the long haul and stream metrics to a time-series store.",
        "description": "A soak holds a modest load for a long window to surface leaks and slow degradation, while <code class=\"text-flare\">outputs</code> stream every metric to Prometheus / InfluxDB / OTLP so you can watch the whole run in Grafana.",
        "example": "14-outputs-and-soak.yaml",
        "curve": "soak",
        "highlights": [
            "Long, flat hold to expose leaks and drift",
            "<code class=\"text-flare\">outputs</code> fan metrics out to external stores",
            "Pairs with the Prometheus + Grafana stack demo",
        ],
        "metrics": ["http_req_duration", "http_reqs"],
        "docs": "/docs/outputs/overview.html",
    },

    # ---- Traffic modelling -------------------------------------------------
    {
        "slug": "scenario-weights",
        "title": "Weighted traffic mix",
        "category": "Traffic modelling",
        "tagline": "70% browse, 20% search, 10% checkout — one scenario, weighted branches.",
        "description": "Model real traffic shape without spinning up separate scenarios: a single flow picks one weighted branch per iteration and tags each path so per-branch metrics fall out automatically. Locust <code class=\"text-flare\">@task</code> weights, natively.",
        "example": "40-scenario-weights.yaml",
        "highlights": [
            "<code class=\"text-flare\">random: { strategy: weighted }</code> branch picker",
            "Each branch <code class=\"text-flare\">name</code> becomes a metric tag",
            "Tune the mix to match production",
        ],
        "docs": "/docs/yaml/flow.html",
    },
    {
        "slug": "flow-control",
        "title": "Flow control & weighted tasks",
        "category": "Traffic modelling",
        "tagline": "Loops, conditionals, groups and weighted tasks inside a single flow.",
        "description": "Compose realistic user journeys: repeat blocks, branch on extracted values, group related requests for reporting, and weight tasks — all declaratively in the flow.",
        "example": "16-flow-control.yaml",
        "video": "08-flow-control",
        "highlights": [
            "<code class=\"text-flare\">loop</code> / <code class=\"text-flare\">if</code> / <code class=\"text-flare\">group</code> blocks",
            "Weighted task selection",
            "Groups roll up into the report",
        ],
        "docs": "/docs/yaml/flow.html",
    },
    {
        "slug": "feeders-and-throttle",
        "title": "Feeders & throttling",
        "category": "Traffic modelling",
        "tagline": "Feed data from CSV/JSON per iteration and cap per-VU request rate.",
        "description": "Drive each iteration from a shared data feeder and throttle how fast a VU may fire, so a big pool doesn't outrun your intended per-user pace.",
        "example": "17-feeders-and-throttle.yaml",
        "video": "09-feeders-throttle",
        "highlights": [
            "Shared vs per-VU feeder modes",
            "Per-VU throttle keeps pacing realistic",
            "Deterministic replay of a dataset",
        ],
        "docs": "/docs/yaml/data.html",
    },
    {
        "slug": "data-driven",
        "title": "Data-driven load",
        "category": "Traffic modelling",
        "tagline": "Parameterise every request from a CSV/JSON dataset.",
        "description": "Point a feeder at a dataset and reference its columns with <code class=\"text-flare\">${data...}</code> — unique users, SKUs or payloads on every iteration instead of the same request over and over.",
        "example": "05-data-driven.yaml",
        "highlights": [
            "CSV / JSON feeders",
            "<code class=\"text-flare\">${data.&lt;feeder&gt;.&lt;col&gt;}</code> interpolation",
            "shared / per-VU / unique consumption modes",
        ],
        "docs": "/docs/yaml/data.html",
        "also": ["data/users.csv", "data/skus.json"],
    },
    {
        "slug": "correlation",
        "title": "Correlation & extraction",
        "category": "Traffic modelling",
        "tagline": "Pull a token/id out of one response and feed it into the next.",
        "description": "The bread and butter of stateful load tests: extract a value (JSONPath / regex / header) from a response and reuse it downstream — login → token → authorised call.",
        "example": "06-correlation.yaml",
        "highlights": [
            "<code class=\"text-flare\">extract</code> with JSONPath / regex / header",
            "Reuse via <code class=\"text-flare\">${var}</code> in later requests",
            "Per-VU variable scope",
        ],
        "docs": "/docs/yaml/extract.html",
    },

    # ---- Validation & CI ---------------------------------------------------
    {
        "slug": "functional-test",
        "title": "Functional / smoke test",
        "category": "Validation & CI",
        "tagline": "One pass over a case table; fail the run on any failed assertion.",
        "description": "loadr doubles as a functional tester: walk a table of cases once (feeder with <code class=\"text-flare\">on_eof: stop</code>), assert each response, and exit non-zero if a single check fails. Drop it into CI as a contract / smoke gate — no separate framework.",
        "example": "41-functional-test.yaml",
        "video": "02-validation",
        "highlights": [
            "Table-driven cases from a feeder",
            "<code class=\"text-flare\">on_eof: stop</code> = exactly one pass",
            "<code class=\"text-flare\">checks: rate&gt;=1.0</code> → exit 99 on any failure",
        ],
        "metrics": ["checks"],
        "docs": "/docs/yaml/checks.html",
    },
    {
        "slug": "check-chains",
        "title": "JMESPath & fused check-chains",
        "category": "Validation & CI",
        "tagline": "Chain extract-and-assert steps that short-circuit on the first miss.",
        "description": "Fuse extraction and assertion into a chain: each link pulls a value and checks it, and the chain stops at the first failure with a precise cause — richer than a flat list of checks.",
        "example": "25-check-chains.yaml",
        "video": "12-check-chains",
        "highlights": [
            "JMESPath over JSON bodies",
            "Fused extract + check links",
            "Short-circuits with a clear failure reason",
        ],
        "metrics": ["checks"],
        "docs": "/docs/yaml/checks.html",
    },
    {
        "slug": "failure-breakdown",
        "title": "Failure breakdown, by cause",
        "category": "Validation & CI",
        "tagline": "Group failures by root cause instead of one undifferentiated error count.",
        "description": "When things break, loadr buckets failures by cause — transport errors, timeouts, bad statuses, failed checks — so you see <em>why</em> the run went red, not just that it did.",
        "example": "26-failure-breakdown.yaml",
        "video": "13-failure-breakdown",
        "highlights": [
            "Failures grouped by normalized cause",
            "Separates transport vs assertion failures",
            "Feeds the HTML report's breakdown",
        ],
        "docs": "/docs/reporting/overview.html",
        "also": ["scripts/failure-breakdown.js"],
    },
    {
        "slug": "tags",
        "title": "Tags & per-route metrics",
        "category": "Validation & CI",
        "tagline": "Tag requests and scenarios to slice every metric by route or group.",
        "description": "Attach tags to requests, groups and scenarios and every metric breaks down along them — p95 per endpoint, error rate per journey — without extra scenarios.",
        "example": "23-tags.yaml",
        "highlights": [
            "Arbitrary key/value tags",
            "Per-tag metric breakdowns",
            "Great for dashboards and thresholds",
        ],
        "docs": "/docs/yaml/tags.html",
    },

    # ---- Auth, sessions & uploads -----------------------------------------
    {
        "slug": "auth-tokens",
        "title": "Per-VU auth tokens + refresh",
        "category": "Auth, sessions & uploads",
        "tagline": "Each VU mints its own token and silently refreshes it before expiry.",
        "description": "Each VU mints a token the first time it needs one, attaches it to every request, and transparently re-mints it just before expiry — with a custom <code class=\"text-flare\">token_refreshes</code> counter tracking how often. Swap the synthetic mint for a call to your auth server.",
        "example": "36-auth-tokens.yaml",
        "highlights": [
            "<code class=\"text-flare\">beforeRequest</code> JS hook mints/refreshes",
            "Per-VU token state",
            "Custom <code class=\"text-flare\">counter</code> metric for refreshes",
        ],
        "metrics": ["token_refreshes"],
        "docs": "/docs/js/api.html",
        "also": ["scripts/auth.js"],
    },
    {
        "slug": "cookies-session",
        "title": "Cookie-based sessions",
        "category": "Auth, sessions & uploads",
        "tagline": "A per-VU cookie jar carries a login session across the whole flow.",
        "description": "loadr keeps a per-VU cookie jar on by default: a login response's <code class=\"text-flare\">Set-Cookie</code> is stored and replayed on every later request, so server-side sessions just work. You can also read/write/clear cookies from JS.",
        "example": "38-cookies-session.yaml",
        "highlights": [
            "Automatic per-VU cookie jar",
            "<code class=\"text-flare\">Set-Cookie</code> stored and replayed",
            "<code class=\"text-flare\">session.cookieSet/Get/clear</code> from JS",
        ],
        "docs": "/docs/js/api.html",
    },
    {
        "slug": "file-uploads",
        "title": "Multipart file uploads",
        "category": "Auth, sessions & uploads",
        "tagline": "POST a multipart/form-data body mixing literal fields with a file part.",
        "description": "Stream a file from disk as one part of a <code class=\"text-flare\">multipart/form-data</code> body alongside literal fields, extract the returned id, and confirm it — with full <code class=\"text-flare\">${...}</code> interpolation on values.",
        "example": "37-file-uploads.yaml",
        "highlights": [
            "<code class=\"text-flare\">multipart</code> body with a <code class=\"text-flare\">file:</code> part",
            "Per-part <code class=\"text-flare\">filename</code> / <code class=\"text-flare\">content_type</code>",
            "Extract + reuse the returned id",
        ],
        "docs": "/docs/yaml/requests.html",
        "also": ["data/upload.txt"],
    },
    {
        "slug": "http-advanced",
        "title": "Advanced HTTP",
        "category": "Auth, sessions & uploads",
        "tagline": "Redirects, retries, timeouts, headers and body shapes in depth.",
        "description": "The full HTTP surface: custom headers, redirect policy, per-request timeouts, form / JSON / raw bodies and response handling — everything beyond the happy path.",
        "example": "21-http-advanced.yaml",
        "highlights": [
            "Redirect + timeout control",
            "form / json / raw / multipart bodies",
            "Header and response tuning",
        ],
        "docs": "/docs/yaml/requests.html",
    },

    # ---- Protocols ---------------------------------------------------------
    {
        "slug": "websocket",
        "title": "WebSockets",
        "category": "Protocols",
        "tagline": "Open a socket, send and receive frames, assert on messages.",
        "example": "09-websocket.yaml",
        "description": "Drive a persistent WebSocket connection: connect, send frames, await messages and assert on them — full-duplex load, not just request/response.",
        "highlights": ["Persistent full-duplex connection", "Send / receive frame steps", "Assertions on messages"],
        "docs": "/docs/protocols/websocket.html",
    },
    {
        "slug": "grpc",
        "title": "gRPC",
        "category": "Protocols",
        "tagline": "Unary and streaming gRPC calls from a .proto, no codegen step.",
        "example": "10-grpc.yaml",
        "description": "Call gRPC services straight from a <code class=\"text-flare\">.proto</code> — unary and streaming — with loadr compiling the descriptors at runtime. No generated stubs to build or check in.",
        "highlights": ["Reflection or <code class=\"text-flare\">.proto</code> source", "Unary + streaming", "Message assertions"],
        "docs": "/docs/protocols/grpc.html",
        "also": ["protos"],
    },
    {
        "slug": "graphql",
        "title": "GraphQL",
        "category": "Protocols",
        "tagline": "Queries and mutations with variables, over HTTP.",
        "example": "11-graphql.yaml",
        "description": "Send GraphQL queries and mutations with variables and assert on the JSON <code class=\"text-flare\">data</code> — a thin, ergonomic layer over the HTTP engine.",
        "highlights": ["Query + mutation with variables", "Assert on <code class=\"text-flare\">data</code> / <code class=\"text-flare\">errors</code>", "Reuse HTTP auth + cookies"],
        "docs": "/docs/protocols/graphql.html",
    },
    {
        "slug": "tcp-udp",
        "title": "Raw TCP & UDP",
        "category": "Protocols",
        "tagline": "Send bytes over a raw socket and match the response.",
        "example": "12-tcp-udp.yaml",
        "description": "Go below HTTP: open a raw TCP or UDP socket, write bytes, read the reply and assert on it — for custom or binary protocols.",
        "highlights": ["Raw TCP + UDP", "Byte-level send / match", "Custom protocol load"],
        "docs": "/docs/protocols/tcp-udp.html",
    },
    {
        "slug": "sse",
        "title": "Server-Sent Events",
        "category": "Protocols",
        "tagline": "Subscribe to an event stream and assert on events over time.",
        "example": "18-sse.yaml",
        "description": "Hold an SSE subscription open and assert on the events that arrive — streaming reads without the full WebSocket dance.",
        "highlights": ["Long-lived SSE subscription", "Per-event assertions", "Streaming read load"],
        "docs": "/docs/protocols/sse.html",
    },
    {
        "slug": "soap",
        "title": "SOAP / XML",
        "category": "Protocols",
        "tagline": "Raw SOAP envelope in, XPath extraction and assertions out.",
        "example": "42-soap.yaml",
        "description": "Send a raw SOAP 1.1 envelope as the body and pull values back out of the XML with XPath extractors and checks — <code class=\"text-flare\">local-name()</code> keeps it namespace-agnostic. No custom function needed.",
        "highlights": ["Raw XML envelope body", "<code class=\"text-flare\">xpath</code> extractors + checks", "Namespace-agnostic matching"],
        "docs": "/docs/yaml/extract.html",
    },
    {
        "slug": "twirp",
        "title": "Twirp RPC",
        "category": "Protocols",
        "tagline": "Twirp over HTTP/JSON — a call is just a POST, no codegen.",
        "example": "43-twirp.yaml",
        "description": "Twirp speaks plain HTTP: POST to <code class=\"text-flare\">/twirp/&lt;Service&gt;/&lt;Method&gt;</code> with a JSON body. loadr's JSON body is exactly Twirp's JSON mode, so a Twirp call is just an HTTP request — extract and assert as usual.",
        "highlights": ["JSON-mode Twirp = plain POST", "Extract fields with JSONPath", "protobuf mode via a file body"],
        "docs": "/docs/protocols/http.html",
    },
    {
        "slug": "browser",
        "title": "Browser (headless)",
        "category": "Protocols",
        "tagline": "Drive a real headless browser page for front-end journeys.",
        "example": "20-browser.yaml",
        "description": "Beyond the protocol layer: script a real headless browser — navigate, interact and assert on the rendered page — to load-test front-end journeys end to end.",
        "highlights": ["Real headless browser", "Navigate + interact steps", "Page-level assertions"],
        "docs": "/docs/protocols/browser.html",
    },

    # ---- Databases & messaging --------------------------------------------
    {
        "slug": "postgres",
        "title": "PostgreSQL",
        "category": "Databases & messaging",
        "tagline": "Parameterised SELECT / INSERT / UPDATE — the query is the request.",
        "example": "27-postgres.yaml",
        "description": "Load-test PostgreSQL directly: the parameterised query <em>is</em> the request, bound safely with <code class=\"text-flare\">$1, $2…</code> and pooled per VU. Install with <code class=\"text-flare\">loadr plugin install postgres</code>.",
        "highlights": ["Parameterised SQL", "Per-VU connection pool", "Runtime plugin — not in the binary"],
        "metrics": ["postgres_reqs", "postgres_req_duration"],
        "docs": "/docs/plugins/postgres.html",
        "video": "16-postgres-plugin",
    },
    {
        "slug": "mongo",
        "title": "MongoDB",
        "category": "Databases & messaging",
        "tagline": "insert / find / update / aggregate — one operation per request.",
        "example": "28-mongo.yaml",
        "description": "Drive MongoDB on the official driver: one operation per request. Install with <code class=\"text-flare\">loadr plugin install mongo</code>.",
        "highlights": ["insert / find / update / aggregate", "Official Mongo driver", "Runtime plugin"],
        "metrics": ["mongo_reqs", "mongo_req_duration"],
        "docs": "/docs/plugins/mongo.html",
        "video": "15-mongo-plugin",
    },
    {
        "slug": "mysql",
        "title": "MySQL",
        "category": "Databases & messaging",
        "tagline": "One query per request over the MySQL wire protocol.",
        "example": "29-mysql.yaml",
        "description": "Drive MySQL over its wire protocol with placeholders bound by the driver and a pool per VU. Install with <code class=\"text-flare\">loadr plugin install mysql</code>.",
        "highlights": ["Wire-protocol queries", "Safe placeholder binding", "Runtime plugin"],
        "metrics": ["mysql_reqs", "mysql_req_duration"],
        "docs": "/docs/plugins/mysql.html",
        "video": "17-mysql-plugin",
    },
    {
        "slug": "redis",
        "title": "Redis",
        "category": "Databases & messaging",
        "tagline": "Speak RESP over TCP — SET, GET, INCR, EXPIRE, one command per request.",
        "example": "30-redis.yaml",
        "description": "Speak RESP directly over TCP, one command per request in argv form. Pure Rust — no redis-crate quirks. Install with <code class=\"text-flare\">loadr plugin install redis</code>.",
        "highlights": ["RESP argv commands", "Pure-Rust client", "Runtime plugin"],
        "metrics": ["redis_reqs", "redis_req_duration"],
        "docs": "/docs/plugins/redis.html",
        "video": "18-redis-plugin",
    },
    {
        "slug": "kafka",
        "title": "Apache Kafka",
        "category": "Databases & messaging",
        "tagline": "Produce and fetch against a broker — pure Rust, no librdkafka.",
        "example": "31-kafka.yaml",
        "description": "Produce and fetch against Kafka, one operation per request, on the pure-Rust <code class=\"text-flare\">rskafka</code> client — no C toolchain. Install with <code class=\"text-flare\">loadr plugin install kafka</code>.",
        "highlights": ["produce / fetch", "Pure-Rust client", "Runtime plugin"],
        "metrics": ["kafka_reqs", "kafka_req_duration"],
        "docs": "/docs/plugins/kafka.html",
        "video": "19-kafka-plugin",
    },
    {
        "slug": "rabbitmq",
        "title": "RabbitMQ",
        "category": "Databases & messaging",
        "tagline": "Publish and consume over AMQP 0.9.1, declaring queues inline.",
        "example": "32-rabbitmq.yaml",
        "description": "Publish and get against an AMQP 0.9.1 broker on the pure-Rust <code class=\"text-flare\">lapin</code> client. Install with <code class=\"text-flare\">loadr plugin install rabbitmq</code>.",
        "highlights": ["publish / get / ack", "Inline queue declaration", "Runtime plugin"],
        "metrics": ["rabbitmq_reqs", "rabbitmq_req_duration"],
        "docs": "/docs/plugins/rabbitmq.html",
        "video": "20-rabbitmq-plugin",
    },
    {
        "slug": "elasticsearch",
        "title": "Elasticsearch",
        "category": "Databases & messaging",
        "tagline": "index / get / search / bulk over the HTTP/JSON API.",
        "example": "33-elasticsearch.yaml",
        "description": "Drive Elasticsearch over its HTTP/JSON API on loadr's own hyper stack — no heavy official client. Install with <code class=\"text-flare\">loadr plugin install elasticsearch</code>.",
        "highlights": ["index / get / search / bulk", "hyper + rustls transport", "Runtime plugin"],
        "metrics": ["elasticsearch_reqs", "elasticsearch_req_duration"],
        "docs": "/docs/plugins/elasticsearch.html",
        "video": "21-elasticsearch-plugin",
    },

    # ---- Scripting & metrics ----------------------------------------------
    {
        "slug": "javascript",
        "title": "JavaScript in the flow",
        "category": "Scripting & metrics",
        "tagline": "Drop into JS for logic the YAML doesn't cover — right inside a flow.",
        "example": "08-javascript.yaml",
        "description": "When declarative YAML isn't enough, run JavaScript inline: build dynamic payloads, branch on logic, sign requests — on loadr's embedded QuickJS runtime, no Node required.",
        "example_lang": "yaml",
        "highlights": ["Embedded QuickJS — no Node", "Inline <code class=\"text-flare\">js:</code> steps + hooks", "Full request/response access"],
        "docs": "/docs/js/api.html",
        "also": ["scripts/helpers.js"],
    },
    {
        "slug": "custom-metrics",
        "title": "Custom metrics",
        "category": "Scripting & metrics",
        "tagline": "Declare counter / rate / trend / gauge metrics and emit from JS.",
        "description": "Track what matters to you — revenue booked, cache-hit rate, a bespoke latency, queue depth — by declaring metrics and emitting to them from JS, then gating on them with thresholds like any built-in.",
        "example": "39-custom-metrics.yaml",
        "highlights": [
            "counter / rate / trend / gauge kinds",
            "<code class=\"text-flare\">session.counterAdd/rateAdd/trendAdd/gaugeSet</code>",
            "Threshold on custom metrics",
        ],
        "metrics": ["revenue_usd", "cache_hit", "checkout_latency"],
        "docs": "/docs/js/api.html",
        "also": ["scripts/custom-metrics.js"],
    },
    {
        "slug": "lifecycle",
        "title": "Lifecycle hooks",
        "category": "Scripting & metrics",
        "tagline": "setup / teardown and per-VU init around the load.",
        "example": "22-lifecycle.yaml",
        "description": "Run code once before the load (seed data, fetch a shared token), once after (clean up, assert totals), and per-VU on init — the test lifecycle, scripted.",
        "highlights": ["<code class=\"text-flare\">setup</code> / <code class=\"text-flare\">teardown</code> once per run", "Per-VU init", "Share state into the flow"],
        "docs": "/docs/js/api.html",
        "also": ["scripts/lifecycle.js"],
    },

    # ---- Scale & operations -----------------------------------------------
    {
        "slug": "distributed",
        "title": "Distributed fleet",
        "category": "Scale & operations",
        "tagline": "One controller fans a single plan out across many agents.",
        "example": "15-distributed.yaml",
        "description": "Scale past one box: a controller splits the load across a fleet of agents and merges their HDR histograms into one true result — accurate percentiles, not an average of averages.",
        "highlights": ["Controller + agent fleet", "HDR histogram merge", "One plan, many machines"],
        "docs": "/docs/distributed/overview.html",
        "video": "06-distributed",
    },
    {
        "slug": "environments",
        "title": "Environments",
        "category": "Scale & operations",
        "tagline": "One plan, many targets — swap base URLs and secrets per environment.",
        "example": "13-environments.yaml",
        "description": "Promote the same plan from local to staging to prod by switching an environment: base URLs, headers and secrets come from the environment, not the flow.",
        "highlights": ["Per-environment config", "Env vars + secrets", "Same plan everywhere"],
        "docs": "/docs/yaml/environments.html",
    },
    {
        "slug": "scenarios-and-groups",
        "title": "Scenarios & groups",
        "category": "Scale & operations",
        "tagline": "Run several named scenarios at once and roll requests into groups.",
        "example": "07-scenarios-and-groups.yaml",
        "description": "Compose a full workload from multiple named scenarios running together, each with its own executor, and group related requests so the report reads like the user journey.",
        "highlights": ["Multiple concurrent scenarios", "Per-scenario executors", "Grouped reporting"],
        "docs": "/docs/yaml/scenarios.html",
    },
    {
        "slug": "timeseries-report",
        "title": "Time-series HTML report",
        "category": "Scale & operations",
        "tagline": "A self-contained HTML report with the whole run's curves.",
        "example": "24-timeseries-report.yaml",
        "description": "Every run can emit a single self-contained HTML report — throughput, latency percentiles and errors over time, plus the failure breakdown — with no external services to host.",
        "highlights": ["Self-contained HTML", "Percentiles + throughput over time", "Failure breakdown built in"],
        "docs": "/docs/reporting/overview.html",
        "video": "11-timeseries-report",
    },

    # ---- Ops integrations --------------------------------------------------
    {
        "slug": "prometheus-grafana",
        "title": "Prometheus + Grafana",
        "category": "Ops integrations",
        "tagline": "Stream metrics to Prometheus and watch the run in a ready-made dashboard.",
        "example": "prometheus-grafana/loadr.yaml",
        "description": "Point loadr's Prometheus output at your stack and drop in the bundled Grafana dashboard to watch throughput, latency and errors live — a full docker-compose stack is included to try it end to end.",
        "highlights": ["Prometheus remote-write / scrape", "Bundled Grafana dashboard JSON", "docker-compose stack to try locally"],
        "docs": "/docs/outputs/prometheus.html",
        "also": ["prometheus-grafana/docker-compose.yml", "prometheus-grafana/grafana-dashboard.json", "prometheus-grafana/README.md"],
    },
    {
        "slug": "cicd",
        "title": "CI/CD performance gate",
        "category": "Ops integrations",
        "tagline": "A perf smoke test wired into GitHub Actions that fails the build on regression.",
        "example": "cicd/perf-smoke.yaml",
        "video": "04-ci-gate",
        "description": "Run a fast perf smoke on every PR: thresholds set the exit code, so a latency or error regression turns the build red. Includes a ready-to-copy GitHub Actions workflow.",
        "example_lang": "yaml",
        "highlights": ["Threshold-gated exit code", "Copy-paste GitHub Actions job", "Fast enough for every PR"],
        "docs": "/docs/guides/ci.html",
        "also": ["cicd/github-actions.yml", "cicd/README.md"],
    },
    {
        "slug": "kubernetes",
        "title": "Kubernetes",
        "category": "Ops integrations",
        "tagline": "Run the controller + agent fleet as a Kubernetes Job.",
        "example": "k8s/run-job.yaml",
        "description": "Take the distributed fleet to Kubernetes: manifests for the controller, agents and a run Job, plus a Dockerfile — scale load generation with your cluster.",
        "example_lang": "yaml",
        "highlights": ["Controller + agents as workloads", "One-shot run Job", "Bundled Dockerfile"],
        "docs": "/docs/distributed/overview.html",
        "also": ["k8s/controller.yaml", "k8s/agents.yaml", "k8s/README.md"],
    },

    # ---- Extending loadr ---------------------------------------------------
    {
        "slug": "write-plugin-go",
        "title": "Write a plugin in Go",
        "category": "Extending loadr",
        "tagline": "Ship a new protocol as a native plugin — here, a Go echo server driver.",
        "example": "35-go-echo.yaml",
        "description": "The plugin model is open: implement a protocol in Go (or any language that builds a C-ABI shared object), declare its URL scheme, and <code class=\"text-flare\">loadr plugin install</code> it. This demo drives a Go echo service through a custom plugin.",
        "highlights": ["Native (C-ABI) plugin", "Declare a URL scheme", "Install like any other plugin"],
        "docs": "/docs/plugins/developing.html",
        "video": "22-go-plugin",
    },
    {
        "slug": "native-protocol-c",
        "title": "A native protocol in C",
        "category": "Extending loadr",
        "tagline": "Drive a C echo server through a hand-written native protocol plugin.",
        "example": "34-c-echo.yaml",
        "description": "The lowest-level extension point: a native protocol plugin written against the C ABI, driving a simple echo server — proof that anything that speaks bytes can become a loadr protocol.",
        "highlights": ["C-ABI protocol plugin", "Byte-level driver", "Same install/verify path"],
        "docs": "/docs/plugins/developing.html",
    },
    # ---- Adopt loadr -------------------------------------------------------
    {
        "slug": "convert",
        "title": "Import from k6 & JMeter",
        "category": "Adopt loadr",
        "tagline": "Convert an existing k6 script or JMeter .jmx into a loadr plan.",
        "description": "Already invested in k6 or JMeter? <code class=\"text-flare\">loadr convert</code> translates a k6 script or a JMeter <code class=\"text-flare\">.jmx</code> into an equivalent loadr YAML plan you can run and refine \u2014 no rewrite from scratch, no lock-in either way.",
        "command": "loadr convert k6-script.js -o plan.yaml",
        "highlights": [
            "k6 JavaScript and JMeter <code class=\"text-flare\">.jmx</code> in",
            "A readable loadr YAML plan out",
            "Run, diff and refine the result",
        ],
        "docs": "/docs/convert/overview.html",
        "video": "05-convert",
    },
    {
        "slug": "web-ui",
        "title": "The live Web UI",
        "category": "Adopt loadr",
        "tagline": "Drive a run from the browser and watch percentiles, throughput and errors live.",
        "description": "Add <code class=\"text-flare\">--ui</code> to any run and loadr serves a local live dashboard \u2014 start/stop, live req/s, p95 and error rate, and a streaming timeline. The same engine, in the browser. No account, nothing phones home.",
        "command": "loadr run examples/01-quickstart.yaml --ui",
        "highlights": [
            "One flag: <code class=\"text-flare\">--ui</code>",
            "Live req/s, p95 and error rate",
            "Served locally \u2014 zero telemetry",
        ],
        "docs": "/docs/webui/overview.html",
        "video": "07-webui",
    },
    {
        "slug": "agent-fleet",
        "title": "The agent fleet, in the UI",
        "category": "Adopt loadr",
        "tagline": "Watch a distributed controller + agents drive one run from the Web UI.",
        "description": "Point the Web UI at a distributed run and watch the whole fleet at once \u2014 each agent's contribution and the merged, true-percentile result. The distributed engine with a live face.",
        "command": "loadr controller --ui-bind 127.0.0.1:6464",
        "highlights": [
            "Controller + agents in one view",
            "Merged HDR percentiles, live",
            "Acknowledged controls + fleet completeness",
        ],
        "docs": "/docs/distributed/overview.html",
        "video": "10-agent-fleet",
    },

    # ---- Authoring & analysis ----------------------------------------------
    {
        "slug": "record",
        "title": "Record a session into a scenario",
        "category": "Authoring & analysis",
        "tagline": "A capturing HTTP(S) proxy turns a real journey into a correlated scenario.",
        "description": "Point a browser or app at <code class=\"text-flare\">loadr record</code> and it writes a ready-to-run scenario — with tokens, CSRF and ids <strong class=\"text-ash\">auto-correlated</strong> from a response into later requests. HTTPS is captured via an install-once local CA. No devtools HAR export.",
        "command": "loadr record -o session.yaml",
        "highlights": [
            "HTTP + HTTPS (MITM via a local CA)",
            "Dynamic values auto-correlated into <code class=\"text-flare\">extract</code> + <code class=\"text-flare\">${...}</code>",
            "Static assets dropped automatically",
        ],
        "docs": "/docs/guides/record.html",
        "video": "54-record",
    },
    {
        "slug": "gen",
        "title": "Generate a test from an API contract",
        "category": "Authoring & analysis",
        "tagline": "OpenAPI, Postman, GraphQL or gRPC in — a runnable, fuzzed scenario out.",
        "description": "<code class=\"text-flare\">loadr gen</code> reads an API contract and writes one request per operation with schema-derived example data. <code class=\"text-flare\">--fuzz</code> adds spec-invalid and adversarial variants gated on <em class=\"text-ash\">never a 5xx</em>.",
        "example": "specs/petstore.yaml",
        "example_lang": "yaml",
        "command": "loadr gen openapi specs/petstore.yaml -o plan.yaml",
        "highlights": [
            "openapi · postman · graphql · grpc",
            "Params &amp; bodies from the schema; <code class=\"text-flare\">$ref</code> resolved",
            "<code class=\"text-flare\">--fuzz</code> = a robustness suite from the contract",
        ],
        "docs": "/docs/guides/gen.html",
        "video": "51-gen",
    },
    {
        "slug": "explain",
        "title": "Explain a run in plain language",
        "category": "Authoring & analysis",
        "tagline": "A deterministic root-cause read of any run — no model, no network.",
        "description": "<code class=\"text-flare\">loadr explain</code> reads a run's summary and reports the threshold verdict, error rate, latency tail, and a heuristic likely cause — saturation vs a slow path vs a healthy run.",
        "command": "loadr explain summary.json",
        "highlights": [
            "Latency tail: p99 vs the median",
            "Saturation vs slow-path heuristic",
            "The offline half of the AI copilot",
        ],
        "docs": "/docs/guides/explain.html",
        "video": "52-explain",
    },
    {
        "slug": "history",
        "title": "Catch regressions statistically",
        "category": "Authoring & analysis",
        "tagline": "A local run history + a robust MAD z-score against a window, not one baseline.",
        "description": "<code class=\"text-flare\">loadr history</code> records every run in SQLite and flags regressions with a robust median/MAD z-score across the last N runs — so one noisy CI run can't false-alarm. Exit 99 gates the merge.",
        "command": "loadr history check summary.json --plan checkout",
        "highlights": [
            "Median + MAD modified z-score",
            "Cold-start &amp; identical-history guardrails",
            "Exit 99 gates the merge",
        ],
        "docs": "/docs/guides/history.html",
        "video": "53-history",
    },
]

CATEGORY_ORDER = [
    "Load profiles",
    "Traffic modelling",
    "Validation & CI",
    "Authoring & analysis",
    "Auth, sessions & uploads",
    "Protocols",
    "Databases & messaging",
    "Scripting & metrics",
    "Scale & operations",
    "Ops integrations",
    "Adopt loadr",
    "Extending loadr",
]
CATEGORY_BLURB = {
    "Load profiles": "The shapes of load — constant, ramping, arrival-rate, spike and soak.",
    "Traffic modelling": "Make synthetic traffic look real — weighted mixes, feeders, correlation.",
    "Validation & CI": "Turn a load test into a pass/fail gate your pipeline can trust.",
    "Authoring & analysis": "Write plans from a recording, a contract or a sentence — then have loadr read the result back to you.",
    "Auth, sessions & uploads": "Tokens, cookies, multipart uploads and the deeper HTTP surface.",
    "Protocols": "Everything past HTTP — WebSocket, gRPC, GraphQL, SSE, SOAP, Twirp, browser.",
    "Databases & messaging": "Drive datastores and brokers at their native protocol via runtime plugins.",
    "Scripting & metrics": "Reach for JavaScript and custom metrics when YAML isn't enough.",
    "Scale & operations": "Distribute the load, target any environment, and report on it.",
    "Ops integrations": "Wire loadr into Prometheus/Grafana, CI/CD and Kubernetes.",
    "Adopt loadr": "Bring existing k6/JMeter tests across and drive loadr from the live Web UI.",
    "Extending loadr": "Ship your own protocols and outputs as plugins.",
}
CATEGORY_ICON = {
    "Load profiles": "M3 17l5-6 4 4 5-8", "Traffic modelling": "M4 6h16M4 12h10M4 18h7",
    "Validation & CI": "M20 6L9 17l-5-5", "Auth, sessions & uploads": "M12 2l7 4v6c0 5-3 7-7 8-4-1-7-3-7-8V6z",
    "Protocols": "M8 3v4M16 3v4M4 11h16M6 21h12", "Databases & messaging": "M12 3c4 0 7 1.3 7 3v12c0 1.7-3 3-7 3s-7-1.3-7-3V6c0-1.7 3-3 7-3z",
    "Scripting & metrics": "M8 9l-4 3 4 3M16 9l4 3-4 3", "Scale & operations": "M3 12h4l3 8 4-16 3 8h4",
    "Ops integrations": "M12 3v6m0 6v6M3 12h6m6 0h6", "Extending loadr": "M12 5v14M5 12h14",
    "Authoring & analysis": "M12 20h9M16.5 3.5a2.1 2.1 0 013 3L7 19l-4 1 1-4z",
    "Adopt loadr": "M12 3v12m0 0l4-4m-4 4l-4-4M4 21h16",
}


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------
def esc(s):
    return html.escape(s, quote=False)


def read_example(rel):
    path = EXAMPLES / rel
    return esc(path.read_text(encoding="utf-8").rstrip("\n"))


def head(title, desc, canonical):
    return f"""<!doctype html>
<html lang="en" class="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="/assets/consent.js"></script>
<title>{esc(title)}</title>
<meta name="description" content="{esc(desc)}">
<meta property="og:title" content="{esc(title)}">
<meta property="og:description" content="{esc(desc)}">
<meta property="og:url" content="{canonical}">
<meta property="og:type" content="website">
<link rel="canonical" href="{canonical}">
<link rel="icon" type="image/png" sizes="64x64" href="/assets/favicon-64.png">
<link rel="apple-touch-icon" href="/assets/apple-touch-icon.png">
<link rel="stylesheet" href="/assets/site.css">
</head>
<body class="antialiased overflow-x-clip">

<!-- INCLUDE:NAV -->
"""

FOOTER = """
</main>

<footer class="border-t border-edge/60 bg-coal/40 py-14">
  <div class="mx-auto max-w-7xl px-5">
    <div class="grid gap-10 md:grid-cols-4">
      <div>
        <div class="flex items-center gap-2">
          <img src="/assets/logo-mark.png" width="20" height="20" alt="" />
          <span class="font-extrabold text-white">loadr<span class="text-ember">.io</span></span>
        </div>
        <p class="mt-3 text-sm text-smoke">Break things on purpose.<br>© 2026 loadr. All rights reserved.</p>
      </div>
      <div>
        <h4 class="text-xs font-bold uppercase tracking-wider text-smoke">Product</h4>
        <ul class="mt-3 space-y-2 text-sm text-smoke">
          <li><a class="hover:text-flare" href="/#features">Features</a></li>
          <li><a class="hover:text-flare" href="/#protocols">Protocols</a></li>
          <li><a class="hover:text-flare" href="/plugins/">Plugins</a></li>
          <li><a class="hover:text-flare" href="/demos/">Demos</a></li>
          <li><a class="hover:text-flare" href="/download/">Download</a></li>
        </ul>
      </div>
      <div>
        <h4 class="text-xs font-bold uppercase tracking-wider text-smoke">Documentation</h4>
        <ul class="mt-3 space-y-2 text-sm text-smoke">
          <li><a class="hover:text-flare" href="/docs/getting-started/first-test.html">Your first test</a></li>
          <li><a class="hover:text-flare" href="/docs/yaml/overview.html">YAML reference</a></li>
          <li><a class="hover:text-flare" href="/docs/js/api.html">JS API</a></li>
          <li><a class="hover:text-flare" href="/examples/">All examples</a></li>
        </ul>
      </div>
      <div>
        <h4 class="text-xs font-bold uppercase tracking-wider text-smoke">Source</h4>
        <ul class="mt-3 space-y-2 text-sm text-smoke">
          <li><a class="hover:text-flare" href="/download/">Download</a></li>
          <li><a class="hover:text-flare" href="https://github.com/levantar-ai/loadr" rel="noopener">Source (GitHub)</a></li>
          <li><a class="hover:text-flare" href="https://github.com/levantar-ai/loadr/releases" rel="noopener">Releases</a></li>
          <li><a class="hover:text-flare" href="https://github.com/levantar-ai/loadr/blob/main/LICENSE" rel="noopener">License (Elastic 2.0)</a></li>
        </ul>
      </div>
    </div>
    <div class="mt-12 border-t border-edge/60 pt-6 text-xs text-smoke">
      © 2026 loadr. Built in Rust. The loadr tool ships zero telemetry — it never phones home. This site uses privacy-first, consent-based analytics only. · <a class="hover:text-flare" href="/privacy/">Privacy</a> · <a class="hover:text-flare" href="/cookies/">Cookies</a>
    </div>
  </div>
</footer>

<script src="/assets/site.js" defer></script>
</body>
</html>
"""

# Small inline load-profile charts (viewBox 0 0 220 64). Points are the VU/rate
# curve over the run; the area under it is lightly filled.
CURVES = {
    "flat":    [(0, 52), (14, 18), (220, 18)],
    "arrival": [(0, 50), (16, 22), (220, 22)],
    "ramp":    [(0, 56), (74, 14), (150, 14), (220, 56)],
    "spike":   [(0, 46), (70, 46), (86, 8), (120, 8), (136, 46), (220, 46)],
    "soak":    [(0, 50), (18, 26), (202, 26), (220, 30)],
    "impulse": [(0, 58), (5, 9), (220, 9)],
}
CURVE_LABEL = {
    "flat": "constant VUs", "arrival": "constant arrival rate", "ramp": "ramping VUs",
    "spike": "spike", "soak": "soak", "impulse": "impulse (cold start)",
}


def curve_svg(kind):
    pts = CURVES[kind]
    line = " ".join(f"{x},{y}" for x, y in pts)
    area = f"0,64 {line} 220,64"
    return (
        '<svg viewBox="0 0 220 64" class="h-16 w-full" preserveAspectRatio="none" aria-hidden="true">'
        f'<polygon points="{area}" fill="#fd1e2e" fill-opacity="0.10"></polygon>'
        f'<polyline points="{line}" fill="none" stroke="#fd1e2e" stroke-width="2" '
        'stroke-linejoin="round" stroke-linecap="round"></polyline>'
        '</svg>'
    )


def cat_slug(cat):
    """A stable anchor id for a category heading ('Auth, sessions & uploads' -> 'auth-sessions-uploads')."""
    s = cat.lower().replace("&", " ").replace(",", " ")
    return "-".join(s.split())


def demos_by_cat():
    by_cat = {}
    for d in DEMOS:
        by_cat.setdefault(d["category"], []).append(d)
    return by_cat


def nav_link(d, active):
    """One demo entry in the left-hand nav — a pill, with a bar + rail dot when active."""
    if active:
        return (
            '<li data-demo-item>'
            f'<a aria-current="page" href="/demos/{d["slug"]}/" '
            'class="relative flex items-start gap-2.5 rounded-lg bg-gradient-to-r from-blood/25 via-blood/10 '
            'to-transparent py-1.5 pl-3.5 pr-2 text-sm font-semibold text-white ring-1 ring-inset ring-blood/25">'
            '<span class="absolute left-0 top-1/2 h-5 w-[3px] -translate-y-1/2 rounded-r-full bg-blood '
            'shadow-[0_0_10px_rgba(220,38,38,0.7)]"></span>'
            f'<span class="leading-snug">{esc(d["title"])}</span></a></li>'
        )
    return (
        '<li data-demo-item>'
        f'<a href="/demos/{d["slug"]}/" '
        'class="group flex items-start gap-2.5 rounded-lg py-1.5 pl-3.5 pr-2 text-sm text-smoke '
        'transition-colors duration-150 hover:bg-coal/70 hover:text-flare">'
        '<span class="mt-[0.44rem] h-1 w-1 shrink-0 rounded-full bg-edge-bright transition-colors '
        'group-hover:bg-blood"></span>'
        f'<span class="leading-snug">{esc(d["title"])}</span></a></li>'
    )


def demo_nav(current_slug=None, sticky_headers=True, cat_base=""):
    """The grouped list of every demo — the body of the left-hand nav.

    ONE component, used in every sidebar slot: the demos index and every detail
    page. Two different navs in the same slot read as a bug, however sensible
    each looked alone.

    Rendered twice per detail page (a mobile <details>, a sticky desktop panel),
    so it carries no ids. Category headers stick to the top of the scroll
    container on desktop; inside the mobile disclosure there is no scroller of
    its own, so they must not.

    cat_base prefixes the category anchors: "" on the index (jump down the page),
    "/demos/" on a detail page (jump back to the index's section).
    """
    by_cat = demos_by_cat()
    # The header must be fully opaque: it scrolls over the list beneath it, and a
    # translucent background lets the items bleed straight through it.
    head_cls = (
        "sticky top-0 z-10 bg-panel shadow-[0_6px_6px_-6px_rgba(0,0,0,0.55)] "
        if sticky_headers
        else ""
    ) + "-mx-2 px-4 py-1.5"
    link_cls = (
        "block text-[10px] font-bold uppercase tracking-[0.14em] text-smoke/60 "
        "transition-colors hover:text-flare "
        "aria-[current=true]:text-flare"
    )

    out = ['<div class="space-y-4">']
    for cat in CATEGORY_ORDER:
        items = by_cat.get(cat, [])
        if not items:
            continue
        out.append(f'<div data-demo-group data-cat="{esc(cat)}">')
        out.append(
            f'<p class="{head_cls}">'
            f'<a data-cat-link href="{cat_base}#{cat_slug(cat)}" class="{link_cls}">{esc(cat)}</a></p>'
        )
        out.append('<ul class="mt-1 space-y-0.5">')
        for d in items:
            out.append(nav_link(d, d["slug"] == current_slug))
        out.append("</ul></div>")
    out.append(
        '<p data-demo-empty hidden class="px-3 py-6 text-center text-sm text-smoke/70">No demos match.</p>'
    )
    out.append("</div>")
    return "\n".join(out)


def nav_filter_box(on_index=False):
    """Search field + 'all demos' link — the panel header, above the scroller.

    On the index that link points at the page you are already on, so it carries
    aria-current instead of pretending to go somewhere.
    """
    cur = ' aria-current="page"' if on_index else ""
    return (
        f'<a href="/demos/"{cur} class="group flex items-center gap-2 text-[13px] font-semibold text-flare">'
        '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" '
        'stroke-linecap="round" stroke-linejoin="round" class="transition-transform group-hover:-translate-x-0.5">'
        '<path d="M19 12H5m0 0l6 6m-6-6l6-6"/></svg>'
        f'<span class="group-hover:underline">All {len(DEMOS)} demos</span></a>'
        '<div class="relative mt-3">'
        '<svg class="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-smoke/60" width="14" '
        'height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round">'
        '<circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>'
        '<input data-demo-filter type="search" placeholder="Filter demos…" aria-label="Filter demos" '
        'class="w-full rounded-lg border border-edge bg-ink/80 py-1.5 pl-8 pr-8 text-sm text-ash placeholder:text-smoke/50 '
        'transition focus:border-blood/50 focus:outline-none focus:ring-2 focus:ring-blood/25">'
        '<kbd data-demo-hint class="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 rounded border '
        'border-edge bg-coal px-1.5 py-0.5 font-mono text-[10px] text-smoke/60">/</kbd>'
        '</div>'
    )


def sidebar_panel(current_slug=None, cat_base="", on_index=False):
    """The sticky sidebar card, identical on the index and every detail page."""
    return nav_panel(
        panel_head(nav_filter_box(on_index)),
        panel_scroller(
            demo_nav(current_slug, cat_base=cat_base), "data-demo-nav", pad_top=False
        ),
        panel_foot(f"{len(DEMOS)} demos · {len(CATEGORY_ORDER)} categories"),
    )


def nav_panel(head, scroller, foot):
    """The chrome around a sticky sidebar: full-height card, its own scroller.

    top-28 clears the fixed site nav with air to spare; the height is sized so
    the card's foot lands ~1.5rem off the bottom of the viewport once pinned,
    rather than stopping short or running under the fold.
    """
    return (
        '<div class="sticky top-28 h-[calc(100vh-8.5rem)]">'
        '<div class="flex h-full flex-col overflow-hidden rounded-2xl border border-edge bg-panel '
        'shadow-[0_1px_0_0_rgba(255,255,255,0.03)_inset]">'
        f'{head}{scroller}{foot}'
        '</div></div>'
    )


def panel_head(label):
    return f'<div class="shrink-0 border-b border-edge/70 px-4 py-3.5">{label}</div>'


def panel_foot(text):
    return (
        '<div class="shrink-0 border-t border-edge/70 bg-coal/40 px-4 py-2.5">'
        f'<span data-demo-count class="font-mono text-[11px] text-smoke/70">{text}</span></div>'
    )


def panel_scroller(body, attr="", pad_top=True):
    # Bottom-only fade for the row the scroller clips; a top fade would wash out
    # the category headers that pin to that edge.
    #
    # pad_top=False for the demo list: a scroll container's top padding sits
    # ABOVE where `sticky top-0` pins, so rows scroll through that strip in plain
    # sight, uncovered by the header that is supposed to hide them.
    pt = "pt-3" if pad_top else "pt-0"
    return (
        f'<div {attr} class="min-h-0 flex-1 overflow-y-auto px-2 pb-3 {pt} '
        '[mask-image:linear-gradient(to_bottom,#000_calc(100%-18px),transparent_100%)]">'
        f'{body}</div>'
    )


# Scrolls the active demo into view inside the sticky column (the list is taller
# than the viewport), and wires the filter box: type to narrow, "/" to focus,
# Escape to clear. Groups whose demos all hide are hidden with them.
NAV_JS = """<script>
(function () {
  var box = document.querySelector('[data-demo-nav]');
  if (!box) return;

  var active = box.querySelector('[aria-current="page"]');
  if (active) {
    var target = active.offsetTop - box.clientHeight / 2 + active.offsetHeight / 2;
    box.scrollTop = target > 0 ? target : 0;
  }

  var input = document.querySelector('[data-demo-filter]');
  if (!input) return;
  var groups = box.querySelectorAll('[data-demo-group]');
  var empty = box.querySelector('[data-demo-empty]');
  var count = document.querySelector('[data-demo-count]');
  var hint = document.querySelector('[data-demo-hint]');

  function apply() {
    var q = input.value.trim().toLowerCase();
    var shown = 0;
    groups.forEach(function (g) {
      var vis = 0;
      g.querySelectorAll('[data-demo-item]').forEach(function (li) {
        var hit = !q || li.textContent.toLowerCase().indexOf(q) !== -1;
        li.hidden = !hit;
        if (hit) vis++;
      });
      g.hidden = vis === 0;
      shown += vis;
    });
    if (empty) empty.hidden = shown !== 0;
    if (count) count.textContent = q ? shown + ' of ' + %d : '%d demos · %d categories';
    if (hint) hint.hidden = !!q;
  }

  input.addEventListener('input', apply);
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') { input.value = ''; apply(); input.blur(); }
  });
  document.addEventListener('keydown', function (e) {
    if (e.key === '/' && document.activeElement !== input &&
        !/^(INPUT|TEXTAREA)$/.test(document.activeElement.tagName)) {
      e.preventDefault();
      input.focus();
    }
  });
})();
</script>"""


def nav_js():
    n_cats = len({d["category"] for d in DEMOS})
    return NAV_JS % (len(DEMOS), len(DEMOS), n_cats)


# Highlights the category you are looking at, in the index jump-list. Driven by
# scroll position rather than an IntersectionObserver: the hero is taller than
# any sensible observer band, so at the top of the page nothing intersects and
# nothing would be highlighted. "Last heading above the fold line" always has an
# answer, and it falls back to the first category.
INDEX_NAV_JS = """<script>
(function () {
  var links = Array.prototype.slice.call(document.querySelectorAll('[data-cat-link]'));
  if (!links.length) return;
  var sections = links.map(function (a) {
    return document.getElementById(a.getAttribute('href').slice(1));
  });

  var FOLD = 130;  // just below the fixed site nav
  function paint() {
    var cur = sections[0];
    sections.forEach(function (s) {
      if (s && s.getBoundingClientRect().top <= FOLD) cur = s;
    });
    links.forEach(function (a, i) {
      if (sections[i] === cur) { a.setAttribute('aria-current', 'true'); }
      else { a.removeAttribute('aria-current'); }
    });
  }

  var ticking = false;
  window.addEventListener('scroll', function () {
    if (ticking) return;
    ticking = true;
    requestAnimationFrame(function () { paint(); ticking = false; });
  }, { passive: true });
  paint();
})();
</script>"""


def demo_icon(cat, size="h-10 w-10"):
    d = CATEGORY_ICON.get(cat, "M12 5v14M5 12h14")
    return (
        f'<span class="flex {size} shrink-0 items-center justify-center rounded-lg border border-edge bg-coal">'
        f'<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#fd1e2e" stroke-width="1.9" '
        f'stroke-linecap="round" stroke-linejoin="round"><path d="{d}"/></svg></span>'
    )


def card(d):
    curve = f'<div class="mt-4">{curve_svg(d["curve"])}</div>' if d.get("curve") else ""
    video = ('<span class="inline-flex items-center gap-1 text-xs text-flare">'
             '<svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5v14l11-7z"/></svg>video</span>'
             ) if d.get("video") else '<span></span>'
    return (
        f'<a href="/demos/{d["slug"]}/" class="group flex flex-col rounded-2xl border border-edge bg-panel p-5 transition hover:border-ember/60 hover:bg-coal/40">'
        '<div class="flex items-center gap-3">'
        + demo_icon(d["category"]) +
        '<div class="min-w-0">'
        f'<h3 class="font-bold text-white">{esc(d["title"])}</h3>'
        f'<code class="font-mono text-xs text-smoke">{esc(d.get("example") or d.get("command", ""))}</code>'
        '</div></div>'
        f'<p class="mt-3 flex-1 text-sm text-smoke">{d["tagline"]}</p>'
        f'{curve}'
        '<div class="mt-4 flex items-center justify-between">'
        f'{video}'
        '<span class="text-sm font-semibold text-flare group-hover:underline">View demo →</span>'
        '</div></a>'
    )


# Canvas build-up walkthroughs: seven structurally-different plan shapes, each
# captured on the desktop canvas one element at a time. Frames live under
# assets/canvas-walk/<slug>/{0,1,2}.png. Stepped through client-side (site.js).
WALKTHROUGHS = [
    ("flat", "Flat sequence", "steps run top to bottom",
     ["Scenario + first request — the browse call.", "Add a think_time — a human pause.", "Add the next request — a linear chain."]),
    ("group", "Grouped", "bundle &amp; name steps",
     ["Scenario + first request.", "Add a group — a named container.", "Nest a request — its STEPS branch fills."]),
    ("branch", "If / else", "conditional paths",
     ["Scenario + login request.", "Add an if — THEN and ELSE appear.", "Fill both — success path + fallback."]),
    ("parallel", "Parallel", "concurrent branches",
     ["Scenario + home request.", "Add a parallel — two empty branches.", "A request in each — run at once."]),
    ("switch", "Switch", "route by value",
     ["Scenario + route request.", "Add a switch — cases fan out.", "Fill cases + default — multi-way routing."]),
    ("foreach", "Data-driven foreach", "one flow per item",
     ["Scenario + list request.", "Add a foreach over a data set.", "One request per item — iteration."]),
    ("retry", "Retry", "resilient, with backoff",
     ["Scenario + submit request.", "Add a retry — an attempt block.", "Wrap a request with backoff."]),
]


def render_walkthroughs():
    cards = []
    for slug, title, hint, caps in WALKTHROUGHS:
        slides = "".join(
            f'<img data-fr data-cap="Step {i + 1}/3 · {esc(caps[i])}" src="/assets/canvas-walk/{slug}/{i}.png" '
            f'loading="lazy" alt="{esc(title)} build-up, step {i + 1}" '
            f'class="absolute inset-0 h-full w-full object-contain transition-opacity duration-300 '
            f'{"opacity-100" if i == len(caps) - 1 else "opacity-0"}">'
            for i in range(3)
        )
        cards.append(
            f'<div data-walk class="group overflow-hidden rounded-2xl border border-edge bg-panel">'
            f'<div class="flex items-baseline justify-between gap-2 border-b border-edge px-3 py-2">'
            f'<span class="font-bold text-white">{title}</span>'
            f'<span class="text-xs text-smoke">{hint}</span></div>'
            f'<div class="relative aspect-[736/580] bg-ink">{slides}'
            f'<button data-pv aria-label="Previous step" class="absolute left-1.5 top-1/2 z-10 grid h-7 w-7 -translate-y-1/2 place-items-center rounded-full border border-edge bg-coal/85 text-smoke opacity-0 transition hover:text-white group-hover:opacity-100">'
            f'<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M15 18l-6-6 6-6"/></svg></button>'
            f'<button data-nx aria-label="Next step" class="absolute right-1.5 top-1/2 z-10 grid h-7 w-7 -translate-y-1/2 place-items-center rounded-full border border-edge bg-coal/85 text-smoke opacity-0 transition hover:text-white group-hover:opacity-100">'
            f'<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"/></svg></button>'
            f'<div data-dots class="absolute inset-x-0 bottom-2 z-10 flex justify-center gap-1.5"></div></div>'
            f'<div data-cap class="min-h-[2.6em] px-3 py-2 text-xs leading-snug text-smoke"></div></div>'
        )
    return (
        '<section class="border-t border-edge/60 bg-coal/40 py-16">'
        '<div class="mx-auto max-w-[88rem] px-5">'
        '<p class="kicker">Compose visually</p>'
        '<h2 class="mt-3 text-3xl font-extrabold tracking-tight text-white sm:text-4xl">Build any plan on the canvas</h2>'
        '<p class="mt-4 max-w-2xl text-smoke">In loadr Desktop, drag steps onto a scenario and the plan takes shape as a node graph. '
        'Here are seven common shapes, built up one element at a time — step through each.</p>'
        '<div class="mt-10 grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">'
        + "\n".join(cards)
        + '</div></div></section>'
    )


def render_index():
    by_cat = demos_by_cat()

    groups = []
    for cat in CATEGORY_ORDER:
        items = by_cat.get(cat, [])
        if not items:
            continue
        grid = "\n        ".join(card(d) for d in items)
        groups.append(
            f'<div id="{cat_slug(cat)}" class="scroll-mt-24">'
            '<div class="flex items-baseline justify-between border-b border-edge/60 pb-3">'
            f'<h2 class="text-xl font-extrabold text-white">{esc(cat)}</h2>'
            f'<span class="text-xs text-smoke">{len(items)} demo{"s" if len(items) != 1 else ""}</span>'
            '</div>'
            f'<p class="mt-2 text-sm text-smoke">{CATEGORY_BLURB.get(cat, "")}</p>'
            '<div class="mt-5 grid gap-5 sm:grid-cols-2 lg:grid-cols-3">'
            f'{grid}'
            '</div></div>'
        )
    groups_html = "\n\n      ".join(groups)

    # Same sidebar as the detail pages. Its category headers anchor down this
    # page's card sections, so it doubles as the jump-list.
    cat_panel = sidebar_panel(cat_base="", on_index=True)

    desc = ("Every loadr demo, categorised: load profiles, traffic modelling, validation, protocols, "
            "databases, scripting, distributed scale and ops integrations. Each tile opens a detail page "
            "with the runnable example, what to look for and the command to run it.")
    body = head("Demos — see loadr work, by category | loadr", desc, "https://loadr.io/demos/")
    body += f"""
<!-- ======================================================= HERO -->
<section class="hero-grid relative overflow-hidden pt-32 pb-12">
  <div class="pointer-events-none absolute -top-40 left-1/2 h-[44rem] w-[64rem] -translate-x-1/2 rounded-[50%] bg-blood/20 blur-[150px]"></div>
  <div class="mx-auto max-w-5xl px-5 text-center">
    <p class="kicker">Demos</p>
    <h1 class="mt-3 text-4xl font-extrabold tracking-tight text-white sm:text-5xl">See loadr work</h1>
    <p class="mx-auto mt-5 max-w-2xl text-lg text-smoke">
      {len(DEMOS)} worked demos across {len(CATEGORY_ORDER)} categories — every one backed by a real, runnable
      example. Pick a tile to see the full YAML, what to look for in the output, and the one command that runs it.
    </p>
    <div class="mt-6 flex flex-wrap items-center justify-center gap-x-6 gap-y-2 text-sm text-smoke">
      <span class="inline-flex items-center gap-1.5"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#4ade80" stroke-width="2.2"><path d="M20 6 9 17l-5-5"/></svg> Runnable examples</span>
      <span class="inline-flex items-center gap-1.5"><svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="#4ade80" stroke-width="2.2"><path d="M20 6 9 17l-5-5"/></svg> One binary, zero deps</span>
      <span class="inline-flex items-center gap-1.5"><a class="hover:text-flare" href="/examples/">Browse the raw examples →</a></span>
    </div>
  </div>
</section>

{render_walkthroughs()}

<!-- ======================================================= DEMO CARDS -->
<section class="pt-10 pb-8 lg:pt-14">
  <div class="mx-auto max-w-[88rem] px-5 lg:flex lg:gap-12 xl:gap-16">

    <aside class="hidden lg:block lg:w-[17.5rem] lg:shrink-0">
      {cat_panel}
    </aside>

    <div class="min-w-0 flex-1 space-y-14">
      {groups_html}
    </div>
  </div>
</section>
{nav_js()}
{INDEX_NAV_JS}

<!-- ======================================================= CTA -->
<section class="border-t border-edge/60 bg-coal/50 py-20">
  <div class="mx-auto max-w-5xl px-5">
    <div class="rounded-2xl border border-edge bg-panel p-8 text-center">
      <h2 class="text-2xl font-extrabold text-white">Run any of these yourself</h2>
      <p class="mx-auto mt-2 max-w-2xl text-sm text-smoke">
        Grab the single binary, clone the examples, and run any demo verbatim. No agents, no runtime, no account.
      </p>
      <div class="mt-6 flex flex-wrap justify-center gap-4">
        <a href="/download/" class="glow rounded-xl bg-blood px-6 py-3 font-bold text-white">Download loadr →</a>
        <a href="/examples/" class="rounded-xl border border-edge-bright bg-coal px-6 py-3 font-semibold text-ash hover:border-ember/60 hover:text-white">Browse all examples</a>
      </div>
    </div>
  </div>
</section>
"""
    body += FOOTER
    return body


def render_detail(d, prev, nxt):
    slug = d["slug"]
    title = f'{d["title"]} — loadr demo'
    desc = d["tagline"] + (f' A runnable loadr demo backed by examples/{d["example"]}.' if d.get("example") else ' A loadr demo.')
    canonical = f"https://loadr.io/demos/{slug}/"
    lang = d.get("example_lang", "yaml")
    example = d.get("example")
    run_cmd = d.get("command") or (f'loadr run examples/{esc(example)}' if example else "")
    if example:
        src_block = (
            f'<div class="codebox mt-4">'
            f'<div class="codebar"><span>examples/{esc(example)}</span>'
            f'<button data-copy="#{slug}Src" class="rounded border border-edge px-2 py-0.5 text-[10px] text-smoke hover:text-flare">copy</button></div>'
            f'<pre id="{slug}Src"><code data-lang="{lang}">{read_example(example)}</code></pre></div>'
            f'<p class="mt-4 text-sm text-smoke">View raw: '
            f'<a class="font-semibold text-flare hover:underline" href="/examples/{esc(example)}">examples/{esc(example)}</a></p>'
        )
    else:
        src_block = ""

    highlights = "\n".join(
        f'<li class="flex gap-2"><span class="mt-0.5 text-ember">▸</span><span>{h}</span></li>'
        for h in d.get("highlights", [])
    )
    metrics = ""
    if d.get("metrics"):
        metrics = ('<p class="mt-6 text-sm text-smoke">Key metrics: '
                   + " · ".join(f'<code class="text-flare">{esc(m)}</code>' for m in d["metrics"])
                   + '.</p>')
    curve = ""
    if d.get("curve"):
        curve = (
            '<div class="mt-6 rounded-2xl border border-edge bg-panel p-5">'
            f'<p class="text-xs font-semibold uppercase tracking-wider text-smoke">Load profile · {CURVE_LABEL.get(d["curve"], "")}</p>'
            f'<div class="mt-3">{curve_svg(d["curve"])}</div>'
            '</div>'
        )
    video = ""
    if d.get("video"):
        video = f"""
      <div class="mt-6 overflow-hidden rounded-xl border border-edge bg-ink">
        <video controls preload="none" aria-label="Demo: {esc(d["title"])}" poster="/videos/{d["video"]}-poster.jpg" class="aspect-video w-full bg-ink">
          <source src="/videos/{d["video"]}.mp4" type="video/mp4">
        </video>
      </div>"""

    also = ""
    if d.get("also"):
        links = " · ".join(
            f'<a class="text-flare hover:underline" href="/examples/{a}">{esc(a)}</a>' for a in d["also"]
        )
        also = f'<p class="mt-4 text-sm text-smoke">Related files: {links}</p>'

    docs_btn = ""
    if d.get("docs"):
        docs_btn = f'<a href="{d["docs"]}" class="rounded-xl border border-edge-bright bg-coal px-6 py-3 font-semibold text-ash hover:border-ember/60 hover:text-white">Read the docs →</a>'

    # The same list twice: a mobile disclosure (no scroller of its own, so no
    # sticky headers) and the desktop panel.
    mobile_nav = demo_nav(slug, sticky_headers=False, cat_base="/demos/")
    nav_panel_html = sidebar_panel(slug, cat_base="/demos/")

    parts = [head(title, desc, canonical)]
    parts.append(f"""
<!-- ======================================================= HERO -->
<section class="hero-grid relative overflow-hidden pt-32 pb-10">
  <div class="pointer-events-none absolute -top-40 left-1/2 h-[40rem] w-[56rem] -translate-x-1/2 rounded-[50%] bg-blood/20 blur-[150px]"></div>
  <div class="mx-auto max-w-[88rem] px-5">
    <nav class="flex flex-wrap items-center gap-x-2 gap-y-1 text-sm text-smoke">
      <a class="hover:text-flare" href="/demos/">Demos</a>
      <span class="text-edge-bright">/</span>
      <a class="hover:text-flare" href="/demos/#{cat_slug(d["category"])}">{esc(d["category"])}</a>
      <span class="text-edge-bright">/</span>
      <span class="text-ash">{esc(d["title"])}</span>
    </nav>
    <div class="mt-4 flex items-center gap-4">
      {demo_icon(d["category"], "h-12 w-12")}
      <h1 class="text-3xl font-black tracking-tight text-white sm:text-4xl">{esc(d["title"])}</h1>
      <span class="rounded-full border border-edge bg-coal px-3 py-1 text-xs text-ash">{esc(d["category"])}</span>
    </div>
    <p class="mt-5 max-w-2xl text-lg text-smoke">{d["description"]}</p>
  </div>
</section>

<!-- ======================================================= DETAIL -->
<section class="pt-10 pb-16 lg:pt-14">
  <div class="mx-auto max-w-[88rem] px-5 lg:flex lg:gap-12 xl:gap-16">

    <!-- left-hand demo nav: a disclosure on mobile, a full-height sticky panel from lg -->
    <details class="mb-8 overflow-hidden rounded-xl border border-edge bg-panel/60 lg:hidden">
      <summary class="flex cursor-pointer select-none items-center justify-between px-4 py-3 text-sm font-semibold text-ash">
        <span>Browse all {len(DEMOS)} demos</span>
        <span class="text-xs font-normal text-smoke/70">{len(CATEGORY_ORDER)} categories</span>
      </summary>
      <div class="max-h-[70vh] overflow-y-auto border-t border-edge px-4 py-4">
        {mobile_nav}
      </div>
    </details>

    <aside class="hidden lg:block lg:w-[17.5rem] lg:shrink-0">
      {nav_panel_html}
    </aside>

    <!-- The sticky panel can only travel inside its parent, and the <aside>
         stretches to this column. Without a floor the column can come out
         shorter than the panel, pinning it to zero travel — so it never sticks.
         Sized to just clear the panel: any taller only opens a void, since the
         pin always ends before the footer either way. prev/next hangs at the
         column's foot so any slack reads as spacing. -->
    <div class="flex min-w-0 flex-1 flex-col lg:min-h-[calc(100vh-4rem)]">
    <div class="grid gap-8 lg:grid-cols-2">
    <div>
      <div class="codebox">
        <div class="codebar"><span>Run it</span><button data-copy="#{slug}Run" class="rounded border border-edge px-2 py-0.5 text-[10px] text-smoke hover:text-flare">copy</button></div>
        <pre id="{slug}Run"><code data-lang="bash">$ {run_cmd}</code></pre>
      </div>
      {src_block}
      {also}
    </div>
    <div>
      <div class="rounded-2xl border border-edge bg-panel p-6">
        <h2 class="text-lg font-bold text-white">What it shows</h2>
        <ul class="mt-4 space-y-2.5 text-sm text-ash">
          {highlights}
        </ul>
        {metrics}
      </div>
      {curve}
      {video}
      <div class="mt-6 flex flex-wrap gap-4">
        <a href="/download/" class="glow rounded-xl bg-blood px-6 py-3 font-bold text-white">Download &amp; run →</a>
        {docs_btn}
      </div>
    </div>
    </div>

    <!-- prev / next lives inside the content column: it gives the sticky
         sidebar's containing block the height it needs to actually stick -->
    <nav class="mt-auto flex items-center justify-between gap-4 border-t border-edge/60 pt-6 text-sm">
""")
    if prev:
        parts.append(f'    <a class="text-smoke hover:text-flare" href="/demos/{prev["slug"]}/">← {esc(prev["title"])}</a>\n')
    else:
        parts.append('    <span></span>\n')
    parts.append('    <a class="font-semibold text-flare hover:underline" href="/demos/">All demos</a>\n')
    if nxt:
        parts.append(f'    <a class="text-smoke hover:text-flare" href="/demos/{nxt["slug"]}/">{esc(nxt["title"])} →</a>\n')
    else:
        parts.append('    <span></span>\n')
    parts.append(f"""    </nav>
    </div>
  </div>
</section>
{nav_js()}
""")
    parts.append(FOOTER)
    return "".join(parts)


def main():
    out_dir = pathlib.Path(__file__).resolve().parent / "demos"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Guard: unique slugs, existing example files.
    seen = set()
    for d in DEMOS:
        assert d["slug"] not in seen, f"duplicate slug {d['slug']}"
        seen.add(d["slug"])
        if d.get("example"):
            assert (EXAMPLES / d["example"]).is_file(), f"missing example {d['example']}"

    (out_dir / "index.html").write_text(render_index(), encoding="utf-8")
    print(f"wrote {out_dir / 'index.html'}")

    for i, d in enumerate(DEMOS):
        prev = DEMOS[i - 1] if i > 0 else None
        nxt = DEMOS[i + 1] if i < len(DEMOS) - 1 else None
        ddir = out_dir / d["slug"]
        ddir.mkdir(parents=True, exist_ok=True)
        (ddir / "index.html").write_text(render_detail(d, prev, nxt), encoding="utf-8")
        print(f"wrote {ddir / 'index.html'}")

    print(f"done: index + {len(DEMOS)} detail pages")


if __name__ == "__main__":
    main()
