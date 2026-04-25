# xk6-kafka-rest — Feature Tracker

> A [k6](https://k6.io/) extension that publishes JSON messages to **Confluent Kafka** via the
> [Confluent REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html),
> with full **OAuth 2.0** support — filling the gap left by
> [xk6-kafka](https://github.com/mostafa/xk6-kafka) which does not support OAuth.

---

## Status Legend

| Symbol | Meaning         |
|--------|-----------------|
| ✅     | Done            |
| 🚧     | In Progress     |
| 📋     | Planned         |
| ❌     | Blocked / Won't Do |

---

## Milestone 1 — Core Setup

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 1.1 | Scaffold xk6 extension boilerplate (Go module, `register.go`) | 📋 | Follow xk6 build system conventions |
| 1.2 | `xk6 build` integration & `Makefile` | 📋 | Produce a custom k6 binary |
| 1.3 | Basic CI pipeline (GitHub Actions) | 📋 | Build + test on push / PR |
| 1.4 | Versioned releases (`goreleaser`) | 📋 | Publish binaries for macOS, Linux, Windows |

---

## Milestone 2 — OAuth 2.0 Authentication

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 2.1 | Client Credentials grant (`client_id` + `client_secret`) | 📋 | Primary Confluent Cloud flow |
| 2.2 | Token endpoint configuration | 📋 | Configurable per-test via k6 options / env vars |
| 2.3 | Automatic token refresh before expiry | 📋 | Cache token, refresh when TTL < threshold |
| 2.4 | Bearer token injection into REST Proxy requests | 📋 | `Authorization: Bearer <token>` header |
| 2.5 | Token refresh retry with back-off | 📋 | Transient IDP failures should not fail the load test |
| 2.6 | Support for custom scopes | 📋 | Pass arbitrary OAuth scopes |
| 2.7 | mTLS / certificate-based auth (optional) | 📋 | For on-prem Confluent clusters |

---

## Milestone 3 — Kafka REST Proxy Producer

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 3.1 | `produce(topic, messages[])` — single-topic publish | 📋 | Uses `POST /topics/{topic}` |
| 3.2 | JSON message serialisation | 📋 | `"value_schema_id"` + raw JSON value |
| 3.3 | Avro / JSON Schema Registry integration | 📋 | Lookup schema ID by subject |
| 3.4 | Custom Kafka message headers | ❌ | Not supported by REST Proxy v2 JSON API |
| 3.5 | Partition key support (`key` field) | 📋 | Route messages to specific partitions |
| 3.6 | Batch publish (multiple records per request) | 📋 | Reduces HTTP round-trips |
| 3.7 | Configurable REST Proxy base URL | 📋 | Support multiple clusters |
| 3.8 | TLS / custom CA certificate for REST Proxy | 📋 | Enterprise on-prem clusters |
| 3.9 | Response parsing & error propagation to k6 | 📋 | Surface Kafka errors as k6 errors |

---

## Milestone 4 — k6 JavaScript API

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 4.1 | `new KafkaRestClient(config)` constructor | 📋 | Accepts `baseUrl`, `oauth`, `tls` options |
| 4.2 | `client.produce(topic, messages)` | 📋 | Returns response metadata |
| 4.3 | `client.produceWithSchema(topic, schemaId, messages)` | 📋 | Publish with explicit schema ID |
| 4.4 | `client.close()` | 📋 | Flush & clean up connections |
| 4.5 | TypeScript type definitions (`.d.ts`) | 📋 | IDE auto-complete for k6 scripts |
| 4.6 | Example k6 scripts in `/examples` | 📋 | Single message, batch, schema-registry |

---

## Milestone 5 — Metrics & Observability

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 5.1 | Custom k6 metric: `kafka_rest_publish_duration` | 📋 | Histogram of produce latency |
| 5.2 | Custom k6 metric: `kafka_rest_publish_errors` | 📋 | Counter of failed publishes |
| 5.3 | Custom k6 metric: `kafka_rest_messages_sent` | 📋 | Counter of successfully published messages |
| 5.4 | Custom k6 metric: `oauth_token_refresh_duration` | 📋 | Histogram of token fetch latency |
| 5.5 | Per-topic metric tags | 📋 | Tag metrics with `topic` label |

---

## Milestone 6 — Configuration

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 6.1 | Environment variable support (`K6_KAFKA_REST_*`) | 📋 | 12-factor config |
| 6.2 | k6 `options` / `__ENV` integration | 📋 | Pass config at test start |
| 6.3 | Per-VU vs. shared client mode | 📋 | Shared token cache across VUs |
| 6.4 | Connection timeout & retry configuration | 📋 | `connectTimeout`, `retries`, `retryDelay` |
| 6.5 | Request timeout per produce call | 📋 | Prevent hung VUs |

---

## Milestone 7 — Testing

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 7.1 | Unit tests for OAuth token manager | 📋 | Mock IDP server |
| 7.2 | Unit tests for REST Proxy producer | 📋 | Mock HTTP server |
| 7.3 | Integration test against local Confluent stack | 📋 | `docker-compose` with REST Proxy + OAuth mock |
| 7.4 | k6 script smoke tests (`k6 run`) | 📋 | Run example scripts in CI |
| 7.5 | Code coverage reporting | 📋 | Enforce minimum coverage threshold |

---

## Milestone 8 — Documentation

| # | Feature | Status | Notes |
|---|---------|--------|-------|
| 8.1 | `README.md` — quickstart guide | 📋 | Install, build, run example |
| 8.2 | Full API reference | 📋 | All JS methods & config options |
| 8.3 | OAuth setup guide (Confluent Cloud) | 📋 | Step-by-step with screenshots |
| 8.4 | Schema Registry usage guide | 📋 | How to register & use schemas |
| 8.5 | Comparison with `xk6-kafka` (why OAuth gap exists) | 📋 | Help users choose the right tool |
| 8.6 | `CHANGELOG.md` | 📋 | Keep a changelog |
| 8.7 | `CONTRIBUTING.md` | 📋 | Dev setup, PR guidelines |

---

## Known Limitations / Out of Scope

| Item | Reason |
|------|--------|
| Kafka Consumer (read messages) | REST Proxy consumer API is stateful; out of scope for load testing |
| SASL/SCRAM, Kerberos auth | Handled by the REST Proxy internally; not needed client-side |
| Avro binary encoding client-side | Delegated to Schema Registry via schema ID |

---

## Dependency Decisions

| Dependency | Purpose | Decision |
|------------|---------|----------|
| `golang.org/x/oauth2` | OAuth 2.0 client | ✅ Use — well-maintained, stdlib-compatible |
| `github.com/grafana/xk6` | Extension SDK | ✅ Required |
| Schema Registry client | Fetch schema IDs | 📋 Evaluate `github.com/riferrei/srclient` |
| `net/http` | HTTP to REST Proxy | ✅ Use stdlib, no extra HTTP client needed |

---

## Open Questions

- [ ] Should the extension support **multiple clusters** in a single test script?
- [ ] Should token cache be **per-VU** or **global** (shared across all VUs)?
- [ ] Do we need **consumer** support for any validation use-cases?
- [ ] Target Confluent Cloud only, or also support **on-prem** REST Proxy?
- [ ] Should we publish to the [xk6 extension registry](https://registry.k6.io/)?

---

*Last updated: 2026-04-25*

