# Changelog

All notable changes to **xk6-kafka-rest** will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

---

## [0.3.0] - 2026-04-26

### Added
- **REST Proxy v3 API support** — opt-in via `apiVersion: 'v3'` in config
  - One HTTP call per record, sent concurrently (bounded by `maxBatchSize`, default 20, ceiling 100)
  - Full **message header** support (`headers: [{ key, value }]`) — v3 only
  - Multi-cluster support via `clusterId` config field (required for v3)
- `Header` type added to TypeScript definitions (`index.d.ts`)
- `headers` field added to `Message` type (silently ignored in v2 mode)
- `apiVersion` and `clusterId` fields added to `ClientConfig`
- New example script `examples/v3-with-headers.js`
- Static `KAFKA_CLUSTER_ID` in local docker-compose stack (`local-dev-cluster-0001`)

### Fixed
- v3 metrics: `error_code: 200` in REST Proxy success response no longer counted as an error
- v3 metrics: all offsets now returned even when a concurrent record fails, preventing double-counting

---

## [0.2.0] - 2026-04-26

### Added
- Per-record error detection — surfaces silent Kafka-level failures from REST Proxy `200 OK` responses
- `kafka_rest_publish_errors` counter now tracks failed records, not just failed HTTP calls
- `kafka_rest_messages_sent` counter now reflects only Kafka-confirmed records
- Auto-chunking: `produce()` now accepts any number of messages; arrays larger than `maxBatchSize` are split automatically
- Config validation at constructor time — missing required fields throw a descriptive `TypeError` immediately
- `baseUrl` trailing-slash normalisation and path guard (rejects full endpoint URLs)
- GitHub Actions CI — `go vet`, `staticcheck`, unit tests, `xk6 build` on every push/PR
- Versioned multi-platform release workflow (Linux amd64/arm64, macOS amd64/arm64, Windows amd64)

---

## [0.1.0] - 2026-04-26

### Added
- Initial release
- OAuth 2.0 Client Credentials grant with automatic token refresh (30 s before expiry)
- `KafkaRestClient` JS constructor accepting `baseUrl`, `tokenUrl`, `clientId`, `clientSecret`, `scope`, `maxBatchSize`
- `produce(topic, messages[])` — JSON batch publish via `POST /topics/{topic}`
- Support for message `key` and `value`
- Custom k6 metrics: `kafka_rest_messages_sent`, `kafka_rest_publish_duration`, `kafka_rest_publish_errors`
- Per-topic metric tags for threshold targeting
- `docker-compose.yml` local stack — Kafka broker, Confluent REST Proxy, Mock OAuth2 server, Kafbat UI
- Example load test script publishing 50 K messages in 200-record batches across 10 VUs

---

[Unreleased]: https://github.com/Waleed2660/xk6-kafka-rest/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/Waleed2660/xk6-kafka-rest/releases/tag/v0.1.0

