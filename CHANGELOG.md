# Changelog

All notable changes to **xk6-kafka-rest** will be documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versions follow [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

### Added
- Per-record error detection — surfaces silent Kafka-level failures from REST Proxy `200 OK` responses
- `kafka_rest_publish_errors` counter now tracks failed records, not just failed HTTP calls
- `kafka_rest_messages_sent` counter now reflects only Kafka-confirmed records
- Batch size limit: default 500, configurable via `maxBatchSize`, hard ceiling 1000
- GitHub Actions CI — `go vet`, `staticcheck`, unit tests, `xk6 build` on every push/PR
- GoReleaser config for versioned multi-platform binary releases

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

