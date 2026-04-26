# xk6-kafka-rest

A [k6](https://k6.io/) extension for publishing JSON messages to **Confluent Kafka** via the
[Confluent REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
with full **OAuth 2.0 (Client Credentials)** support.

> ⚠️ **This extension is currently under active development.** APIs may change between versions.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Current Limitations](#current-limitations)
- [Build](#build)
- [Quick Start](#quick-start)
  - [v2 — Simple batch produce](#v2--simple-batch-produce)
  - [v3 — Headers and multi-cluster](#v3--headers-and-multi-cluster)
- [Local Development (Docker)](#local-development-docker)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Metrics](#metrics)
- [IDE Autocomplete (TypeScript)](#ide-autocomplete-typescript)
- [License](#license)

---

## Features

- ✅ OAuth 2.0 Client Credentials grant — automatic token fetch & refresh with retry
- ✅ **v2 API** — JSON batch produce, all records in a single HTTP call
- ✅ **v3 API** — per-record headers, multi-cluster support, concurrent produce
- ✅ Auto-chunking — pass any number of messages; the extension splits them automatically
- ✅ Per-record error detection — surfaces silent Kafka-level failures
- ✅ Custom k6 metrics: `kafka_rest_messages_sent`, `kafka_rest_publish_duration`, `kafka_rest_publish_errors`
- ✅ Per-topic metric tags

---

## Requirements

- Go 1.21+
- [`xk6`](https://github.com/grafana/xk6) — `go install go.k6.io/xk6/cmd/xk6@latest`

---

## Current Limitations

| Feature | Notes |
|---------|-------|
| Message headers | v2 only — the REST Proxy v2 JSON API does not support per-record headers. Use v3 for header support. |
| Avro / Schema Registry | JSON payloads only at this time |

---

## Build

```bash
xk6 build --with github.com/Waleed2660/xk6-kafka-rest@latest

./k6 version
```

> **Note:** The module path is case-sensitive — use `Waleed2660` with a capital `W`.

---

## Quick Start

### v2 — Simple batch produce

No cluster ID needed. All records sent in one HTTP call per batch.

```javascript
import { KafkaRestClient } from 'k6/x/kafka-rest';

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL,       // e.g. https://pkc-xxx.confluent.cloud
  tokenUrl:     __ENV.OAUTH_TOKEN_URL,      // e.g. https://idp.example.com/oauth/token
  clientId:     __ENV.CLIENT_ID,
  clientSecret: __ENV.CLIENT_SECRET,
  scope:        'kafka',                    // optional
  // apiVersion defaults to 'v2'
});

export default function () {
  client.produce('my-topic', [
    { key: 'order-123', value: { event: 'ORDER_PLACED', amount: 99.99 } },
    { key: 'order-124', value: { event: 'ORDER_PLACED', amount: 49.99 } },
  ]);
}
```

### v3 — Headers and multi-cluster

Requires a `clusterId`. Each record is sent as an individual (concurrent) HTTP call, enabling full header support.

```javascript
import { KafkaRestClient } from 'k6/x/kafka-rest';

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL,
  tokenUrl:     __ENV.OAUTH_TOKEN_URL,
  clientId:     __ENV.CLIENT_ID,
  clientSecret: __ENV.CLIENT_SECRET,
  scope:        'kafka',
  apiVersion:   'v3',
  clusterId:    __ENV.CLUSTER_ID,   // find via: GET {baseUrl}/v3/clusters
  maxBatchSize: 20,                 // concurrent HTTP calls per produce()
});

export default function () {
  client.produce('my-topic', [
    {
      key:   'order-123',
      value: { event: 'ORDER_PLACED', amount: 99.99 },
      headers: [
        { key: 'x-trace-id',  value: 'abc-123' },
        { key: 'x-source',    value: 'checkout-service' },
      ],
    },
  ]);
}
```

Run either script:
```bash
KAFKA_REST_URL=https://... \
OAUTH_TOKEN_URL=https://... \
CLIENT_ID=xxx \
CLIENT_SECRET=yyy \
CLUSTER_ID=lkc-xxxxx \     # v3 only
./k6 run script.js
```

---

## Local Development (Docker)

Spin up a full local stack — Kafka, REST Proxy, mock OAuth server, and Kafbat UI:

```bash
cd local-dev
docker compose up -d
# Kafbat UI: http://localhost:8090
# REST Proxy: http://localhost:8082
# Mock OAuth: http://localhost:8080/default/token
```

The local cluster ID is pre-configured as `local-dev-cluster-0001` (set via `KAFKA_CLUSTER_ID` in docker-compose).

Then run the bundled test script:
```bash
./k6 run local-dev/test-script.js
```

---

## Examples

See the [`/examples`](examples/) folder for ready-to-run scripts:

| Script | API | What it shows |
|--------|-----|--------------|
| [`single-message.js`](examples/single-message.js) | v2 | One message per iteration with `check()` assertions |
| [`batch-produce.js`](examples/batch-produce.js) | v2 | 50K messages in 500-record batches across 10 VUs |
| [`multi-topic.js`](examples/multi-topic.js) | v2 | Multiple topics with per-topic thresholds |
| [`v3-with-headers.js`](examples/v3-with-headers.js) | v3 | Per-record headers, `clusterId` config |

---

## API Reference

### `new KafkaRestClient(config)`

**Common options (both v2 and v3)**

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `baseUrl` | `string` | ✅ | REST Proxy root URL — e.g. `https://pkc-xxx.confluent.cloud` |
| `tokenUrl` | `string` | ✅ | OAuth 2.0 token endpoint |
| `clientId` | `string` | ✅ | OAuth client ID |
| `clientSecret` | `string` | ✅ | OAuth client secret |
| `scope` | `string` | — | OAuth scope (space-separated). Default: `""` |
| `apiVersion` | `"v2"` \| `"v3"` | — | REST Proxy API version. Default: `"v2"` |

**v2-only options**

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `maxBatchSize` | `number` | `500` | Records per HTTP call. Arrays larger than this are auto-chunked. Ceiling: `1000`. Reduce for large payloads. |

**v3-only options**

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `clusterId` | `string` | ✅ | Confluent cluster ID. Find via `GET {baseUrl}/v3/clusters` |
| `maxBatchSize` | `number` | — | Max concurrent HTTP calls per `produce()`. Default: `20`. Ceiling: `100`. |

---

### `client.produce(topic, messages)`

Publishes messages to a Kafka topic. Behaviour differs by API version:
- **v2**: sends all messages in one HTTP request (auto-chunked if over `maxBatchSize`)
- **v3**: sends each message as its own HTTP request, up to `maxBatchSize` in parallel

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Kafka topic name |
| `messages` | `Message[]` | Array of message objects |

**`Message` shape**

```typescript
{
  key?:     string | object                    // optional partition key
  value:    object                             // message payload (JSON)
  headers?: { key: string, value: string }[]  // v3 only — ignored in v2
}
```

**Returns** `ProduceResponse`

```typescript
{
  offsets: {
    partition:   number
    offset:      number
    error_code?: number   // non-zero = record rejected by Kafka
    error?:      string
  }[]
}
```

> ⚠️ **Per-record errors**: The REST Proxy can return HTTP `200 OK` while individual records have failed. The extension detects these, surfaces them as errors, and counts them in `kafka_rest_publish_errors`.

---

### `client.close()`

No-op. Reserved for future connection cleanup.

---

## Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `kafka_rest_messages_sent` | Counter | `topic` | Records confirmed by Kafka |
| `kafka_rest_publish_duration` | Trend (ms) | `topic` | Round-trip time per `produce()` call |
| `kafka_rest_publish_errors` | Counter | `topic` | Failed records (HTTP errors or per-record Kafka errors) |

Use in thresholds:

```javascript
export const options = {
  thresholds: {
    'kafka_rest_publish_duration{topic:my-topic}': ['p(95)<500'],
    'kafka_rest_publish_errors':                   ['count==0'],
  },
};
```

---

## IDE Autocomplete (TypeScript)

Type definitions are included in `index.d.ts`. To enable autocomplete in VS Code or any TypeScript-aware editor:

**1. Copy the types into your project**

```bash
cp /path/to/xk6-kafka-rest/index.d.ts ./xk6-kafka-rest.d.ts
```

**2. Add a path mapping to `tsconfig.json`**

```json
{
  "compilerOptions": {
    "paths": {
      "k6/x/kafka-rest": ["./xk6-kafka-rest.d.ts"]
    }
  }
}
```


---

## License

[Apache 2.0](LICENSE)
