# xk6-kafka-rest

A [k6](https://k6.io/) extension for publishing JSON messages to **Confluent Kafka** via the
[Confluent REST Proxy](https://docs.confluent.io/platform/current/kafka-rest/index.html)
with full **OAuth 2.0 (Client Credentials)** support.

> ⚠️ **This extension is currently under active development.** APIs may change between versions.
> See [Current Limitations](#current-limitations) for features not yet supported.

---

## Table of Contents

- [Features](#features)
- [Requirements](#requirements)
- [Current Limitations](#current-limitations)
- [Build](#build)
- [Quick Start](#quick-start)
- [Local Development (Docker)](#local-development-docker)
- [Examples](#examples)
- [API Reference](#api-reference)
- [Metrics](#metrics)
- [IDE Autocomplete (TypeScript)](#ide-autocomplete-typescript)
- [License](#license)

---

## Features

- ✅ OAuth 2.0 Client Credentials grant — automatic token fetch & refresh
- ✅ JSON batch produce (`POST /topics/{topic}`)
- ✅ Configurable batch size limit (default 500, hard ceiling 1000)
- ✅ Per-record error detection — surfaces silent Kafka-level failures
- ✅ Custom k6 metrics: `kafka_rest_messages_sent`, `kafka_rest_publish_duration`, `kafka_rest_publish_errors`
- ✅ Per-topic metric tags
- ✅ Custom message keys

---

## Requirements

- Go 1.21+
- [`xk6`](https://github.com/grafana/xk6) — `go install go.k6.io/xk6/cmd/xk6@latest`

---

## Current Limitations

> This extension is under active development. The following features are not yet supported:

| Feature | Notes |
|---------|-------|
| Message headers | The REST Proxy v2 JSON API does not support per-record headers |
| Avro / Schema Registry | JSON payloads only at this time |

---

## Build

```bash
xk6 build --with github.com/Waleed2660/xk6-kafka-rest@latest

# This produces ./k6 — use it instead of the system k6 binary
./k6 version
```

---

## Quick Start

```javascript
import { KafkaRestClient } from 'k6/x/kafka-rest';

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL,       // e.g. https://xxx.confluent.cloud
  tokenUrl:     __ENV.OAUTH_TOKEN_URL,      // e.g. https://idp.example.com/oauth/token
  clientId:     __ENV.CLIENT_ID,
  clientSecret: __ENV.CLIENT_SECRET,
  scope:        'kafka',                    // optional
});

export default function () {
  const result = client.produce('my-topic', [
    { key: 'order-123', value: { event: 'ORDER_PLACED', amount: 99.99 } },
  ]);
  console.log(JSON.stringify(result));
}
```

Run it:
```bash
KAFKA_REST_URL=https://... \
OAUTH_TOKEN_URL=https://... \
CLIENT_ID=xxx \
CLIENT_SECRET=yyy \
./k6 run script.js
```

---

## Local Development (Docker)

Spin up a full local stack — Kafka, REST Proxy, mock OAuth server, and Kafbat UI:

```bash
cd local-dev
docker compose up -d
# UI available at http://localhost:8090
```

Then run the bundled test script:
```bash
./k6 run local-dev/test-script.js
```

---

## Examples

See the [`/examples`](examples/) folder for ready-to-run scripts:

| Script | What it shows |
|--------|--------------|
| [`single-message.js`](examples/single-message.js) | One message per iteration with `check()` assertions |
| [`batch-produce.js`](examples/batch-produce.js) | 50 K messages in 500-record batches across 10 VUs |
| [`multi-topic.js`](examples/multi-topic.js) | Multiple topics with independent per-topic thresholds |

---

## API Reference

### `new KafkaRestClient(config)`

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `baseUrl` | `string` | **required** | Confluent REST Proxy base URL |
| `tokenUrl` | `string` | **required** | OAuth 2.0 token endpoint |
| `clientId` | `string` | **required** | OAuth client ID |
| `clientSecret` | `string` | **required** | OAuth client secret |
| `scope` | `string` | `""` | OAuth scope (space-separated) |
| `maxBatchSize` | `number` | `500` | Max records per `produce()` call. Hard ceiling: `1000` |

---

### `client.produce(topic, messages)`

Publishes a batch of messages to a Kafka topic in a single HTTP request.

**Parameters**

| Name | Type | Description |
|------|------|-------------|
| `topic` | `string` | Kafka topic name |
| `messages` | `Message[]` | Array of message objects |

**`Message` shape**

```typescript
{
  key?:  string | object   // optional partition key
  value: object            // message payload (serialised as JSON)
}
```

**Returns** `ProduceResponse`

```typescript
{
  offsets: {
    partition:   number
    offset:      number
    error_code?: number   // non-zero means this record was rejected by Kafka
    error?:      string
  }[]
}
```

> ⚠️ **Per-record errors**: The REST Proxy can return HTTP `200 OK` while individual records
> have failed (non-zero `error_code`). The extension detects these and returns an error with
> details, also counting them in `kafka_rest_publish_errors`.

---

### `client.close()`

No-op. Reserved for future connection cleanup.

---

## Metrics

| Metric | Type | Tags | Description |
|--------|------|------|-------------|
| `kafka_rest_messages_sent` | Counter | `topic` | Number of records confirmed by Kafka |
| `kafka_rest_publish_duration` | Trend (ms) | `topic` | Round-trip time of each produce HTTP call |
| `kafka_rest_publish_errors` | Counter | `topic` | Number of failed records (HTTP or per-record) |

Use them in thresholds:

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

Type definitions are included in `index.d.ts`. To enable autocomplete in VS Code
or any TypeScript-aware editor:

**1. Copy or symlink the types into your project**

```bash
# from your k6 scripts folder
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

You'll now get full autocomplete and type checking for `KafkaRestClient`,
`Message`, `ProduceResponse`, and all config options.

---

## License

[Apache 2.0](LICENSE)
