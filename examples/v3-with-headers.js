/**
 * v3-with-headers.js
 *
 * Publishes messages using the REST Proxy v3 API with full header support.
 * Use this when your Confluent deployment only exposes the v3 API, or when
 * you need per-record Kafka headers (e.g. trace IDs, schema versions).
 *
 * v3 vs v2:
 *   v3 sends one HTTP call per record (concurrently), supports headers.
 *   v2 sends all records in one HTTP call (batched), no headers.
 *
 * Prerequisites:
 *   Find your cluster ID: curl {baseUrl}/v3/clusters | jq '.data[0].cluster_id'
 *
 * Run:
 *   KAFKA_REST_URL=https://... OAUTH_TOKEN_URL=https://... \
 *   CLIENT_ID=xxx CLIENT_SECRET=yyy CLUSTER_ID=lkc-abc123 \
 *   ./k6 run examples/v3-with-headers.js
 */
import { KafkaRestClient } from 'k6/x/kafka-rest';
import { check } from 'k6';

export const options = {
  vus:        5,
  iterations: 50,
};

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL  || 'http://localhost:8082',
  tokenUrl:     __ENV.OAUTH_TOKEN_URL || 'http://localhost:8080/default/token',
  clientId:     __ENV.CLIENT_ID       || 'test-client',
  clientSecret: __ENV.CLIENT_SECRET   || 'test-secret',
  scope:        __ENV.OAUTH_SCOPE     || 'kafka',
  apiVersion:   'v3',
  clusterId:    __ENV.CLUSTER_ID      || 'cluster_1',
  maxBatchSize: 20, // concurrent HTTP calls per produce() invocation
});

function randomHex(len) {
  let s = '';
  while (s.length < len) s += Math.random().toString(16).slice(2);
  return s.slice(0, len);
}

export default function () {
  const traceId = randomHex(32);

  const result = client.produce('payments', [
    {
      key: randomHex(16),
      value: {
        paymentId: randomHex(20),
        amount:    parseFloat((Math.random() * 1000).toFixed(2)),
        currency:  'GBP',
        status:    'AUTHORISED',
      },
      headers: [
        { key: 'x-trace-id',       value: traceId },
        { key: 'x-source-service', value: 'payment-gateway' },
        { key: 'schema-version',   value: '2' },
        { key: 'content-type',     value: 'application/json' },
      ],
    },
  ]);

  check(result, {
    'published':        (r) => r.offsets.length === 1,
    'no record errors': (r) => r.offsets[0].error_code == null,
  });

  console.log(`trace=${traceId} partition=${result.offsets[0].partition} offset=${result.offsets[0].offset}`);
}

