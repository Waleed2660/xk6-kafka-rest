/**
 * batch-produce.js
 *
 * Publishes messages in large batches for maximum throughput.
 * Demonstrates batch sizing, thresholds, and per-topic metrics.
 *
 * Run:
 *   KAFKA_REST_URL=https://... OAUTH_TOKEN_URL=https://... \
 *   CLIENT_ID=xxx CLIENT_SECRET=yyy \
 *   ./k6 run examples/batch-produce.js
 */
import { KafkaRestClient } from 'k6/x/kafka-rest';
import { check } from 'k6';

const BATCH_SIZE  = 200;
const TOTAL_MSGS  = 50_000;

export const options = {
  scenarios: {
    publish: {
      executor:   'shared-iterations',
      vus:        10,
      iterations: TOTAL_MSGS / BATCH_SIZE,
    },
  },
  thresholds: {
    'kafka_rest_publish_duration{topic:orders}': ['p(95)<500'],
    'kafka_rest_publish_errors':                 ['count==0'],
  },
};

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL      || 'http://localhost:8082',
  tokenUrl:     __ENV.OAUTH_TOKEN_URL     || 'http://localhost:8080/default/token',
  clientId:     __ENV.CLIENT_ID           || 'test-client',
  clientSecret: __ENV.CLIENT_SECRET       || 'test-secret',
  scope:        __ENV.OAUTH_SCOPE         || 'kafka',
  maxBatchSize: BATCH_SIZE,
});

function randomHex(len) {
  let s = '';
  while (s.length < len) s += Math.random().toString(16).slice(2);
  return s.slice(0, len);
}

function randomUUID() {
  return `${randomHex(8)}-${randomHex(4)}-4${randomHex(3)}-${randomHex(4)}-${randomHex(12)}`;
}

function buildOrderEvent() {
  return {
    eventId:   randomUUID(),
    timestamp: new Date().toISOString(),
    orderId:   randomHex(20),
    status:    ['PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'][Math.floor(Math.random() * 4)],
    amount:    parseFloat((Math.random() * 10000).toFixed(2)),
    currency:  'USD',
    customerId: randomHex(16),
  };
}

export default function () {
  const messages = Array.from({ length: BATCH_SIZE }, () => ({
    key:   randomUUID(),
    value: buildOrderEvent(),
  }));

  const result = client.produce('orders', messages);

  check(result, {
    'batch accepted':       (r) => r.offsets.length === BATCH_SIZE,
    'no per-record errors': (r) => r.offsets.every((o) => !o.error_code),
  });
}

