// script.js
import { KafkaRestClient } from 'k6/x/kafka-rest';

export const options = {
  scenarios: {
    publish: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 1000,  // 1000 iterations × 50 messages = 50,000 total
    },
  },
};

const CLUSTER_ID = __ENV.CLUSTER_ID || 'local-dev-cluster-0001';

const client = new KafkaRestClient({
  baseUrl:      'http://localhost:8082',
  tokenUrl:     'http://localhost:8080/default/token',
  clientId:     'test-client',
  clientSecret: 'test-secret',
  scope:        'kafka',
  apiVersion:   'v3',
  clusterId:    CLUSTER_ID,
  maxBatchSize: 50,
});

const BATCH_SIZE = 50;

// ── Helpers ────────────────────────────────────────────────────────────────

function randomHex(len) {
  let s = '';
  while (s.length < len) s += Math.random().toString(16).slice(2);
  return s.slice(0, len);
}

function randomKey() {
  return `${randomHex(8)}-${randomHex(4)}-4${randomHex(3)}-${randomHex(4)}-${randomHex(12)}`;
}

// Moderately large payload (~700–900 bytes of JSON per message)
function buildPayload() {
  return {
    eventId:   randomKey(),
    timestamp: new Date().toISOString(),
    source:    'xk6-kafka-rest-load-test',
    version:   '1.0.0',
    user: {
      id:       randomHex(16),
      username: `user_${randomHex(8)}`,
      email:    `user_${randomHex(6)}@example.com`,
      roles:    ['reader', 'writer'],
      metadata: {
        region:    'eu-west-1',
        tenantId:  randomHex(12),
        sessionId: randomKey(),
      },
    },
    payload: {
      orderId:    randomHex(20),
      status:     ['PENDING', 'PROCESSING', 'COMPLETED', 'FAILED'][Math.floor(Math.random() * 4)],
      amount:     parseFloat((Math.random() * 10000).toFixed(2)),
      currency:   'USD',
      items: [
        { sku: `SKU-${randomHex(8)}`, qty: Math.ceil(Math.random() * 10), price: parseFloat((Math.random() * 500).toFixed(2)) },
        { sku: `SKU-${randomHex(8)}`, qty: Math.ceil(Math.random() * 10), price: parseFloat((Math.random() * 500).toFixed(2)) },
        { sku: `SKU-${randomHex(8)}`, qty: Math.ceil(Math.random() * 10), price: parseFloat((Math.random() * 500).toFixed(2)) },
      ],
      shippingAddress: {
        line1:   `${Math.ceil(Math.random() * 999)} Load Test Street`,
        city:    'Testville',
        country: 'GB',
        zip:     `TS${randomHex(4).toUpperCase()}`,
      },
    },
    tags: [`tag-${randomHex(4)}`, `tag-${randomHex(4)}`, 'k6'],
    correlationId: randomKey(),
  };
}

// ── Test ───────────────────────────────────────────────────────────────────

export default function () {
  const messages = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    messages.push({
      key:   randomKey(),
      value: buildPayload(),
      headers: [
        { key: 'x-trace-id',       value: randomKey() },
        { key: 'x-source-service', value: 'xk6-kafka-rest-load-test' },
        { key: 'schema-version',   value: '1' },
      ],
    });
  }

  const result = client.produce('my-topic', messages);
  const sent = result.offsets.length;
  const partitions = [...new Set(result.offsets.map(o => o.partition))].length;
  console.log(`sent=${sent} across ${partitions} partition(s)`);
}