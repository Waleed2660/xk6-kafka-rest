// script.js
import { KafkaRestClient } from 'k6/x/kafka-rest';

// 50,000 messages ÷ 200 per batch = 250 iterations
export const options = {
  scenarios: {
    publish: {
      executor: 'shared-iterations',
      vus: 10,
      iterations: 250,   // 250 batches × 200 messages = 50,000 total
    },
  },
};

const client = new KafkaRestClient({
  baseUrl:      'http://localhost:8082',
  tokenUrl:     'http://localhost:8080/default/token',
  clientId:     'test-client',
  clientSecret: 'test-secret',
  scope:        'kafka',
});

const BATCH_SIZE = 500;

// ── Helpers ────────────────────────────────────────────────────────────────

function randomHex(len) {
  let s = '';
  while (s.length < len) s += Math.random().toString(16).slice(2);
  return s.slice(0, len);
}

function randomKey() {
  // UUID v4 shape: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
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
    messages.push({ key: randomKey(), value: buildPayload() });
  }

  const result = client.produce('my-topic', messages);
  console.log(`batch offsets: ${result.offsets[0].offset} → ${result.offsets[result.offsets.length - 1].offset}`);
}