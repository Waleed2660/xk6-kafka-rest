/**
 * multi-topic.js
 *
 * Publishes to multiple Kafka topics in a single test run.
 * Shows how per-topic metric tags let you set independent thresholds
 * for each topic.
 *
 * Run:
 *   KAFKA_REST_URL=https://... OAUTH_TOKEN_URL=https://... \
 *   CLIENT_ID=xxx CLIENT_SECRET=yyy \
 *   ./k6 run examples/multi-topic.js
 */
import { KafkaRestClient } from 'k6/x/kafka-rest';
import { check } from 'k6';

export const options = {
  vus:        5,
  iterations: 50,
  thresholds: {
    'kafka_rest_publish_duration{topic:orders}':       ['p(95)<300'],
    'kafka_rest_publish_duration{topic:analytics}':    ['p(95)<800'],
    'kafka_rest_publish_errors':                       ['count==0'],
  },
};

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL      || 'http://localhost:8082',
  tokenUrl:     __ENV.OAUTH_TOKEN_URL     || 'http://localhost:8080/default/token',
  clientId:     __ENV.CLIENT_ID           || 'test-client',
  clientSecret: __ENV.CLIENT_SECRET       || 'test-secret',
  scope:        __ENV.OAUTH_SCOPE         || 'kafka',
});

function randomHex(len) {
  let s = '';
  while (s.length < len) s += Math.random().toString(16).slice(2);
  return s.slice(0, len);
}

export default function () {
  const orderId = randomHex(16);

  const orderResult = client.produce('orders', [
    {
      key:   orderId,
      value: { orderId, event: 'ORDER_PLACED', amount: parseFloat((Math.random() * 500).toFixed(2)) },
    },
  ]);
  check(orderResult, { 'order published': (r) => r.offsets[0] && !r.offsets[0].error_code });

  const analyticsMessages = Array.from({ length: 10 }, () => ({
    key:   randomHex(16),
    value: {
      sessionId:  randomHex(20),
      event:      'PAGE_VIEW',
      userId:     randomHex(12),
      properties: { page: '/checkout', referrer: 'https://example.com', ts: Date.now() },
    },
  }));

  const analyticsResult = client.produce('analytics', analyticsMessages);
  check(analyticsResult, { 'analytics published': (r) => r.offsets.every((o) => !o.error_code) });
}

