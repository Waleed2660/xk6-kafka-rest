/**
 * single-message.js
 *
 * The simplest possible example — publish one message per iteration.
 * Good starting point for verifying your setup works end-to-end.
 *
 * Run:
 *   KAFKA_REST_URL=https://... OAUTH_TOKEN_URL=https://... \
 *   CLIENT_ID=xxx CLIENT_SECRET=yyy \
 *   ./k6 run examples/single-message.js
 */
import { KafkaRestClient } from 'k6/x/kafka-rest';
import { check } from 'k6';

export const options = {
  vus: 1,
  iterations: 1,
};

const client = new KafkaRestClient({
  baseUrl:      __ENV.KAFKA_REST_URL      || 'http://localhost:8082',
  tokenUrl:     __ENV.OAUTH_TOKEN_URL     || 'http://localhost:8080/default/token',
  clientId:     __ENV.CLIENT_ID           || 'test-client',
  clientSecret: __ENV.CLIENT_SECRET       || 'test-secret',
  scope:        __ENV.OAUTH_SCOPE         || 'kafka',
});

export default function () {
  const result = client.produce('my-topic', [
    {
      key:   'order-001',
      value: {
        orderId:   'order-001',
        event:     'ORDER_PLACED',
        amount:    49.99,
        currency:  'USD',
        timestamp: new Date().toISOString(),
      },
    },
  ]);

  check(result, {
    'message published':     (r) => r.offsets.length === 1,
    'no per-record errors':  (r) => r.offsets.every((o) => !o.error_code),
  });

  console.log(`published to partition ${result.offsets[0].partition} at offset ${result.offsets[0].offset}`);
}

