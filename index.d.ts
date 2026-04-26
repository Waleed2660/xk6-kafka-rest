/**
 * xk6-kafka-rest — TypeScript type definitions
 *
 * Import in your k6 script:
 *   import { KafkaRestClient } from 'k6/x/kafka-rest';
 *
 * For IDE support add to your tsconfig.json:
 *   { "compilerOptions": { "paths": { "k6/x/kafka-rest": ["./node_modules/xk6-kafka-rest/index.d.ts"] } } }
 */

/**
 * Configuration passed to the KafkaRestClient constructor.
 */
export interface ClientConfig {
  /** Confluent REST Proxy base URL — e.g. `https://xxx.confluent.cloud` */
  baseUrl: string;

  /** OAuth 2.0 token endpoint — e.g. `https://idp.example.com/oauth/token` */
  tokenUrl: string;

  /** OAuth client ID */
  clientId: string;

  /** OAuth client secret */
  clientSecret: string;

  /**
   * OAuth scope (space-separated).
   * @default ""
   */
  scope?: string;

  /**
   * Number of records per HTTP call to the REST Proxy.
   * Arrays larger than this are automatically split and sent as sequential requests.
   * Ceiling is 1000 (REST Proxy request size limit). Defaults to 500.
   * @default 500
   */
  maxBatchSize?: number;
}

/**
 * A single Kafka record to be published.
 */
export interface Message {
  /**
   * Optional partition key. When set, Kafka routes the message to a
   * consistent partition determined by the key hash.
   */
  key?: string | Record<string, unknown>;

  /** Message payload — serialised as JSON by the REST Proxy. */
  value: Record<string, unknown> | unknown;
}

/**
 * Per-record result returned by the REST Proxy.
 */
export interface OffsetMetadata {
  /** Partition the record landed in. */
  partition: number;

  /** Offset of the record within the partition. */
  offset: number;

  /**
   * Non-zero if this individual record was rejected by Kafka.
   * The extension surfaces these as errors even when the HTTP response is 200 OK.
   */
  error_code?: number;

  /** Human-readable error description when `error_code` is set. */
  error?: string;
}

/**
 * Response returned by `client.produce()`.
 */
export interface ProduceResponse {
  /** Schema ID used for the message key (present when using Schema Registry). */
  key_schema_id?: number;

  /** Schema ID used for the message value (present when using Schema Registry). */
  value_schema_id?: number;

  /**
   * One entry per record in the batch.
   * Check `error_code` on each entry to detect per-record failures.
   */
  offsets: OffsetMetadata[];
}

/**
 * k6 extension client for publishing JSON messages to Confluent Kafka
 * via the REST Proxy with OAuth 2.0 authentication.
 *
 * @example
 * ```ts
 * import { KafkaRestClient } from 'k6/x/kafka-rest';
 *
 * const client = new KafkaRestClient({
 *   baseUrl:      'https://xxx.confluent.cloud',
 *   tokenUrl:     'https://idp.example.com/oauth/token',
 *   clientId:     __ENV.CLIENT_ID,
 *   clientSecret: __ENV.CLIENT_SECRET,
 *   scope:        'kafka',
 *   maxBatchSize: 200,
 * });
 *
 * export default function () {
 *   const result = client.produce('my-topic', [
 *     { key: 'order-1', value: { event: 'ORDER_PLACED', amount: 99.99 } },
 *   ]);
 *
 *   result.offsets.forEach((o) => {
 *     if (o.error_code) console.error(`record failed: ${o.error}`);
 *   });
 * }
 * ```
 */
export declare class KafkaRestClient {
  constructor(config: ClientConfig);

  /**
   * Publishes a batch of messages to a Kafka topic in a single HTTP request.
   *
   * Throws if:
   * - `messages.length` exceeds `maxBatchSize` (default 500, hard ceiling 1000)
   * - The OAuth token cannot be obtained after retries
   * - The REST Proxy returns a non-2xx HTTP status
   * - One or more records contain a non-zero `error_code` in the response
   *
   * @param topic   - Kafka topic name
   * @param messages - Batch of records to publish
   * @returns ProduceResponse with per-record offset metadata
   */
  produce(topic: string, messages: Message[]): ProduceResponse;

  /**
   * Releases any held resources. Safe to call at end-of-test in a `teardown`
   * function. Currently a no-op; reserved for future connection cleanup.
   */
  close(): void;
}

