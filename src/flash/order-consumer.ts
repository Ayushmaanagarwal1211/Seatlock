import { Consumer, Message, MessagesStream, MessagesStreamFallbackModes, MessagesStreamModes, stringDeserializers } from '@platformatic/kafka';
import { Pool } from 'pg';
import { FlashOrderMessage } from '../shared/kafka';
import { log } from '../shared/log';
import { writeOrders, WriteResult } from './order-writer';

type OrderMessage = Message<string, string, string, string>;

export interface OrderConsumerOptions {
  pool: Pool;
  brokers: string[];
  topic: string;
  groupId: string;
  batchSize: number; // write once this many orders are waiting...
  flushMs: number; // ...or after this long, whichever comes first
}

/**
 * Reads orders from Kafka and writes them to Postgres in batches.
 *
 * Offsets are committed MANUALLY, and only after the batch is safely in Postgres. If this
 * process crashes in between, Kafka gives the same messages again (from the last committed
 * offset) to whichever writer takes over the partition: at-least-once delivery, made safe
 * by writeOrders ignoring orders it already has.
 *
 * Several writers can run in the same consumer group: Kafka splits the topic's partitions
 * between them.
 */
export class OrderConsumer {
  readonly totals: WriteResult = { inserted: 0, duplicates: 0, rejected: 0 };
  private consumer: Consumer<string, string, string, string>;
  private stream: MessagesStream<string, string, string, string> | null = null;
  private buffer: OrderMessage[] = [];
  private writing: Promise<void> | null = null;
  private timer: NodeJS.Timeout | null = null;
  private stopped = false;

  constructor(private options: OrderConsumerOptions) {
    this.consumer = new Consumer({
      clientId: `order-writer-${process.pid}`,
      groupId: options.groupId,
      bootstrapBrokers: options.brokers,
      deserializers: stringDeserializers,
      // If a writer dies, Kafka moves its partitions to the others once it misses heartbeats
      // for sessionTimeout. The client's default is a minute; 10s keeps the stall short.
      sessionTimeout: 10_000,
      rebalanceTimeout: 15_000,
      heartbeatInterval: 1000,
    });
  }

  async start(): Promise<void> {
    this.stream = await this.consumer.consume({
      topics: [this.options.topic],
      // Resume after the last committed offset; a brand-new group starts at the beginning.
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
      autocommit: false,
      maxWaitTime: 100,
    });
    this.stream.on('data', (message: OrderMessage) => {
      this.buffer.push(message);
      if (this.buffer.length >= this.options.batchSize) void this.flush();
    });
    this.stream.on('error', (err: Error) => log.error('[order-writer] stream error:', err));
    this.timer = setInterval(() => void this.flush(), this.options.flushMs);
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.stream?.pause();
    await this.writing;
    await this.flush(); // write what's buffered so the next writer doesn't redo it
    await this.consumer.close(true);
  }

  // One batch at a time. While a batch is being written, the stream is paused so the
  // buffer can't grow without limit (backpressure: unread orders wait safely in Kafka).
  private async flush(): Promise<void> {
    if (this.writing || this.buffer.length === 0) return;
    const batch = this.buffer.splice(0, this.options.batchSize);
    this.stream?.pause();
    this.writing = this.writeWithRetry(batch).finally(() => {
      this.writing = null;
      if (!this.stopped) this.stream?.resume();
    });
    await this.writing;
    if (this.buffer.length >= this.options.batchSize) await this.flush();
  }

  private async writeWithRetry(batch: OrderMessage[]): Promise<void> {
    const orders = batch.map((m) => JSON.parse(m.value) as FlashOrderMessage);
    for (let attempt = 1; ; attempt++) {
      try {
        const result = await writeOrders(this.options.pool, orders);
        this.totals.inserted += result.inserted;
        this.totals.duplicates += result.duplicates;
        this.totals.rejected += result.rejected;
        break;
      } catch (err) {
        // Postgres is down or overloaded: keep the batch, wait, try again. Nothing is
        // committed to Kafka until it succeeds.
        log.error(`[order-writer] writing ${orders.length} orders failed (attempt ${attempt}), retrying:`, err);
        await new Promise((r) => setTimeout(r, Math.min(200 * 2 ** attempt, 5000)));
      }
    }
    // Commit the last message of each partition: that commits everything before it too.
    const lastByPartition = new Map<number, OrderMessage>();
    for (const m of batch) lastByPartition.set(m.partition, m);
    await Promise.all([...lastByPartition.values()].map((m) => m.commit()));
  }
}
