import { Admin, ListOffsetTimestamps, Producer, ProduceAcks, stringSerializers } from '@platformatic/kafka';

export interface FlashOrderMessage {
  orderId: string;
  saleId: string;
  userId: string;
  reservedAt: string;
}

export function createProducer(brokers: string[], clientId: string): Producer<string, string, string, string> {
  return new Producer({
    clientId,
    bootstrapBrokers: brokers,
    serializers: stringSerializers,
    // Idempotent producer: if a send is retried after a network blip, the broker drops the
    // duplicate instead of writing the message twice.
    idempotent: true,
    // acks = all: the broker answers only once every in-sync replica has the message, so an
    // acknowledged order survives a broker crash.
    acks: ProduceAcks.ALL,
  });
}

// Creates the topic if it doesn't exist yet. Safe to call on every startup.
export async function ensureTopic(brokers: string[], topic: string, partitions: number): Promise<void> {
  const admin = new Admin({ clientId: 'seatlock-admin', bootstrapBrokers: brokers });
  try {
    const existing = await admin.listTopics();
    if (!existing.includes(topic)) {
      await admin.createTopics({ topics: [topic], partitions, replicas: 1 });
    }
  } catch (err) {
    if (!String((err as Error).message).includes('already exists')) throw err;
  } finally {
    await admin.close();
  }
}

/**
 * Consumer lag: how many messages in the topic the group hasn't committed yet.
 * 0 means the consumers have processed everything that was published.
 */
export async function consumerLag(brokers: string[], groupId: string, topic: string, partitions: number): Promise<number> {
  const admin = new Admin({ clientId: 'seatlock-lag', bootstrapBrokers: brokers });
  try {
    const [group] = await admin.listConsumerGroupOffsets({ groups: [groupId] });
    const committed = new Map(
      (group?.topics.find((t) => t.name === topic)?.partitions ?? []).map((p) => [p.partitionIndex, p.committedOffset]),
    );
    const [latest] = await admin.listOffsets({
      topics: [
        {
          name: topic,
          partitions: Array.from({ length: partitions }, (_, i) => ({ partitionIndex: i, timestamp: ListOffsetTimestamps.LATEST })),
        },
      ],
    });
    let lag = 0n;
    for (const p of latest.partitions) {
      const done = committed.get(p.partitionIndex) ?? 0n;
      lag += p.offset - (done > 0n ? done : 0n);
    }
    return Number(lag);
  } finally {
    await admin.close();
  }
}

/**
 * Collects messages for a few milliseconds and sends them to Kafka in one request.
 *
 * At thousands of buys per second, one Kafka request per buy would spend most of the time
 * on round trips. Waiting up to `lingerMs` (5ms) to fill a batch is the same trade-off as
 * Kafka's own linger.ms setting: a little latency for much higher throughput. Each caller
 * still gets its own promise, resolved when ITS batch is acknowledged.
 */
export class BatchingProducer {
  private queue: { message: FlashOrderMessage; resolve: () => void; reject: (err: Error) => void }[] = [];
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private producer: Producer<string, string, string, string>,
    private topic: string,
    private lingerMs: number,
    private maxBatch = 500,
  ) {}

  send(message: FlashOrderMessage): Promise<void> {
    return new Promise((resolve, reject) => {
      this.queue.push({ message, resolve, reject });
      if (this.queue.length >= this.maxBatch) this.flush();
      else if (!this.timer) this.timer = setTimeout(() => this.flush(), this.lingerMs);
    });
  }

  async close(): Promise<void> {
    this.flush();
    await this.producer.close();
  }

  private flush(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    const batch = this.queue.splice(0, this.queue.length);
    if (batch.length === 0) return;
    this.producer
      .send({
        messages: batch.map(({ message }) => ({
          topic: this.topic,
          // Keyed by user: one user's messages always go to the same partition, in order.
          key: message.userId,
          value: JSON.stringify(message),
        })),
      })
      .then(
        () => batch.forEach((item) => item.resolve()),
        (err: Error) => batch.forEach((item) => item.reject(err)),
      );
  }
}
