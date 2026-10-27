import { randomInt } from 'node:crypto';
import { GatewayEvent } from '../shared/gateway-types';
import { SIGNATURE_HEADER, signPayload } from '../shared/webhook-signature';

export interface WebhookChaos {
  duplicateRate: number; // share of events delivered twice
  maxDelayMs: number; // each delivery waits 0..maxDelayMs, so events can arrive out of order
  dropRate: number; // share of events never delivered (the merchant must reconcile)
}

export interface WebhookStats {
  events: number;
  deliveries: number; // successful POSTs, including duplicates
  duplicatesSent: number;
  dropped: number;
  failedAttempts: number; // non-2xx or network errors, retried later
}

const RETRY_DELAYS_MS = [500, 1000, 2000, 4000, 8000, 16000, 30000, 60000];

/**
 * Delivers webhooks like a real gateway: AT LEAST ONCE. Each event is retried with growing
 * delays until the merchant answers 2xx, and the chaos settings add the inconvenient parts
 * real gateways have: duplicates, random delays (so events arrive out of order), and
 * events that never arrive.
 */
export class WebhookSender {
  readonly stats: WebhookStats = { events: 0, deliveries: 0, duplicatesSent: 0, dropped: 0, failedAttempts: 0 };
  private timers = new Set<NodeJS.Timeout>();
  private inFlight = 0;
  private closed = false;

  constructor(
    private url: string,
    private secret: string,
    private chaos: WebhookChaos,
  ) {}

  send(event: GatewayEvent): void {
    this.stats.events++;
    if (Math.random() < this.chaos.dropRate) {
      this.stats.dropped++;
      return;
    }
    this.schedule(event, 0, this.delay());
    if (Math.random() < this.chaos.duplicateRate) {
      this.stats.duplicatesSent++;
      this.schedule(event, 0, this.delay() + 50);
    }
  }

  // Events still waiting to be delivered (for the load test, to know when it's done)
  get pending(): number {
    return this.timers.size + this.inFlight;
  }

  close(): void {
    this.closed = true;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
  }

  private delay(): number {
    return this.chaos.maxDelayMs > 0 ? randomInt(this.chaos.maxDelayMs + 1) : 0;
  }

  private schedule(event: GatewayEvent, attempt: number, delayMs: number): void {
    if (this.closed) return;
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      void this.deliver(event, attempt);
    }, delayMs);
    this.timers.add(timer);
  }

  private async deliver(event: GatewayEvent, attempt: number): Promise<void> {
    const body = JSON.stringify(event);
    this.inFlight++;
    try {
      const res = await fetch(this.url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', [SIGNATURE_HEADER]: signPayload(this.secret, body) },
        body,
        signal: AbortSignal.timeout(10_000),
      });
      if (res.ok) {
        this.stats.deliveries++;
        return;
      }
      throw new Error(`HTTP ${res.status}`);
    } catch {
      this.stats.failedAttempts++;
      if (attempt < RETRY_DELAYS_MS.length) this.schedule(event, attempt + 1, RETRY_DELAYS_MS[attempt]);
    } finally {
      this.inFlight--;
    }
  }
}
