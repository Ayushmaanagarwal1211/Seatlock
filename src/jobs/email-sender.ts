import { log } from '../shared/log';

export interface Email {
  idempotencyKey: string;
  to: string;
  subject: string;
  body: string;
}

// Thrown for errors that retrying won't fix (e.g. Relay rejects the request as invalid).
export class PermanentError extends Error {}

export interface EmailSender {
  send(email: Email): Promise<void>;
}

/**
 * Sends email through Relay (github.com/Ayushmaanagarwal1211/notificatiion_service), the
 * email delivery service from my first project: POST /v1/emails with an Idempotency-Key.
 *
 * The outbox delivers each email AT LEAST once: if we crash after Relay accepted it but
 * before marking the outbox row done, we send it again. Relay deduplicates on the key
 * (it answers the retry with the original email), so the user still gets one email.
 */
export class RelayEmailSender implements EmailSender {
  constructor(
    private relayUrl: string,
    private timeoutMs = 5000,
  ) {}

  async send(email: Email): Promise<void> {
    const res = await fetch(`${this.relayUrl}/v1/emails`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': email.idempotencyKey },
      body: JSON.stringify({ to: email.to, subject: email.subject, body: email.body, priority: 'transactional' }),
      signal: AbortSignal.timeout(this.timeoutMs),
    });
    if (res.status === 202) return;
    const message = `Relay answered HTTP ${res.status}: ${await res.text()}`;
    // 4xx means our request is wrong (bad address, key reused): retrying won't help.
    // 408 and 429 are the exceptions: "too slow" and "too many", worth retrying later.
    if (res.status >= 400 && res.status < 500 && res.status !== 408 && res.status !== 429) {
      throw new PermanentError(message);
    }
    throw new Error(message);
  }
}

// For running Seatlock without Relay: prints the email instead of sending it.
export class LogEmailSender implements EmailSender {
  async send(email: Email): Promise<void> {
    log.info(`[email] to ${email.to}: ${email.subject}`);
  }
}
