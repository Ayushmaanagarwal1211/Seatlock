import { GatewayPayment } from './gateway-types';

export class GatewayError extends Error {
  constructor(
    message: string,
    readonly status?: number,
  ) {
    super(message);
  }
}

/**
 * Seatlock's client for the payment gateway's API. Every call that creates something
 * sends an Idempotency-Key, so a retried call (after a timeout, a crash, a redeploy)
 * returns the original payment or refund instead of charging or refunding twice.
 */
export class GatewayClient {
  constructor(
    private baseUrl: string,
    private apiKey: string,
    private timeoutMs = 5000,
  ) {}

  // The idempotency key is Seatlock's payment id: one gateway payment per Seatlock payment.
  createPayment(paymentId: string, amountPaise: number): Promise<GatewayPayment> {
    return this.request('POST', '/v1/payments', { amountPaise, reference: paymentId }, paymentId);
  }

  getPayment(gatewayPaymentId: string): Promise<GatewayPayment> {
    return this.request('GET', `/v1/payments/${encodeURIComponent(gatewayPaymentId)}`);
  }

  // One refund per Seatlock payment, however many times this is retried.
  refund(paymentId: string, gatewayPaymentId: string): Promise<GatewayPayment> {
    return this.request('POST', '/v1/refunds', { paymentId: gatewayPaymentId }, `refund:${paymentId}`);
  }

  payUrl(gatewayPaymentId: string): string {
    return `${this.baseUrl}/v1/payments/${encodeURIComponent(gatewayPaymentId)}/complete`;
  }

  private async request<T>(method: string, path: string, body?: object, idempotencyKey?: string): Promise<T> {
    let res: Response;
    try {
      res = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers: {
          Authorization: `Bearer ${this.apiKey}`,
          ...(body ? { 'Content-Type': 'application/json' } : {}),
          ...(idempotencyKey ? { 'Idempotency-Key': idempotencyKey } : {}),
        },
        body: body ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      throw new GatewayError(`gateway unreachable: ${(err as Error).message}`);
    }
    if (!res.ok) throw new GatewayError(`gateway ${method} ${path} failed: HTTP ${res.status}`, res.status);
    return (await res.json()) as T;
  }
}
