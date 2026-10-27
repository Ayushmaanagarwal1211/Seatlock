// What the payment gateway's API and webhooks look like (the mock in src/gateway, and the
// client in src/api/payments both use these).

export type GatewayPaymentStatus = 'created' | 'succeeded' | 'failed' | 'refunded';

export interface GatewayPayment {
  id: string; // e.g. pay_3f9c...
  reference: string; // the merchant's own id for it: Seatlock's payment id
  amountPaise: number;
  status: GatewayPaymentStatus;
  createdAt: string;
}

export type GatewayEventType = 'payment.succeeded' | 'payment.failed' | 'refund.succeeded';

export interface GatewayEvent {
  id: string; // e.g. evt_8a1b...: unique per event, the same on every delivery of it
  type: GatewayEventType;
  createdAt: string;
  data: { paymentId: string; reference: string; amountPaise: number };
}
