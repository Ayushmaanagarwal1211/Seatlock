import { z } from 'zod';

export const uuidSchema = z.uuid();

// The user id comes from a header. In production an API gateway would set it after
// verifying the user's login token; authentication is out of scope for this service.
export const userIdSchema = z.string().trim().min(1).max(100);

// Clients generate this, usually a UUID, once per action, and send the same value on retries.
export const idempotencyKeySchema = z.string().trim().min(1).max(255);

export const createEventSchema = z.object({
  name: z.string().trim().min(1).max(200),
  venue: z.string().trim().min(1).max(200),
  startsAt: z.coerce.date(),
  holdSeconds: z.number().int().min(1).max(3600).default(600),
  maxSeatsPerHold: z.number().int().min(1).max(10).default(6),
  sections: z
    .array(
      z.object({
        name: z.string().trim().min(1).max(50),
        rows: z.number().int().min(1).max(200),
        seatsPerRow: z.number().int().min(1).max(500),
        pricePaise: z.number().int().positive(),
      }),
    )
    .min(1)
    .refine((sections) => new Set(sections.map((s) => s.name)).size === sections.length, 'Section names must be unique')
    .refine((sections) => sections.reduce((n, s) => n + s.rows * s.seatsPerRow, 0) <= 100_000, 'At most 100,000 seats'),
});
export type CreateEventInput = z.infer<typeof createEventSchema>;

export const createHoldSchema = z.object({
  seatIds: z
    .array(z.number().int().positive())
    .min(1)
    .max(10)
    .refine((ids) => new Set(ids).size === ids.length, 'Seat ids must be unique'),
});
export type CreateHoldInput = z.infer<typeof createHoldSchema>;
