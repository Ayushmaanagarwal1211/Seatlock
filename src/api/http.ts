import { Request, Response } from 'express';
import { z } from 'zod';
import { idempotencyKeySchema, userIdSchema, uuidSchema } from './schemas';

// Small helpers shared by the routes. Each returns the parsed value, or sends the error
// response and returns null (the route then just returns).

export function requireUser(req: Request, res: Response): string | null {
  const user = userIdSchema.safeParse(req.get('X-User-Id'));
  if (!user.success) {
    res.status(401).json({ error: 'X-User-Id header is required.' });
    return null;
  }
  return user.data;
}

export function requireIdempotencyKey(req: Request, res: Response): string | null {
  const key = idempotencyKeySchema.safeParse(req.get('Idempotency-Key'));
  if (!key.success) {
    res.status(400).json({ error: 'Idempotency-Key header is required (max 255 characters).' });
    return null;
  }
  return key.data;
}

export function parseBody<T extends z.ZodType>(schema: T, req: Request, res: Response): z.infer<T> | null {
  const body = schema.safeParse(req.body);
  if (!body.success) {
    res.status(400).json({ error: 'Invalid request body', details: z.flattenError(body.error) });
    return null;
  }
  return body.data;
}

// A malformed id can't exist, so it's a 404 like any other unknown id.
export function parseId(value: unknown, res: Response, what: string): string | null {
  const id = uuidSchema.safeParse(value);
  if (!id.success) {
    res.status(404).json({ error: `${what} not found` });
    return null;
  }
  return id.data;
}
