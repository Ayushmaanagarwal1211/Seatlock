import { Router } from 'express';
import type { AppDeps } from '../app';
import { parseId, requireUser } from '../http';
import { getHold, releaseHold, toHoldResponse } from './holds.service';

export function holdsRouter({ pool, holdStrategy }: AppDeps): Router {
  const router = Router();

  router.get('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Hold');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;
    const hold = await getHold(pool, id, userId);
    if (!hold) {
      res.status(404).json({ error: 'Hold not found' });
      return;
    }
    res.json(toHoldResponse(hold));
  });

  // DELETE /v1/holds/:id: give the seats back before paying
  router.delete('/:id', async (req, res) => {
    const id = parseId(req.params.id, res, 'Hold');
    if (!id) return;
    const userId = requireUser(req, res);
    if (!userId) return;

    const result = await releaseHold(pool, holdStrategy, id, userId);
    switch (result.kind) {
      case 'released':
        res.json(toHoldResponse(result.hold));
        return;
      case 'not_found':
        res.status(404).json({ error: 'Hold not found' });
        return;
      case 'not_active':
        res.status(409).json({ error: `The hold is ${result.status}.` });
        return;
      case 'payment_in_progress':
        res.status(409).json({ error: 'A payment for this hold is in progress.' });
        return;
    }
  });

  return router;
}
