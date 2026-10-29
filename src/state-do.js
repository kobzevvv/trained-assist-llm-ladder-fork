// LadderState Durable Object — ONE global instance holds model health + Go key rotation for every
// caller, with strong consistency (all requests serialize through this object). Methods are called
// via Workers RPC from src/index.js; state persists in the object's storage.
//
// Pins (epic #17) are stored SEPARATELY from the shared 'state' blob — one key per conversation
// (`pin:<Kh>`) — so a write never rewrites the whole health object, and the health blob never
// carries thousands of pin entries. Hot path: snapshot() reads at most ONE extra key (piggybacked
// on the already-existing snapshot RPC); writes piggyback on recordSuccess / recordFailure.
// Expired pins are dropped lazily by a throttled sweep (≤ once/min, after ~100 pin writes) — never
// a per-request scan.

import { DurableObject } from 'cloudflare:workers';
import * as S from './state.js';

const KEY = 'state';
const PIN_PREFIX = 'pin:';
const SWEEP_MIN_MS = 60 * 1000;
const SWEEP_OPS = 100;

export class LadderState extends DurableObject {
  async _load() {
    if (!this._state) this._state = (await this.ctx.storage.get(KEY)) || S.emptyState();
    return this._state;
  }

  async _save() {
    await this.ctx.storage.put(KEY, this._state);
  }

  async snapshot(poolSize, pinKey) {
    const s = S.snapshot(await this._load(), poolSize);
    let pin = null;
    if (pinKey) {
      try {
        const raw = await this.ctx.storage.get(PIN_PREFIX + String(pinKey));
        if (S.pinFresh(raw)) pin = raw;
        // an expired pin is dropped by the sweep — read path stays read-only
      } catch { /* fail-open: no pin, the request proceeds as a new conversation */ }
    }
    return { ...s, pin };
  }

  async recordFailure(model, failure, extra) {
    S.recordFailure(await this._load(), model, failure);
    if (extra && extra.pinRemove) {
      try { await this.ctx.storage.delete(PIN_PREFIX + String(extra.pinRemove.pinKey)); } catch { /* fail-open */ }
    }
    await this._save();
    await this._sweepMaybe();
  }

  async recordSuccess(model, extra) {
    const st = await this._load();
    const needsSave = !!st.health[model];
    if (needsSave) S.recordSuccess(st, model);
    if (extra && extra.pin) {
      const key = PIN_PREFIX + String(extra.pin.pinKey);
      try {
        const v = S.pinDirty(await this.ctx.storage.get(key), extra.pin.rung);
        if (v) await this.ctx.storage.put(key, v);
      } catch { /* fail-open: the pin write is best-effort */ }
    }
    if (needsSave) await this._save(); // healthy rung: no state write, as before
    await this._sweepMaybe();
  }

  async pinStats() {
    const byRung = {};
    let count = 0;
    try {
      const now = Date.now();
      const list = await this.ctx.storage.list({ prefix: PIN_PREFIX });
      for (const [name, value] of list) {
        if (S.pinFresh(value, now)) {
          count++;
          byRung[value.rung] = (byRung[value.rung] || 0) + 1;
        } else {
          await this.ctx.storage.delete(name); // opportunistic cleanup
        }
      }
    } catch { /* fail-open: report zeros rather than fail /v1/state */ }
    return { count, byRung };
  }

  // Lazy LRU/TTL sweep — throttled so it never runs per request. Clearing on the shared health
  // blob is NOT part of it: TTL is the primary limiter, the sweep only reclaims storage.
  async _sweepMaybe() {
    if (this._sweepAt && Date.now() - this._sweepAt < SWEEP_MIN_MS) return;
    this._sweepOps = (this._sweepOps || 0) + 1;
    if (this._sweepOps < SWEEP_OPS) return;
    this._sweepAt = Date.now();
    this._sweepOps = 0;
    try {
      const now = Date.now();
      const list = await this.ctx.storage.list({ prefix: PIN_PREFIX });
      for (const [name, value] of list) if (!S.pinFresh(value, now)) await this.ctx.storage.delete(name);
    } catch { /* sweep is best-effort */ }
  }

  async rotateKey(poolSize, ttlMs, failedIndex, scope) {
    const r = S.rotateKey(await this._load(), poolSize, ttlMs, Date.now(), failedIndex, scope);
    await this._save();
    return r;
  }

  async park(models, untilMs) {
    S.park(await this._load(), models, untilMs);
    await this._save();
  }

  // Park an OpenRouter ACCOUNT until its own limit resets (issue #297). Separate from the Go key
  // rotation state: the two pools are different credentials of different providers.
  async parkAccount(index, untilMs) {
    S.parkAccount(await this._load(), index, untilMs);
    await this._save();
  }

  async resetKeys() {
    S.resetKeys(await this._load());
    await this._save();
  }

  async reset() {
    this._state = S.emptyState();
    const list = await this.ctx.storage.list({ prefix: PIN_PREFIX });
    for (const [name] of list) await this.ctx.storage.delete(name);
    await this._save();
  }
}
