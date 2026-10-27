// trained-assist-llm-ladder — OpenAI-compatible chat completions over a model ladder
// (OpenCode Go → paid OpenRouter last) for small service LLM calls across trained-assist repos.
//
//   GET  /health                 liveness + ladder names (no auth)
//   GET  /pool/health            pool receiver liveness, {service:"pool",ok:true} (no auth)
//   POST /pool/trigger           runs-pool dispatch, Bearer POOL_TRIGGER_TOKEN, body ≤ 8 KB (own token)
//   GET  /v1/models              ladders as model ids (auth)
//   GET  /v1/state               model health + key rotation snapshot (auth)
//   POST /v1/state/reset-keys    unpark all Go keys + Go rungs (auth, ops lever)
//   GET  /v1/free-models        free-model inventory: context/price + last availability probe (auth)
//   POST /v1/free-models/collect one collection pass + diff report (auth; the only writer — the Go
//                               catalog key lives in the worker secret and never leaves it)
//   GET  /zen/health             zen-runner liveness: registry size, model count, live caps (no auth)
//   GET  /zen/models             availability table + "call it or skip it" verdict (ZEN_RUNNER_TOKEN)
//   POST /zen/run                {model,runs?} → 202 run_id | 409 quarantine | 429 budget | 502 GitHub
//   POST /zen/report             {run_id?,model,ok,kind?,error?} → the state the ladder reads back
//   POST /zen/repos              registry row (repo + encrypted token or env:NAME), round-robin ring
//   POST /zen/pool/invoke        {model,messages?,prompt?,wait_ms?} → 200 {text} — one long-lived job as an API
//   POST /zen/pool/register      a GitHub Actions job registers itself and gets a lease
//   GET  /zen/pool/pull?lease=…  long-poll for a task; 200 {bye} = exit cleanly
//   POST /zen/pool/result        the answer (text) + the availability verdict
//   POST /zen/pool/stop          tell a job to exit on its next pull
//   GET  /zen/pool/result/{id}   the answer whenever it lands, even after a 504
//   GET  /zen/pool/health        how many jobs are live right now (no auth)
//   POST /v1/chat/completions    body.model = ladder ("service", legacy alias "deepseek", "service:review") (auth)
//
// Auth: `Authorization: Bearer <LADDER_TOKEN>`. Non-streaming → a normal chat.completion whose
// `model` is the rung that answered (also in `x-ladder-model`). stream:true → SSE relayed from
// the chosen rung (chosen before the first token; no failover after it) — how opencode uses the
// `free` model. Tools pass through as is. Optional body fields: ladder_timeout_ms (per
// rung, non-stream), ladder_ttfb_ms (stream: first-token window), ladder_total_timeout_ms,
// ladder_rung (benchmarks: pin one rung of the ladder, no failover).
//
// This file is the Worker entry: it wires the Durable Object binding and dispatches to the route
// in src/handler.js (kept free of the Workerd runtime so plain `node --test` can exercise it).

import { handle } from './handler.js';
import { runMaintenance } from './zen-ring.js';
import { resolveErrorPublisher } from './events.js';
import { FreeModelRefresher } from './free-models.js';

export { LadderState } from './state-do.js';
export { handle, makeStore } from './handler.js';

export default {
  fetch(request, env, ctx) {
    const events = resolveErrorPublisher(env);
    return handle(request, env, { events, waitUntil: ctx?.waitUntil?.bind(ctx) });
  },
  // Every 2 min (wrangler.toml [triggers]): grow the ring if the queue is waiting, bury dead
  // tasks, re-check quarantined models. Bounded by ZEN_SWEEP_MAX and by the same 50/500 budget as
  // an ordinary /zen/run; both halves are idempotent, so a missed or duplicated tick is harmless.
  scheduled(event, env, ctx) {
    ctx.waitUntil(runMaintenance(env));
  },
  // New scheduled task: run free model collection and benchmark every 2 hours
  'scheduled-free-models': function(event, env, ctx) {
    ctx.waitUntil(runFreeModelCollection(env));
  },
  // Additional scheduled task: run free model benchmark every 6 hours
  'scheduled-free-models-benchmark': function(event, env, ctx) {
    ctx.waitUntil(runBenchmarkFreeModels(env));
  }
};

// Free model collection task
async function runFreeModelCollection(env) {
  try {
    const refresher = new FreeModelRefresher({
      env,
      intervalMs: 2 * 60 * 60 * 1000 // 2 hours
    });
    
    await refresher.run();
  } catch (error) {
    console.error('Scheduled free model collection failed:', error);
  }
}

// Free model benchmark task
async function runBenchmarkFreeModels(env) {
  try {
    const refresher = new FreeModelRefresher({ env });
    
    // Check if we have recent benchmark data
    const benchmarkData = await refresher.getTopModelsForBenchmark();
    if (!benchmarkData || benchmarkData.length === 0) {
      console.log('No benchmark data available, skipping benchmark run');
      return;
    }
    
    console.log('Running scheduled free model benchmark...');
    await refresher.benchmarkTopModels();
    
  } catch (error) {
    console.error('Scheduled free model benchmark failed:', error);
  }
}