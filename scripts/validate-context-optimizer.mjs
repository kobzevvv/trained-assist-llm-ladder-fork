#!/usr/bin/env node
// validate-context-optimizer.mjs — replay the context optimizer over real
// historical fat sessions WITHOUT live LLM calls (deterministic mock map).
//
// Prints only sizes, chunk/summary counts and compression ratios — never the
// content of the private session prompts (same rule as fat-session-bench.mjs).
//
// Usage:
//   node scripts/validate-context-optimizer.mjs            # threshold 80000
//   node scripts/validate-context-optimizer.mjs --limit 20000
//   node scripts/validate-context-optimizer.mjs --fail every    # all maps fail
//   node scripts/validate-context-optimizer.mjs --fail every2   # every 2nd fails
//
// Exit code 0 = no crash (fail-open holds). Failures are EXPECTED for the
// --fail modes; the script just proves the schema does not break.

import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import zlib from 'node:zlib';
import { optimizeContext, getLastUserQuestion } from '../src/context-optimizer.js';
import { estimateInputTokens } from '../src/size-policy.js';

const arg = (name, def) => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] && !process.argv[i + 1].startsWith('--') ? process.argv[i + 1] : def;
};
const LIMIT = Number(arg('limit', 80000));
const FAIL = arg('fail', 'none'); // none | every | every2
const CACHE = arg('cache', path.join(os.homedir(), '.cache/llm-ladder-fat-bench/mbk_luda_recruiter/sessions'));

function loadSessions() {
  const out = [];
  for (const f of fs.readdirSync(CACHE)) {
    if (!f.endsWith('.json.gz') && !f.endsWith('.json')) continue;
    const p = path.join(CACHE, f);
    try {
      const raw = f.endsWith('.gz') ? zlib.gunzipSync(fs.readFileSync(p)) : fs.readFileSync(p);
      const d = JSON.parse(raw.toString('utf8'));
      if (Array.isArray(d?.messages)) out.push({ id: d.id || f, messages: d.messages });
    } catch { /* skip corrupt */ }
  }
  return out;
}

function mockAttempt(state, env, rung, body, keyIndex, opts) {
  state.n++;
  const prompt = body.messages[0].content;
  if (FAIL === 'every') return { ok: false, error: 'simulated outage' };
  if (FAIL === 'every2' && state.n % 2 === 0) return { ok: false, error: 'simulated 500' };
  // Deterministic "relevant" summary: take the first text of the chunk.
  const start = prompt.indexOf('<<<CHUNK');
  const chunk = prompt.slice(start);
  const text = chunk.replace(/[\[\]{}"]/g, ' ').replace(/\s+/g, ' ').trim();
  const snippet = text.slice(0, 120);
  return { ok: true, content: `SUMMARY: ${snippet}` };
}

async function run() {
  const sessions = loadSessions();
  if (!sessions.length) { console.log('сессий нет'); return; }

  const rows = [];
  for (const s of sessions) {
    const inTok = estimateInputTokens({ messages: s.messages });
    const nMsg = s.messages.length;
    const lastUser = getLastUserQuestion(s.messages);
    if (inTok <= LIMIT) continue; // under threshold → optimizer would not fire
    const body = { model: 'service', messages: s.messages };
    const state = { n: 0 };
    let out;
    let err = null;
    try {
      out = await optimizeContext(body, {}, async () => {}, 'opencode-go/beam-free', (e, r, b, k, o) => mockAttempt(state, e, r, b, k, o), LIMIT);
    } catch (e) { err = e.message; }
    const optimized = out !== body;
    const outTok = optimized ? estimateInputTokens(out) : inTok;
    rows.push({
      id: s.id.slice(0, 8),
      inTok, outTok,
      nMsg,
      chunks: state.n,
      summaries: optimized ? (out.messages.find(m => typeof m.content === 'string' && m.content.includes('Relevant context summaries'))?.content.match(/SUMMARY/g)?.length || 0) : 0,
      ratio: optimized ? (inTok / Math.max(1, outTok)).toFixed(1) : '—',
      optimized,
      err,
      lastUserKept: optimized ? out.messages.some(m => m.role === 'user') : null,
    });
  }

  if (!rows.length) {
    console.log(`ни одна сессия не превышает порог ${LIMIT} токенов — оптимизатор не сработал бы`);
    return;
  }

  rows.sort((a, b) => b.inTok - a.inTok);
  const w = (v, n) => String(v).padStart(n);
  console.log(`\nпорог ${LIMIT}t · режим отказа: ${FAIL} · сессий выше порога: ${rows.length}\n`);
  console.log(`  ${'id'.padEnd(9)} ${w('in.tok', 7)} ${w('out.tok', 7)} ${w('msg', 5)} ${w('chunks', 6)} ${w('summ', 5)} ${w('x', 5)}  вопрос`);
  for (const r of rows) {
    const q = r.optimized ? (r.lastUserKept ? 'сохранён' : 'ПОТЕРЯН') : (r.err ? `ОШИБКА:${r.err}` : 'без изм.');
    console.log(`  ${r.id.padEnd(9)} ${w(r.inTok, 7)} ${w(r.outTok, 7)} ${w(r.nMsg, 5)} ${w(r.chunks, 6)} ${w(r.summaries, 5)} ${w(r.ratio, 5)}  ${q}`);
  }

  const optim = rows.filter(r => r.optimized);
  const avgRatio = optim.length ? (optim.reduce((a, r) => a + Number(r.ratio), 0) / optim.length).toFixed(1) : 0;
  console.log(`\n  оптимизировано: ${optim.length}/${rows.length} · среднее сжатие: ×${avgRatio}`);
  console.log(`  среднее чанков на сессию: ${(rows.reduce((a, r) => a + r.chunks, 0) / rows.length).toFixed(1)}`);
  const lostQ = rows.filter(r => r.optimized && r.lastUserKept === false).length;
  if (lostQ) console.log(`  ⚠ ВОПРОС ПОТЕРЯН в ${lostQ} сессиях`);
  else console.log(`  вопрос сохранён во всех оптимизированных сессиях`);
}

run();
