#!/usr/bin/env node
// Integration test for context-optimizer: simulates various failure scenarios
// Run: node scripts/test-context-optimizer.mjs

import { optimizeContext } from '../src/context-optimizer.js';

const scenarios = [
  {
    name: 'All map calls succeed',
    attempt: async (env, model, body) => {
      const prompt = body.messages[0].content;
      const chunkId = prompt.match(/turn \d+/)?.[0] || 'unknown';
      return { ok: true, content: `SUMMARY for ${chunkId}: relevant decisions` };
    },
    expect: 'optimized',
    description: '3 chunks, all return summaries → should optimize'
  },
  {
    name: '1 of 3 map calls fails (HTTP 500)',
    attempt: async (env, model, body, keyIndex, opts, callCount = { n: 0 }) => {
      callCount.n++;
      if (callCount.n === 2) return { ok: false, error: 'HTTP 500: internal error' };
      return { ok: true, content: `SUMMARY ${callCount.n}: relevant` };
    },
    expect: 'optimized_with_gap',
    description: 'partial failure → still optimizes with 2 summaries'
  },
  {
    name: 'All map calls fail (timeouts)',
    attempt: async () => ({ ok: false, error: 'fetch failed: timeout' }),
    expect: 'fail_open',
    description: 'all fail → returns original body unchanged'
  },
  {
    name: 'Map call throws exception',
    attempt: async () => { throw new Error('network crashed'); },
    expect: 'fail_open',
    description: 'exception in attempt → fail-open'
  },
  {
    name: 'Some IRRELEVANT, some summaries',
    attempt: async (env, model, body, keyIndex, opts, callCount = { n: 0 }) => {
      callCount.n++;
      if (callCount.n === 1) return { ok: true, content: 'IRRELEVANT' };
      return { ok: true, content: `SUMMARY ${callCount.n}: relevant info` };
    },
    expect: 'optimized_filtered',
    description: 'IRRELEVANT chunks excluded, relevant summaries kept'
  },
  {
    name: 'All IRRELEVANT',
    attempt: async () => ({ ok: true, content: 'IRRELEVANT' }),
    expect: 'fail_open',
    description: 'no relevant chunks → fail-open'
  },
  {
    name: 'Empty content from map',
    attempt: async () => ({ ok: true, content: '' }),
    expect: 'fail_open',
    description: 'empty summaries → fail-open'
  },
  {
    name: 'Mixed: throw + ok:false + ok:true',
    attempt: async (env, model, body, keyIndex, opts, callCount = { n: 0 }) => {
      callCount.n++;
      if (callCount.n === 1) throw new Error('reset');
      if (callCount.n === 2) return { ok: false, error: 'HTTP 429' };
      return { ok: true, content: 'SUMMARY 3: only survivor' };
    },
    expect: 'optimized_partial',
    description: '1 throw + 1 error + 1 success → optimizes with 1 summary'
  }
];

function buildBody(numChunks = 3) {
  const messages = [{ role: 'system', content: 'You are helpful.' }];
  for (let c = 0; c < numChunks; c++) {
    for (let i = 0; i < 5; i++) {
      messages.push({ role: 'assistant', content: `chunk${c} turn${i}: ${'x'.repeat(120)}` });
    }
  }
  messages.push({ role: 'user', content: 'the actual question' });
  return { model: 'service', messages };
}

let pass = 0;
let fail = 0;

for (const scenario of scenarios) {
  const body = buildBody(3);
  const callCount = { n: 0 };
  const attempt = (env, model, bodyArg, keyIndex, opts) => scenario.attempt(env, model, bodyArg, keyIndex, opts, callCount);

  try {
    const result = await optimizeContext(
      body, {}, async () => {}, 'opencode-go/beam-free', attempt, 100
    );

    let verdict;
    if (scenario.expect === 'fail_open') {
      verdict = result === body ? 'PASS' : 'FAIL (should be original body)';
    } else if (result === body) {
      verdict = 'FAIL (should be optimized)';
    } else {
      const summaryMsg = result.messages?.find(m => m.content?.includes('Relevant context summaries'));
      if (!summaryMsg) {
        verdict = 'FAIL (no summaries message)';
      } else {
        const summaryCount = (summaryMsg.content.match(/SUMMARY/g) || []).length;
        verdict = `PASS (optimized, ${summaryCount} summaries, ${callCount.n} map calls)`;
      }
    }

    if (verdict.startsWith('PASS')) {
      pass++;
      console.log(`✅ ${scenario.name}: ${verdict}`);
      console.log(`   ${scenario.description}`);
    } else {
      fail++;
      console.log(`❌ ${scenario.name}: ${verdict}`);
      console.log(`   ${scenario.description}`);
    }
  } catch (e) {
    fail++;
    console.log(`❌ ${scenario.name}: THREW ${e.message}`);
  }
}

console.log(`\n${'='.repeat(60)}`);
console.log(`Results: ${pass} passed, ${fail} failed`);
process.exit(fail > 0 ? 1 : 0);
