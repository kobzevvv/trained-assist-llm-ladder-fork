import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

// Guard against the #335 class of bug at the CI layer: a migration file exists
// but is never applied during deploy. Every schema/migrations/*.sql must be
// referenced by .github/workflows/ci.yml.
test('every migration file is applied in CI', () => {
  const migrations = fs.readdirSync('schema/migrations')
    .filter((f) => f.endsWith('.sql'))
    .sort();
  const ci = fs.readFileSync('.github/workflows/ci.yml', 'utf8');
  const missing = migrations.filter((f) => !ci.includes(f));
  assert.deepEqual(missing, [], `migrations not referenced in ci.yml: ${missing.join(', ')}`);
});
