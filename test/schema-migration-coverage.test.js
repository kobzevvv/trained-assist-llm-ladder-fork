import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';

// Guard against the #335 class of bug: code references a D1 table that has no
// migration (or whose migration is not applied). Every underscored table name
// that appears in a SQL template literal in src/ must be declared by some
// schema/*.sql or schema/migrations/*.sql file.
test('every table referenced in src/ SQL is declared in schema', () => {
  const sqlFiles = [];
  for (const dir of ['schema', 'schema/migrations']) {
    for (const f of fs.readdirSync(dir)) {
      if (f.endsWith('.sql')) sqlFiles.push(path.join(dir, f));
    }
  }
  const tables = new Set();
  for (const f of sqlFiles) {
    const txt = fs.readFileSync(f, 'utf8');
    for (const m of txt.matchAll(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["'`]?(\w+)/gi)) {
      tables.add(m[1].toLowerCase());
    }
  }

  const refs = new Set();
  const srcFiles = fs.readdirSync('src').filter((f) => f.endsWith('.js'));
  for (const f of srcFiles) {
    const txt = fs.readFileSync(path.join('src', f), 'utf8');
    // Table names held in *_TABLE constants (e.g. FREE_MODELS_TABLE = 'free_models').
    for (const m of txt.matchAll(/_TABLE\s*=\s*['"](\w+)['"]/g)) {
      refs.add(m[1].toLowerCase());
    }
    for (const m of txt.matchAll(/`([^`]*)`/g)) {
      const sql = m[1];
      if (!/\b(SELECT|INSERT|UPDATE|DELETE|FROM|JOIN|TABLE)\b/i.test(sql)) continue;
      for (const t of sql.matchAll(/\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+["'`]?(\w+)/gi)) {
        refs.add(t[1].toLowerCase());
      }
    }
  }

  const missing = [...refs]
    .filter((r) => r.includes('_') && !tables.has(r))
    .sort();
  assert.deepEqual(missing, [], `tables referenced in src/ but missing from schema: ${missing.join(', ')}`);
});
