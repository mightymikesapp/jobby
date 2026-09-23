#!/usr/bin/env node

/**
 * dedup-tracker.mjs — Remove duplicate entries from data/pipeline.md
 *
 * Groups entries by normalized company name, then fuzzy-matches roles.
 * Keeps the entry with the highest score. Preserves the most advanced status.
 *
 * Usage:
 *   node dedup-tracker.mjs [--dry-run]
 *
 * Adapted from career-ops dedup-tracker.mjs for legal job search context.
 */

import { open, readFile, rename, unlink } from 'fs/promises';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PIPELINE_FILE = resolve(__dirname, 'data/pipeline.md');

// Write via a same-directory temp file + rename so a crash mid-write can never
// leave the tracker truncated.
async function atomicWrite(path, content) {
  const tmp = `${path}.tmp-${process.pid}`;
  const handle = await open(tmp, 'w', 0o600);
  try {
    await handle.writeFile(content);
    await handle.sync();
  } finally {
    await handle.close();
  }
  try {
    await rename(tmp, path);
  } catch (err) {
    await unlink(tmp).catch(() => {});
    throw err;
  }
}

// Status advancement order (higher = more advanced)
const STATUS_RANK = {
  'skip': 0,
  'evaluated': 1,
  'applied': 2,
  'responded': 3,
  'interview': 4,
  'offer': 5,
  'rejected': 3, // Same as responded (they replied)
  'closed': 0,
};

const COMPANY_ALIASES = new Map([
  ['ferc', 'ferc'],
  ['federalenergyregulatorycommission', 'ferc'],
  ['wmg', 'warnermusicgroup'],
  ['warnermusicgroup', 'warnermusicgroup'],
  ['uscopyrightoffice', 'uscopyrightoffice'],
  ['unitedstatescopyrightoffice', 'uscopyrightoffice'],
  ['waltdisney', 'disney'],
  ['disney', 'disney'],
]);

function normalizeCompany(name) {
  const key = name
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/\b(the|incorporated|inc|llc|ltd|corp|corporation|company|co)\b/g, ' ')
    .replace(/[^a-z0-9]/g, '');
  return COMPANY_ALIASES.get(key) ?? key;
}

function normalizeRole(s) {
  return s.toLowerCase().replace(/[^a-z0-9\s]/g, ' ').replace(/\s+/g, ' ').trim();
}

// True ONLY for genuine duplicates: exact normalized match, or >=80% token
// overlap (Jaccard). The old ">=2 shared words" rule merged DIFFERENT roles —
// e.g. "Legal Engineer Associate" vs "Legal Engineer - Strategic Programs" —
// and deleted one of them.
function fuzzyRoleMatch(a, b) {
  const na = normalizeRole(a), nb = normalizeRole(b);
  if (na === nb) return true;
  const setA = new Set(na.split(' ').filter(w => w.length > 2));
  const setB = new Set(nb.split(' ').filter(w => w.length > 2));
  if (setA.size === 0 || setB.size === 0) return false;
  let inter = 0;
  for (const w of setA) if (setB.has(w)) inter++;
  const union = new Set([...setA, ...setB]).size;
  return inter / union >= 0.8;
}

function parseScore(scoreStr) {
  // Parse the LEADING number only. "[3.5/5]" -> 3.5, but a placeholder like
  // "[—/5]" must NOT match the "5" in the out-of-5 suffix (that made unscored
  // rows parse as 5, win merges, and sort to the top).
  const match = scoreStr.match(/^\s*([\d.]+)/);
  return match ? parseFloat(match[1]) : -1;  // sentinel: unscored sorts last, never wins a merge
}

function statusRank(status) {
  return STATUS_RANK[status.toLowerCase()] ?? 1;
}

function mergeInto(keep, dup) {
  // keep = entry already in the kept list; dup = the duplicate being merged in.
  const winner = dup.scoreNum > keep.scoreNum ? dup : keep;
  const other  = winner === keep ? dup : keep;
  const status = statusRank(dup.status) >= statusRank(keep.status) ? dup.status : keep.status;
  const hasReport = s => /\[report\]/i.test(s) || /reports\//i.test(s);
  // Never lose a report link: if the winner's tail has none but the other does, keep the other's.
  let rest = winner.rest;
  if (!hasReport(rest) && hasReport(other.rest)) rest = other.rest;
  keep.score = winner.score;
  keep.scoreNum = winner.scoreNum;
  keep.role = winner.role;
  keep.company = winner.company;
  keep.status = status;
  keep.rest = rest;
  keep.raw = `- [${winner.score}] ${winner.company} — ${winner.role} | ${status} | ${rest}`;
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  const content = await readFile(PIPELINE_FILE, 'utf-8');
  const lines = content.split('\n');

  // Tag every line, preserving original order and all non-entry lines (section
  // headers, blank lines) so the document structure is never flattened.
  const tagged = lines.map((line) => {
    const match = line.match(/^-\s+\[(.+?)\]\s+(.+?)\s+[—-]+\s+(.+?)\s+\|\s+(.+?)\s+\|\s+(.+)/);
    if (match) {
      return {
        type: 'entry',
        score: match[1], company: match[2].trim(), role: match[3].trim(),
        status: match[4].trim(), rest: match[5].trim(), raw: line,
        scoreNum: parseScore(match[1]), dropped: false,
      };
    }
    return { type: 'other', text: line };
  });

  const entries = tagged.filter(t => t.type === 'entry');
  if (entries.length === 0) {
    console.log('No entries found in pipeline.');
    return;
  }

  // Dedup in place: each entry merges into the first earlier KEPT entry that is
  // the same company AND a genuine role duplicate.
  const kept = [];
  let removedCount = 0;
  for (const entry of entries) {
    const target = kept.find(k =>
      normalizeCompany(k.company) === normalizeCompany(entry.company) &&
      fuzzyRoleMatch(k.role, entry.role)
    );
    if (target) {
      mergeInto(target, entry);
      entry.dropped = true;
      removedCount++;
      console.log(`  DEDUP: "${entry.company} — ${entry.role}" (score ${entry.score}) merged into "${target.role}"`);
    } else {
      kept.push(entry);
    }
  }

  if (removedCount === 0) {
    console.log(`No duplicates found among ${entries.length} entries.`);
    return;
  }

  console.log(`\nMerged ${removedCount} duplicate(s). ${kept.length} entries remain.`);

  if (dryRun) {
    console.log('(dry run — no changes made)');
    return;
  }

  // Rebuild in original order: updated raw for kept entries, skip dropped ones,
  // pass through every other line untouched.
  const out = [];
  for (const t of tagged) {
    if (t.type === 'other') out.push(t.text);
    else if (!t.dropped) out.push(t.raw);
  }
  await atomicWrite(PIPELINE_FILE, out.join('\n'));

  console.log(`Updated data/pipeline.md`);
}

main().catch(err => {
  console.error('Dedup failed:', err.message);
  process.exit(1);
});
