#!/usr/bin/env node

/**
 * merge-tracker.mjs — Merge evaluation reports into the pipeline tracker
 *
 * Scans reports/ directory for evaluation reports and ensures each one
 * has an entry in data/pipeline.md. Deduplicates by company + role.
 *
 * Usage:
 *   node merge-tracker.mjs [--dry-run]
 *
 * Adapted from career-ops merge-tracker.mjs for legal job search context.
 */

import { readdir, readFile, writeFile } from 'fs/promises';
import { resolve, basename } from 'path';
import { fileURLToPath } from 'url';
import { dirname } from 'path';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS_DIR = resolve(__dirname, 'reports');
const PIPELINE_FILE = resolve(__dirname, 'data/pipeline.md');

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
// overlap (Jaccard). The old ">=2 shared words" rule treated DIFFERENT roles as
// dupes and silently refused to add the new report to the pipeline.
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

async function parseReport(filepath) {
  const content = await readFile(filepath, 'utf-8');
  const lines = content.split('\n');

  let company = '', role = '', score = '', url = '', barRequired = '', expGate = '', preBar = '';

  // Parse header: "# Evaluation: {Company} — {Role}"
  const titleMatch = content.match(/^#\s+Evaluation:\s+(.+?)\s+[—-]+\s+(.+)/m);
  if (titleMatch) {
    company = titleMatch[1].trim();
    role = titleMatch[2].trim();
  }

  for (const line of lines) {
    if (line.startsWith('**Score:**')) score = line.replace('**Score:**', '').trim();
    if (line.startsWith('**URL:**')) url = line.replace('**URL:**', '').trim();
    if (line.startsWith('**Bar Required:**')) barRequired = line.replace('**Bar Required:**', '').trim();
    if (line.startsWith('**Experience Gate:**')) expGate = line.replace('**Experience Gate:**', '').trim();
    if (line.startsWith('**Pre-bar Eligible:**')) preBar = line.replace('**Pre-bar Eligible:**', '').trim();
  }

  return { company, role, score, url, barRequired, expGate, preBar, file: basename(filepath) };
}

async function parsePipeline() {
  try {
    const content = await readFile(PIPELINE_FILE, 'utf-8');
    const entries = [];
    for (const line of content.split('\n')) {
      const match = line.match(/^-\s+\[(.+?)\]\s+(.+?)\s+[—-]+\s+(.+?)\s+\|\s+(.+?)\s+\|\s+\[Report\]\((.+?)\)/);
      if (match) {
        const reportLink = match[5].trim();
        entries.push({
          score: match[1],
          company: match[2].trim(),
          role: match[3].trim(),
          status: match[4].trim(),
          reportLink,
          reportFile: basename(reportLink),
          raw: line,
        });
      }
    }
    return entries;
  } catch {
    return [];
  }
}

async function main() {
  const dryRun = process.argv.includes('--dry-run');

  // Read all reports
  let reportFiles;
  try {
    reportFiles = (await readdir(REPORTS_DIR)).filter(f => f.endsWith('.md')).sort();
  } catch {
    console.log('No reports/ directory found.');
    return;
  }

  if (reportFiles.length === 0) {
    console.log('No reports found.');
    return;
  }

  // Parse reports and existing pipeline
  const reports = await Promise.all(reportFiles.map(f => parseReport(resolve(REPORTS_DIR, f))));
  const existing = await parsePipeline();

  // Find new reports not in pipeline
  const newEntries = [];
  const skipped = [];
  for (const report of reports) {
    if (!report.company || !report.role) continue;

    const dup = existing.find(e =>
      e.reportFile === report.file ||
      (
        normalizeCompany(e.company) === normalizeCompany(report.company) &&
        fuzzyRoleMatch(e.role, report.role)
      )
    );

    if (dup) {
      skipped.push({ report, dup });
    } else {
      newEntries.push(report);
    }
  }

  // Surface duplicates rather than silently dropping them — a real false-match
  // here means a second same-company report would never appear in the pipeline.
  if (skipped.length) {
    console.log(`Skipped ${skipped.length} report(s) already tracked (not dropped):`);
    for (const { report, dup } of skipped) {
      console.log(`  ${report.file}: "${report.company} — ${report.role}"  ≈  "${dup.company} — ${dup.role}"`);
    }
  }

  if (newEntries.length === 0) {
    console.log(`All ${reports.length} reports already in pipeline. Nothing to merge.`);
    return;
  }

  console.log(`Found ${newEntries.length} new report(s) to add:`);
  for (const entry of newEntries) {
    const line = `- [${entry.score}] ${entry.company} — ${entry.role} | Evaluated | [Report](reports/${entry.file})`;
    console.log(`  ${line}`);
  }

  if (dryRun) {
    console.log('\n(dry run — no changes made)');
    return;
  }

  // Append to pipeline
  let pipelineContent;
  try {
    pipelineContent = await readFile(PIPELINE_FILE, 'utf-8');
  } catch {
    pipelineContent = '# Evaluation Pipeline\n\n';
  }

  const newLines = newEntries.map(e =>
    `- [${e.score}] ${e.company} — ${e.role} | Evaluated | [Report](reports/${e.file})`
  );

  pipelineContent = pipelineContent.trimEnd() + '\n' + newLines.join('\n') + '\n';
  await writeFile(PIPELINE_FILE, pipelineContent);

  console.log(`\nMerged ${newEntries.length} entries into data/pipeline.md`);
}

main().catch(err => {
  console.error('Merge failed:', err.message);
  process.exit(1);
});
