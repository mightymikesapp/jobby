#!/usr/bin/env node

/**
 * check-liveness.mjs — Playwright job link liveness checker
 *
 * Tests whether job posting URLs are still active or have expired.
 * Zero Claude API tokens — pure Playwright.
 *
 * Usage:
 *   node check-liveness.mjs <url1> [url2] ...
 *   node check-liveness.mjs --file urls.txt
 *
 * Exit code: 0 if all active, 1 if any expired or uncertain
 *
 * Adapted from career-ops. Added legal/government-specific patterns.
 */

import { chromium } from 'playwright';
import { readFile } from 'fs/promises';

const EXPIRED_PATTERNS = [
  // General ATS patterns
  /job (is )?no longer available/i,
  /job.*no longer open/i,
  /position has been filled/i,
  /this job has expired/i,
  /job posting has expired/i,
  /no longer accepting applications/i,
  /this (position|role|job) (is )?no longer/i,
  /this job (listing )?is closed/i,
  /job (listing )?not found/i,
  /the page you are looking for doesn.t exist/i,
  /\d+\s+jobs?\s+found/i,
  /search for jobs page is loaded/i,
  // Government / federal portals
  /application period has closed/i,
  /this announcement has closed/i,
  /this vacancy has been filled/i,
  /announcement.*closed/i,
  // International
  /diese stelle (ist )?(nicht mehr|bereits) besetzt/i,
  /offre (expirée|n'est plus disponible)/i,
];

const EXPIRED_URL_PATTERNS = [
  /[?&]error=true/i,   // Greenhouse redirect on closed jobs
];

const APPLY_PATTERNS = [
  /\bapply\b/i,
  /\bsolicitar\b/i,
  /\bbewerben\b/i,
  /\bpostuler\b/i,
  /submit application/i,
  /easy apply/i,
  /start application/i,
  /ich bewerbe mich/i,
];

const MIN_CONTENT_CHARS = 300;

async function checkUrl(page, url) {
  try {
    const response = await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 15000 });

    const status = response?.status() ?? 0;
    if (status === 404 || status === 410) {
      return { result: 'expired', reason: `HTTP ${status}` };
    }

    // Give SPAs time to hydrate (Ashby/Greenhouse boards can be slow)
    await page.waitForTimeout(3500);

    const finalUrl = page.url();
    for (const pattern of EXPIRED_URL_PATTERNS) {
      if (pattern.test(finalUrl)) {
        return { result: 'expired', reason: `redirect to ${finalUrl}` };
      }
    }

    const bodyText = await page.evaluate(() => document.body?.innerText ?? '');

    // Check EXPIRED signals FIRST — a closed posting often still renders a
    // site-wide "Apply" nav/footer button, which would otherwise read as active.
    for (const pattern of EXPIRED_PATTERNS) {
      if (pattern.test(bodyText)) {
        return { result: 'expired', reason: `pattern matched: ${pattern.source}` };
      }
    }

    // Apply button is the strongest positive signal — trusted only after expired ruled out.
    if (APPLY_PATTERNS.some(p => p.test(bodyText))) {
      return { result: 'active', reason: 'apply button detected' };
    }

    // Thin content usually means the SPA never hydrated — recoverable, so flag as
    // uncertain (was wrongly 'expired', which hid slow live boards like Ashby).
    if (bodyText.trim().length < MIN_CONTENT_CHARS) {
      return { result: 'uncertain', reason: 'insufficient content (SPA may not have hydrated)' };
    }

    return { result: 'uncertain', reason: 'content present but no apply button found' };

  } catch (err) {
    return { result: 'uncertain', reason: `navigation error: ${err.message.split('\n')[0]}` };
  }
}

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0) {
    console.error('Usage: node check-liveness.mjs <url1> [url2] ...');
    console.error('       node check-liveness.mjs --file urls.txt');
    process.exit(1);
  }

  let urls;
  if (args[0] === '--file') {
    if (!args[1]) {
      console.error('Missing path after --file');
      process.exit(1);
    }
    const text = await readFile(args[1], 'utf-8');
    urls = text.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
  } else {
    urls = args;
  }

  console.log(`Checking ${urls.length} URL(s)...\n`);

  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage();

    let active = 0, expired = 0, uncertain = 0;

    // Sequential — never Playwright in parallel
    for (const url of urls) {
      const { result, reason } = await checkUrl(page, url);
      const icon = { active: 'ACTIVE', expired: 'EXPIRED', uncertain: 'UNCERTAIN' }[result];
      console.log(`[${icon}] ${url}`);
      if (result !== 'active') console.log(`         ${reason}`);
      if (result === 'active') active++;
      else if (result === 'expired') expired++;
      else uncertain++;
    }

    console.log(`\nResults: ${active} active  ${expired} expired  ${uncertain} uncertain`);
    if (expired > 0 || uncertain > 0) process.exitCode = 1;
  } finally {
    await browser.close();
  }
}

main().catch(err => {
  console.error('Fatal:', err.message);
  process.exit(1);
});
