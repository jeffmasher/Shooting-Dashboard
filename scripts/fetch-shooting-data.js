/**
 * fetch-shooting-data.js
 * Runs via GitHub Actions to fetch blocked city data server-side
 * and write results to data/manual-auto.json
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

// ─── Helpers ──────────────────────────────────────────────────────────────────

function fetchUrl(targetUrl, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const parsed = new URL(targetUrl);
    const lib = parsed.protocol === 'https:' ? https : http;
    const options = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === 'https:' ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method: 'GET',
      headers: { 'User-Agent': 'Mozilla/5.0 (compatible; ShootingDashboard/1.0)' },
      timeout: timeoutMs,
    };
    const req = lib.request(options, (res) => {
      if ([301,302,303,307,308].includes(res.statusCode) && res.headers.location) {
        const redirect = res.headers.location.startsWith('http')
          ? res.headers.location
          : parsed.origin + res.headers.location;
        return fetchUrl(redirect, timeoutMs).then(resolve).catch(reject);
      }
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks) }));
      res.on('error', reject);
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('Timeout')); });
    req.on('error', reject);
    req.end();
  });
}

// ─── PDF parsing ──────────────────────────────────────────────────────────────

async function extractPdfTokens(buffer) {
  // pdfjs-dist is installed at repo root (node_modules/)
  const pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js');
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(1);
  const tc = await page.getTextContent();
  return tc.items.map(i => i.str.trim()).filter(s => s.length > 0);
}

// ─── Detroit ──────────────────────────────────────────────────────────────────

async function fetchDetroit() {
  // Find most recent Thursday
  const d = new Date();
  const day = d.getDay();
  const daysBack = day >= 4 ? day - 4 : day + 3;
  d.setDate(d.getDate() - daysBack);
  const yyyy = d.getFullYear();
  const mm   = String(d.getMonth()+1).padStart(2,'0');
  const dd   = String(d.getDate()).padStart(2,'0');
  const yy   = String(yyyy).slice(2);
  const pdfUrl = `https://detroitmi.gov/sites/detroitmi.localhost/files/events/${yyyy}-${mm}/${yy}${mm}${dd}%20DPD%20Stats.pdf`;

  console.log('Detroit PDF URL:', pdfUrl);
  const resp = await fetchUrl(pdfUrl);
  if (resp.status !== 200) throw new Error(`Detroit PDF HTTP ${resp.status}`);

  const tokens = await extractPdfTokens(resp.body);
  const text = tokens.join(' ');

  // Date
  const dateMatch = text.match(/\w+day,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
    const mo = months[dateMatch[1].toLowerCase()];
    asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;
  }

  // Find Non-Fatal Shooting row
  let nfsIdx = -1;
  for (let i = 0; i < tokens.length; i++) {
    if (tokens[i].match(/^Non.?Fatal$/i) && tokens[i+1]?.match(/^Shooting/i)) { nfsIdx = i; break; }
    if (tokens[i].match(/^Non.?Fatal\s+Shooting/i)) { nfsIdx = i; break; }
  }
  if (nfsIdx === -1) throw new Error('Non-Fatal Shooting row not found. Tokens: ' + tokens.slice(0,60).join('|'));

  const nums = [];
  for (let j = nfsIdx+1; j < tokens.length && nums.length < 5; j++) {
    if (/^-?[\d,]+$/.test(tokens[j])) nums.push(parseInt(tokens[j].replace(/,/g,'')));
  }
  if (nums.length < 4) throw new Error(`Not enough numbers: ${nums.join(',')}`);

  // [priorDay, prior7Days, ytd_current, ytd_prior]
  return { ytd: nums[2], prior: nums[3], asof };
}

// ─── Durham ───────────────────────────────────────────────────────────────────

async function fetchDurham() {
  const archiveUrl = 'https://www.durhamnc.gov/Archive.aspx?AMID=211';
  console.log('Durham archive URL:', archiveUrl);
  const archResp = await fetchUrl(archiveUrl);
  if (archResp.status !== 200) throw new Error(`Durham archive HTTP ${archResp.status}`);

  const html = archResp.body.toString('utf8');
  const adidMatches = [...html.matchAll(/ADID=(\d+)/g)].map(m => parseInt(m[1]));
  if (!adidMatches.length) throw new Error('No ADID links found');
  const latestAdid = Math.max(...adidMatches);
  const pdfUrl = `https://www.durhamnc.gov/ArchiveCenter/ViewFile/Item/${latestAdid}`;

  console.log('Durham PDF URL:', pdfUrl, '(ADID:', latestAdid + ')');
  const pdfResp = await fetchUrl(pdfUrl);
  if (pdfResp.status !== 200) throw new Error(`Durham PDF HTTP ${pdfResp.status}`);

  const tokens = await extractPdfTokens(pdfResp.body);
  const text = tokens.join(' ');

  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  if (!dateMatch) throw new Error('Date not found. Tokens: ' + tokens.slice(0,30).join('|'));
  const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
  const mo = months[dateMatch[1].toLowerCase()];
  const asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;

  const fatalIdx    = tokens.findIndex(t => t.match(/^Fatal$/i));
  const nonfatalIdx = tokens.findIndex((t,i) => t.match(/^Non.?Fatal$/i) && i > fatalIdx);

  function grab3(startIdx) {
    const nums = [];
    for (let i = startIdx+1; i < tokens.length && nums.length < 3; i++) {
      if (/^\d+$/.test(tokens[i])) nums.push(parseInt(tokens[i]));
    }
    return nums;
  }

  const fatal3    = fatalIdx    !== -1 ? grab3(fatalIdx)    : [];
  const nonfatal3 = nonfatalIdx !== -1 ? grab3(nonfatalIdx) : [];

  let ytd, prior;
  if (fatal3.length === 3 && nonfatal3.length === 3) {
    ytd   = fatal3[2] + nonfatal3[2];
    prior = fatal3[1] + nonfatal3[1];
  } else {
    const allNums = tokens.filter(t => /^\d+$/.test(t) && parseInt(t) < 500).map(Number);
    if (allNums.length < 12) throw new Error('Not enough numbers: ' + allNums.join(','));
    ytd   = allNums[8]  + allNums[11];
    prior = allNums[7]  + allNums[10];
  }

  return { ytd, prior, asof, adid: latestAdid };
}

// ─── Milwaukee (Tableau) ──────────────────────────────────────────────────────

async function fetchMilwaukee() {
  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();

  console.log('Milwaukee: loading Tableau dashboard...');
  await page.goto(
    'https://public.tableau.com/views/MilwaukeePoliceDepartment-PartICrimes/MPDPublicCrimeDashboard',
    { waitUntil: 'networkidle', timeout: 60000 }
  );

  // Wait for the table to render — look for "Non-Fatal" text in the viz
  await page.waitForFunction(() => {
    const frames = Array.from(document.querySelectorAll('iframe'));
    for (const f of frames) {
      try {
        const text = f.contentDocument?.body?.innerText || '';
        if (text.includes('Non-Fatal')) return true;
      } catch(e) {}
    }
    // Also check top-level
    return document.body.innerText.includes('Non-Fatal');
  }, { timeout: 30000 });

  // Extract the as-of date from "Data Current Through: M/D/YYYY"
  const fullText = await page.evaluate(() => document.body.innerText);

  const dateMatch = fullText.match(/Data Current Through[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }

  // Find the Non-Fatal Shooting row and grab YTD 2026 and YTD 2025
  // The table columns are: OFFENSE | Full Year 2024 | Full Year 2025 | %chg | YTD 2024 | YTD 2025 | YTD 2026 | ...
  const lines = fullText.split('\n').map(l => l.trim()).filter(Boolean);
  let ytd = null, prior = null;

  for (let i = 0; i < lines.length; i++) {
    if (lines[i].match(/Non-Fatal\s+Shooting/i) || (lines[i].match(/Non-Fatal/i) && lines[i+1]?.match(/^Shooting/i))) {
      // Numbers follow: full2024, full2025, %chg, ytd2024, ytd2025, ytd2026, ...
      const nums = [];
      const start = lines[i].match(/Non-Fatal/i) && lines[i+1]?.match(/^Shooting/i) ? i+2 : i+1;
      for (let j = start; j < Math.min(start+15, lines.length) && nums.length < 7; j++) {
        const m = lines[j].match(/^-?[\d,]+$/);
        if (m) nums.push(parseInt(lines[j].replace(/,/g,'')));
        else if (lines[j].match(/^-?\d+%$/)) continue; // skip % change cells
      }
      console.log('Milwaukee nums:', nums);
      if (nums.length >= 6) {
        // [full2024, full2025, ytd2024, ytd2025, ytd2026, ...]
        ytd   = nums[5]; // YTD 2026
        prior = nums[4]; // YTD 2025
      }
      break;
    }
  }

  await browser.close();

  if (ytd === null) throw new Error('Could not find Non-Fatal Shooting YTD values.\nPage text sample: ' + lines.slice(0,40).join(' | '));

  return { ytd, prior, asof };
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  const results = {};
  const fetchedAt = new Date().toISOString();

  // Detroit
  try {
    console.log('\n--- Fetching Detroit ---');
    results.detroit = { ...(await fetchDetroit()), fetchedAt, ok: true };
    console.log('Detroit:', results.detroit);
  } catch (e) {
    console.error('Detroit error:', e.message);
    results.detroit = { ok: false, error: e.message, fetchedAt };
  }

  // Durham
  try {
    console.log('\n--- Fetching Durham ---');
    results.durham = { ...(await fetchDurham()), fetchedAt, ok: true };
    console.log('Durham:', results.durham);
  } catch (e) {
    console.error('Durham error:', e.message);
    results.durham = { ok: false, error: e.message, fetchedAt };
  }

  // Milwaukee
  try {
    console.log('\n--- Fetching Milwaukee ---');
    results.milwaukee = { ...(await fetchMilwaukee()), fetchedAt, ok: true };
    console.log('Milwaukee:', results.milwaukee);
  } catch (e) {
    console.error('Milwaukee error:', e.message);
    results.milwaukee = { ok: false, error: e.message, fetchedAt };
  }

  // Write output
  const outDir = path.join(__dirname, '..', 'data');
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, 'manual-auto.json');
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log('\nWrote', outPath);
  console.log(JSON.stringify(results, null, 2));
}

main().catch(e => { console.error(e); process.exit(1); });
