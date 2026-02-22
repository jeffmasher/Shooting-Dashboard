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

async function extractPdfTokens(buffer, pageNum = 1) {
  // pdfjs-dist is installed at repo root (node_modules/)
  let pdfjsLib;
  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }
  catch(e) { pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(pageNum);
  const tc = await page.getTextContent();
  const raw = tc.items.map(i => i.str).filter(s => s.length > 0);

  // Collapse runs of single characters caused by custom font encoding
  // e.g. ['N','o','n','-','F','a','t','a','l'] -> ['Non-Fatal']
  const merged = [];
  let run = '';
  for (const tok of raw) {
    if (tok.length === 1 && tok.trim().length > 0) {
      run += tok;
    } else {
      if (run.length > 0) { merged.push(run.trim()); run = ''; }
      const t = tok.trim();
      if (t.length > 0) merged.push(t);
    }
  }
  if (run.length > 0) merged.push(run.trim());

  // Further pass: re-split merged tokens on whitespace in case multiple words merged
  const tokens = [];
  for (const t of merged) {
    const parts = t.split(/\s+/).filter(p => p.length > 0);
    tokens.push(...parts);
  }
  return tokens;
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

  // Join all tokens and search for Non-Fatal Shooting row
  // Tokens may be partially merged so search the joined string
  const joined = tokens.join(' ');
  const nfsMatch = joined.match(/Non.?Fatal\s*Shooting[\s\S]*?(?=\w+Homicide|\w+Sex|\w+Assault|\w+Robbery|\w+Burglary|$)/i);
  if (!nfsMatch) throw new Error('Non-Fatal Shooting row not found. Tokens: ' + tokens.slice(0,60).join('|'));

  // Extract all numbers from the matched section
  const nums = [];
  const numMatches = nfsMatch[0].matchAll(/-?[\d,]+(?:\.\d+)?/g);
  for (const m of numMatches) {
    const n = parseFloat(m[0].replace(/,/g, ''));
    if (!isNaN(n) && Number.isInteger(n)) nums.push(n);
  }
  if (nums.length < 4) throw new Error(`Not enough numbers after Non-Fatal Shooting: ${nums.join(',')}`);

  // Layout: priorDay, prior7Days, ytd_current, ytd_prior
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

  // Try page 1 for the date, then scan all pages for the data table
  let pdfjsLib;
  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }
  catch(e) { pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfResp.body) }).promise;
  const numPages = pdf.numPages;
  console.log('Durham PDF pages:', numPages);

  // Collect tokens from all pages
  let allTokens = [];
  for (let p = 1; p <= numPages; p++) {
    const pg = await pdf.getPage(p);
    const tc = await pg.getTextContent();
    const raw = tc.items.map(i => i.str).filter(s => s.length > 0);
    const merged = [];
    let run = '';
    for (const tok of raw) {
      if (tok.length === 1 && tok.trim().length > 0) { run += tok; }
      else { if (run.length > 0) { merged.push(run.trim()); run = ''; } const t = tok.trim(); if (t.length > 0) merged.push(t); }
    }
    if (run.length > 0) merged.push(run.trim());
    const pageTokens = [];
    for (const t of merged) { pageTokens.push(...t.split(/\s+/).filter(x => x.length > 0)); }
    allTokens = allTokens.concat(pageTokens);
  }
  const tokens = allTokens;
  const text = tokens.join(' ');

  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  if (!dateMatch) throw new Error('Date not found. Tokens: ' + tokens.slice(0,30).join('|'));
  const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
  const mo = months[dateMatch[1].toLowerCase()];
  const asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;

  const fatalIdx    = tokens.findIndex(t => t.match(/^Fatal$/i));
  const nonfatalIdx = tokens.findIndex((t,i) => t.match(/^Non.?Fatal$/i) && i > fatalIdx);

  // Grab exactly 3 YTD numbers after a label - only accept values < 2000
  // (YTD shooting counts won't be >2000; all-time totals would be much higher)
  function grab3(startIdx) {
    const nums = [];
    for (let i = startIdx+1; i < tokens.length && nums.length < 3; i++) {
      if (/^\d+$/.test(tokens[i])) {
        const n = parseInt(tokens[i]);
        if (n < 2000) nums.push(n);
      }
      // Stop if we hit the next section label
      if (nums.length === 0 && tokens[i].match(/^(Fatal|Non|Shooting|Total|Year)/i) && i > startIdx+2) break;
    }
    return nums;
  }

  const fatal3    = fatalIdx    !== -1 ? grab3(fatalIdx)    : [];
  const nonfatal3 = nonfatalIdx !== -1 ? grab3(nonfatalIdx) : [];

  console.log('Durham fatal3:', fatal3, 'nonfatal3:', nonfatal3);

  let ytd, prior;
  if (fatal3.length >= 2 && nonfatal3.length >= 2) {
    // Columns are [2024_ytd, 2025_ytd, 2026_ytd] - take last two available
    ytd   = fatal3[fatal3.length-1]    + nonfatal3[nonfatal3.length-1];
    prior = fatal3[fatal3.length-2]    + nonfatal3[nonfatal3.length-2];
  } else {
    throw new Error('Could not parse Durham fatal/nonfatal nums. fatal3=' + fatal3.join(',') + ' nonfatal3=' + nonfatal3.join(',') + ' tokens=' + tokens.slice(0,50).join('|'));
  }

  return { ytd, prior, asof, adid: latestAdid };
}

// ─── Milwaukee (Tableau) ──────────────────────────────────────────────────────

async function fetchMilwaukee() {
  const { chromium } = require('playwright');

  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  page.setDefaultTimeout(30000);

  console.log('Milwaukee: loading Tableau dashboard...');
  await page.goto(
    'https://public.tableau.com/views/MilwaukeePoliceDepartment-PartICrimes/MPDPublicCrimeDashboard?:embed=y&:showVizHome=no',
    { waitUntil: 'domcontentloaded', timeout: 60000 }
  );

  // Wait up to 30 seconds for "Non-Fatal" to appear anywhere on the page
  try {
    await page.waitForFunction(
      () => document.body.innerText.includes('Non-Fatal'),
      { timeout: 30000 }
    );
  } catch(e) {
    console.log('Milwaukee: Non-Fatal not found after 30s, proceeding anyway...');
  }
  await page.waitForTimeout(3000);

  // Extract the as-of date from "Data Current Through: M/D/YYYY"
  const fullText = await page.evaluate(() => document.body.innerText);

  const dateMatch = fullText.match(/Data Current Through[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }

  // Milwaukee table layout (from page text):
  // Headers: "2024 | 2025 | %CHANGE | 2024-25 | YTD 2024 | YTD 2025 | YTD 2026 | %CHANGE YTD 2024-26 | %CHANGE"
  // Then rows: "Non-Fatal Shooting" followed by its numbers, then "Carjacking" row
  // Strategy: find "Non-Fatal" then "Shooting", collect numbers until next offense type
  const lines = fullText.split('\n').map(l => l.trim()).filter(Boolean);
  let ytd = null, prior = null;

  // Find index of "Non-Fatal" line (the row label)
  let nfIdx = -1;
  for (let i = 0; i < lines.length; i++) {
    if (lines[i] === 'Non-Fatal' && lines[i+1] === 'Shooting') { nfIdx = i; break; }
    if (lines[i].match(/^Non-Fatal\s+Shooting$/i)) { nfIdx = i; break; }
  }

  if (nfIdx !== -1) {
    const startIdx = (lines[nfIdx] === 'Non-Fatal') ? nfIdx + 2 : nfIdx + 1;
    const nums = [];
    for (let j = startIdx; j < Math.min(startIdx + 20, lines.length) && nums.length < 9; j++) {
      if (/^-?[\d,]+$/.test(lines[j])) nums.push(parseInt(lines[j].replace(/,/g, '')));
      else if (/^-?\d+\.?\d*%$/.test(lines[j])) continue; // skip % values
      else if (lines[j].match(/^(Carjacking|Homicide|Robbery|Assault)/i)) break;
    }
    console.log('Milwaukee nums:', nums);
    // Columns: full2024, full2025, ytd2024, ytd2025, ytd2026 (may have %change cols too)
    // Filter out likely %change values (small numbers -100 to 100) vs counts
    const counts = nums.filter((n, i) => {
      // The first 2 are full-year counts (larger), next are YTD counts
      return true; // take all, pick by position
    });
    if (counts.length >= 5) {
      // Expected: [full2024, full2025, ytd2024, ytd2025, ytd2026, ...]
      prior = counts[counts.length - 2]; // YTD 2025
      ytd   = counts[counts.length - 1]; // YTD 2026
      // But only if last value looks like a shooting count (< 500)
      if (ytd > 500) { prior = counts[3]; ytd = counts[4]; }
    }
  }

  await browser.close();

  if (ytd === null) throw new Error('Could not find Non-Fatal Shooting YTD values.\nFull text: ' + lines.join(' | '));

  return { ytd, prior, asof };
}

// ─── Memphis (Power BI) ───────────────────────────────────────────────────────

async function fetchMemphis() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  page.setDefaultTimeout(30000);

  console.log('Memphis: loading Power BI dashboard...');
  await page.goto(
    'https://app.powerbigov.us/view?r=eyJrIjoiZTYyYmQ0Y2QtZTM0Ni00ZTFiLThkMjMtOTYxYWZiOWUyZDU4IiwidCI6IjQxNjQ3NTYxLTY1MzctNDQyMy05NmE5LTg1OWU4OWY4OTE5ZiJ9',
    { waitUntil: 'domcontentloaded', timeout: 60000 }
  );

  // Wait for Power BI content to render
  try {
    await page.waitForFunction(
      () => document.body.innerText.includes('Crime Overview'),
      { timeout: 30000 }
    );
  } catch(e) {
    console.log('Memphis: Crime Overview not found after 30s, proceeding anyway...');
  }
  await page.waitForTimeout(5000);

  // Grab as-of date from page 1: "Data through 2/21/2026" bottom right
  const page1Text = await page.evaluate(() => document.body.innerText);
  console.log('Memphis page1 sample:', page1Text.substring(0, 400));
  let asof = null;
  const dateMatch = page1Text.match(/Data through\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  if (dateMatch) {
    asof = dateMatch[3] + '-' + dateMatch[1].padStart(2,'0') + '-' + dateMatch[2].padStart(2,'0');
    console.log('Memphis as-of:', asof);
  }

  // Click Crime Summary tab
  console.log('Memphis: clicking Crime Summary tab...');
  try {
    await page.getByText('Crime Summary').first().click();
  } catch(e) {
    await page.locator('text=Crime Summary').first().click();
  }
  await page.waitForTimeout(4000);

  // Click Non-Fatal Shooting button
  console.log('Memphis: clicking Non-Fatal Shooting...');
  await page.locator('text=Non-Fatal').first().click();
  await page.waitForTimeout(4000);

  // Read chart - shows "2026: 69" and "2025: 92 (-25%)"
  const chartText = await page.evaluate(() => document.body.innerText);
  console.log('Memphis chart sample:', chartText.substring(0, 600));

  await browser.close();

  const yr = new Date().getFullYear();

  // Chart title shows "2026: 69" and "2025: 92 (-25%)" - parse directly from title
  const ytdMatch   = chartText.match(new RegExp(yr + ':\\s*(\\d+)'));
  const priorMatch = chartText.match(new RegExp((yr - 1) + ':\\s*(\\d+)'));

  console.log('Memphis ytdMatch:', ytdMatch && ytdMatch[0], 'priorMatch:', priorMatch && priorMatch[0]);

  if (!ytdMatch) throw new Error('Could not find ' + yr + ': N in chart text. Sample: ' + chartText.substring(0, 400));

  return {
    ytd:   parseInt(ytdMatch[1]),
    prior: priorMatch ? parseInt(priorMatch[1]) : null,
    asof
  };
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

  // Memphis
  try {
    console.log('\n--- Fetching Memphis ---');
    results.memphis = { ...(await fetchMemphis()), fetchedAt, ok: true };
    console.log('Memphis:', results.memphis);
  } catch (e) {
    console.error('Memphis error:', e.message);
    results.memphis = { ok: false, error: e.message, fetchedAt };
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
