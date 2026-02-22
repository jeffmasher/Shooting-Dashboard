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
  // Durham PDF is an image-based bar chart - use Claude vision API to extract numbers
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

  // Get as-of date from PDF text layer (title text is accessible even if chart is image)
  const pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js'));
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfResp.body) }).promise;
  const pg = await pdf.getPage(1);
  const tc = await pg.getTextContent();
  const text = tc.items.map(i => i.str).join(' ');
  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
    const mo = months[dateMatch[1].toLowerCase()];
    if (mo) asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;
  }

  // Render first page to PNG using Playwright, then send to Claude vision API
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  await page.goto(pdfUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
  await page.waitForTimeout(2000);
  const screenshotBuf = await page.screenshot({ fullPage: true });
  await browser.close();

  const base64Image = screenshotBuf.toString('base64');
  console.log('Durham: screenshot taken, size:', screenshotBuf.length, 'bytes');

  // Call Claude API to extract chart values
  const claudeResp = await fetchUrl('https://api.anthropic.com/v1/messages');
  // Use https.request directly for POST with body
  const claudeData = await new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 256,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: base64Image } },
          { type: 'text', text: 'This is a Durham Police Department shooting data chart. Look at the "Non-Fatal" bar group on the right side. What are the exact numbers shown above the three bars for 2024, 2025, and 2026? Reply with ONLY: 2024=N 2025=N 2026=N' }
        ]
      }]
    });
    const req = require('https').request({
      hostname: 'api.anthropic.com',
      path: '/v1/messages',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01',
        'Content-Length': Buffer.byteLength(body)
      }
    }, (res) => {
      const chunks = [];
      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try { resolve(JSON.parse(Buffer.concat(chunks).toString())); }
        catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });

  console.log('Durham Claude response:', JSON.stringify(claudeData).substring(0, 300));
  const responseText = claudeData.content?.[0]?.text || '';
  console.log('Durham vision response:', responseText);

  const m2024 = responseText.match(/2024=(\d+)/);
  const m2025 = responseText.match(/2025=(\d+)/);
  const m2026 = responseText.match(/2026=(\d+)/);

  if (!m2026) throw new Error('Could not parse Durham chart values from vision API. Response: ' + responseText);

  return {
    ytd:   parseInt(m2026[1]),
    prior: m2025 ? parseInt(m2025[1]) : null,
    asof
  };
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

  // Get date from top-level page text
  const fullText = await page.evaluate(() => document.body.innerText);
  const dateMatch = fullText.match(/Data Current Through[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }

  // Table row data lives inside Tableau iframes - search all frames
  let ytd = null, prior = null;
  const allFrames = page.frames();
  console.log('Milwaukee: frame count:', allFrames.length);

  for (const frame of allFrames) {
    try {
      const frameText = await frame.evaluate(() => document.body?.innerText || '');
      if (!frameText.includes('Non-Fatal') || frameText.length < 200) continue;
      console.log('Milwaukee: checking frame, length:', frameText.length, 'sample:', frameText.substring(0,100));

      const lines = frameText.split('\n').map(l => l.trim()).filter(Boolean);
      for (let i = 0; i < lines.length; i++) {
        if ((lines[i] === 'Non-Fatal' && lines[i+1] === 'Shooting') ||
            lines[i].match(/^Non-Fatal\s+Shooting$/i)) {
          const startIdx = lines[i] === 'Non-Fatal' ? i + 2 : i + 1;
          const nums = [];
          for (let j = startIdx; j < Math.min(startIdx + 20, lines.length) && nums.length < 9; j++) {
            if (/^-?[\d,]+$/.test(lines[j])) nums.push(parseInt(lines[j].replace(/,/g, '')));
            else if (/^-?\d+\.?\d*%$/.test(lines[j])) continue;
            else if (lines[j].match(/^(Carjacking|Homicide|Robbery|Assault|Rape)/i)) break;
          }
          console.log('Milwaukee nums from frame:', nums);
          if (nums.length >= 3) {
            ytd   = nums[nums.length - 1];
            prior = nums[nums.length - 2];
          }
          break;
        }
      }
      if (ytd !== null) break;
    } catch(e) { /* cross-origin frame, skip */ }
  }

  await browser.close();

  if (ytd === null) throw new Error('Could not find Non-Fatal Shooting YTD values in any frame.\nTop-level text snippet: ' + fullText.substring(0, 300));

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
  // Title is '2026: 692025: 92 (-25%)' - match both years in one pass
  const bothMatch = chartText.match(new RegExp(yr + ':\\s*(\\d+)' + (yr-1) + ':\\s*(\\d+)'));
  const ytdMatch   = bothMatch ? {1: bothMatch[1]} : null;
  const priorMatch = bothMatch ? {1: bothMatch[2]} : null;

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
