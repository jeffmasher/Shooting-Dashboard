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

  // Date - try text first, fall back to URL filename (YYMMDD e.g. 260219 = 2026-02-19)
  const dateMatch = text.match(/\w+day,\s+(\w+)\s+(\d{1,2}),\s+(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
    const mo = months[dateMatch[1].toLowerCase()];
    asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;
  }
  if (!asof) {
    const fnMatch = pdfUrl.match(/\/(\d{2})(\d{2})(\d{2})%20DPD/);
    if (fnMatch) asof = `20${fnMatch[1]}-${fnMatch[2]}-${fnMatch[3]}`;
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
  // Durham PDF contains an image-based bar chart - render via pdfjs + canvas, send to Claude vision
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

  // Get as-of date from PDF text layer
  const pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js'));
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfResp.body) }).promise;
  const pg1 = await pdf.getPage(1);
  const tc  = await pg1.getTextContent();
  const text = tc.items.map(i => i.str).join(' ');
  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
    const mo = months[dateMatch[1].toLowerCase()];
    if (mo) asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;
  }
  console.log('Durham asof:', asof);

  // Render PDF page to PNG using pdfjs + node-canvas
  const { createCanvas } = require('canvas');
  const viewport = pg1.getViewport({ scale: 2.0 });
  const canvas  = createCanvas(viewport.width, viewport.height);
  const ctx     = canvas.getContext('2d');
  await pg1.render({ canvasContext: ctx, viewport }).promise.catch(e => { throw new Error('Durham PDF render failed: ' + (e && e.message || String(e))); });
  const pngBuf = canvas.toBuffer('image/png');
  console.log('Durham: rendered PDF to PNG, size:', pngBuf.length, 'bytes');

  const base64Image = pngBuf.toString('base64');

  // Send to Claude vision API
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

  const responseText = claudeData.content?.[0]?.text || '';
  console.log('Durham vision response:', responseText);

  const m2025 = responseText.match(/2025=(\d+)/);
  const m2026 = responseText.match(/2026=(\d+)/);
  if (!m2026) throw new Error('Could not parse Durham chart values. Response: ' + responseText + ' API resp: ' + JSON.stringify(claudeData).substring(0,200));

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

  // Wait for dashboard to render
  try {
    await page.waitForFunction(
      () => document.body.innerText.includes('Non-Fatal'),
      { timeout: 30000 }
    );
  } catch(e) {
    console.log('Milwaukee: Non-Fatal not found after 30s, proceeding anyway...');
  }
  await page.waitForTimeout(5000);

  // Get as-of date
  const fullText = await page.evaluate(() => document.body.innerText);
  const dateMatch = fullText.match(/Data Current Through[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }
  console.log('Milwaukee asof:', asof);

  // Screenshot the page and send to Claude vision API
  const screenshotBuf = await page.screenshot({ fullPage: false });
  await browser.close();
  console.log('Milwaukee: screenshot taken, size:', screenshotBuf.length, 'bytes');

  const base64Image = screenshotBuf.toString('base64');

  const claudeData = await new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 256,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: 'image/png', data: base64Image } },
          { type: 'text', text: 'This is a Milwaukee Police Department crime dashboard. Find the row labeled "Non-Fatal Shooting" in the table. It has columns for YTD 2024, YTD 2025, and YTD 2026. What are those three YTD numbers? Reply with ONLY: YTD2024=N YTD2025=N YTD2026=N' }
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

  const responseText = claudeData.content?.[0]?.text || '';
  console.log('Milwaukee vision response:', responseText);

  const m2025 = responseText.match(/YTD2025=(\d+)/);
  const m2026 = responseText.match(/YTD2026=(\d+)/);

  if (!m2026) throw new Error('Could not parse Milwaukee YTD from vision API. Response: ' + responseText);

  return {
    ytd:   parseInt(m2026[1]),
    prior: m2025 ? parseInt(m2025[1]) : null,
    asof
  };
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
  await page.waitForTimeout(6000); // extra wait for chart to render

  // Read chart text + take screenshot for vision fallback
  const chartText = await page.evaluate(() => document.body.innerText);
  console.log('Memphis chart sample:', chartText.substring(0, 800));
  const screenshotBuf = await page.screenshot({ fullPage: false });
  console.log('Memphis: screenshot taken, size:', screenshotBuf.length, 'bytes');

  await browser.close();

  const yr = new Date().getFullYear();

  // Power BI renders the Non-Fatal Shooting bar chart with a title area showing
  // "2026: 70" and "2025: 97 (-27.84%)" — but this may be SVG text not in innerText.
  // The "Year To Date" section lists years and values separately but values are NOT
  // in year order (they follow DOM render order, not chronological).
  // Strategy priority: title text → vision API fallback.
  let ytd = null, prior = null;

  // Strategy 1: Chart title shows "2026: 70" and "2025: 97 (-27.84%)"
  // CRITICAL: Power BI concatenates these on ONE line with NO separator:
  //   "2026: 702025: 97 (-27.84%)"
  // Greedy \d+ would capture "702025" instead of "70".
  // Use lazy \d+? with lookahead: stop before next YYYY:, whitespace, or paren.
  var ytdMatch = chartText.match(new RegExp(yr + ':\\s*(\\d+?)(?=\\d{4}:|\\s|\\(|$)'));
  var priorMatch = chartText.match(new RegExp((yr-1) + ':\\s*(\\d+?)(?=\\d{4}:|\\s|\\(|$)'));
  if (ytdMatch) {
    ytd = parseInt(ytdMatch[1]);
    console.log('Memphis: found YTD from title: ' + yr + ': ' + ytd);
  }
  if (priorMatch) {
    prior = parseInt(priorMatch[1]);
    console.log('Memphis: found prior from title: ' + (yr-1) + ': ' + prior);
  }

  // Sanity check: shooting counts should be 0-999 for YTD
  if (ytd !== null && ytd > 999) {
    console.log('Memphis: implausible ytd=' + ytd + ', resetting to null for vision fallback');
    ytd = null;
  }

  // Strategy 2 (fallback): Screenshot + Vision API
  // The chart is a simple bar chart with values above each bar and years on x-axis.
  if (ytd === null) {
    console.log('Memphis: text parsing failed, using vision API...');
    const base64Image = screenshotBuf.toString('base64');
    const claudeData = await new Promise((resolve, reject) => {
      const body = JSON.stringify({
        model: 'claude-haiku-4-5-20251001',
        max_tokens: 64,
        messages: [{
          role: 'user',
          content: [
            { type: 'image', source: { type: 'base64', media_type: 'image/png', data: base64Image } },
            { type: 'text', text: 'This is a Memphis Non-Fatal Shooting Incidents bar chart. The chart title area shows "YEAR: COUNT" for the current and prior year. What are the two values? Reply ONLY in this format: YTD=N PRIOR=N (where YTD is the current/latest year count and PRIOR is the previous year count)' }
          ]
        }]
      });
      const req = require('https').request({
        hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY,
                   'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) }
      }, (res) => {
        const chunks = []; res.on('data', c => chunks.push(c));
        res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch(e) { reject(e); } });
      });
      req.on('error', reject); req.write(body); req.end();
    });

    const visionText = (claudeData.content?.[0]?.text || '').trim();
    console.log('Memphis vision response:', visionText);

    var vYtd = visionText.match(/YTD=(\d+)/);
    var vPrior = visionText.match(/PRIOR=(\d+)/);
    if (vYtd) ytd = parseInt(vYtd[1]);
    if (vPrior) prior = parseInt(vPrior[1]);
  }

  console.log('Memphis parsed: ytd=' + ytd + ' prior=' + prior);

  if (ytd === null) throw new Error('Could not find ' + yr + ' Non-Fatal Shooting value. Chart text sample: ' + chartText.substring(0, 800));

  return { ytd, prior, asof };
}

// ─── Hampton (JPEG image) ─────────────────────────────────────────────────────

async function fetchHampton() {
  // Hampton posts a JPEG table at a fixed URL - use Claude vision to extract numbers
  const jpegUrl = 'https://www.hampton.gov/DocumentCenter/View/31010/Gunshot-Injury-Data-?bidId=';
  console.log('Hampton: fetching JPEG from', jpegUrl);

  const resp = await fetchUrl(jpegUrl);
  if (resp.status !== 200) throw new Error(`Hampton JPEG HTTP ${resp.status}`);

  const base64Image = resp.body.toString('base64');
  // Detect media type - likely JPEG but confirm
  const mediaType = resp.body[0] === 0xFF && resp.body[1] === 0xD8 ? 'image/jpeg' : 'image/png';
  console.log('Hampton: image size:', resp.body.length, 'bytes, type:', mediaType);

  const claudeData = await new Promise((resolve, reject) => {
    const yr = new Date().getFullYear();
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 256,
      messages: [{
        role: 'user',
        content: [
          { type: 'image', source: { type: 'base64', media_type: mediaType, data: base64Image } },
          { type: 'text', text: `This is a Hampton VA gunshot injury data table. Find the row "Persons Injured from Gunshots (not deceased)" - that is the non-fatal shooting count. What are the YTD ${yr-1} and YTD ${yr} values in that row? Also look for any date range shown (e.g. "Jan. 1 - Feb. 14, ${yr}"). Reply with ONLY: PRIOR=N YTD=N ASOF=YYYY-MM-DD (use the end date of the range for ASOF, or null if not found)` }
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

  const responseText = claudeData.content?.[0]?.text || '';
  console.log('Hampton vision response:', responseText);

  const priorMatch = responseText.match(/PRIOR=(\d+)/);
  const ytdMatch   = responseText.match(/YTD=(\d+)/);
  const asofMatch  = responseText.match(/ASOF=(\d{4}-\d{2}-\d{2})/);

  if (!ytdMatch) throw new Error('Could not parse Hampton values. Response: ' + responseText);

  return {
    ytd:   parseInt(ytdMatch[1]),
    prior: priorMatch ? parseInt(priorMatch[1]) : null,
    asof:  asofMatch ? asofMatch[1] : null
  };
}


// ─── Pittsburgh (Power BI Gov) ───────────────────────────────────────────────

async function fetchPittsburgh() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 768 });
  page.setDefaultTimeout(30000);

  const url = 'https://app.powerbigov.us/view?r=eyJrIjoiMDYzNWMyNGItNWNjMS00ODMwLWIxZDgtMTNkNzhlZDE2OWFjIiwidCI6ImY1ZjQ3OTE3LWM5MDQtNDM2OC05MTIwLWQzMjdjZjE3NTU5MSJ9';
  console.log('Pittsburgh: loading Power BI dashboard...');
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  // Power BI Gov renders via canvas/iframe - wait generously for initial load
  await page.waitForTimeout(20000);

  // Get as-of date from page 1 header "Last Updated: M/DD/YYYY"
  const page1Text = await page.evaluate(() => document.body.innerText);
  const dateMatch = page1Text.match(/Last Updated[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }
  console.log('Pittsburgh asof:', asof);
  console.log('Pittsburgh page1 snippet:', page1Text.substring(0, 400));

  // Navigate to "Year to Date Stats" page - try multiple selectors
  console.log('Pittsburgh: navigating to Year to Date Stats page...');
  let navigated = false;
  for (const selector of [
    '[aria-label="Year to Date Stats"]',
    '[aria-label="Annual Stats"]',
    'button.sectionItem:first-child',
    '.pbi-glyph-chevronrightmedium',
  ]) {
    try {
      await page.locator(selector).first().click({ force: true, timeout: 5000 });
      await page.waitForTimeout(8000);
      console.log('Pittsburgh: navigated via', selector);
      navigated = true;
      break;
    } catch(e) { /* try next */ }
  }
  if (!navigated) console.log('Pittsburgh: could not navigate, will screenshot current page');

  // Log what page we're on now
  const page2Text = await page.evaluate(() => document.body.innerText);
  console.log('Pittsburgh post-nav snippet:', page2Text.substring(0, 400));

  // Note: Gun legend click was attempted but Power BI Gov does not update
  // the accessibility tree when cross-filtering, so we read all-weapon totals.
  // (Guns are ~93% of incidents so this is a close approximation of gun-only.)

  // Parse values directly from page text
  const pageText = await page.evaluate(() => document.body.innerText);
  await browser.close();
  await browser.close();

  const yr = new Date().getFullYear();

  // Extract homicides: find "Number of Homicides" section, grab yr and yr-1 values
  let homYtd = null, homPrior = null, nfsYtd = null, nfsPrior = null;

  // Pattern: after "Number of Homicides", lines are: "Select Row\nYEAR\nVALUE\n..."
  const homSection = pageText.match(/Number of Homicides[\s\S]*?Number of Non-Fatal/);
  if (homSection) {
    const rows = homSection[0].matchAll(/(\d{4})\n(\d+)\n/g);
    for (const r of rows) {
      if (parseInt(r[1]) === yr)     homYtd   = parseInt(r[2]);
      if (parseInt(r[1]) === yr - 1) homPrior = parseInt(r[2]);
    }
  }

  // Pattern: after "Number of Non-Fatal Shootings"
  const nfsSection = pageText.match(/Number of Non-Fatal Shootings[\s\S]*?(?:Last 28|YTD %|$)/);
  if (nfsSection) {
    const rows = nfsSection[0].matchAll(/(\d{4})\n(\d+)\n/g);
    for (const r of rows) {
      if (parseInt(r[1]) === yr)     nfsYtd   = parseInt(r[2]);
      if (parseInt(r[1]) === yr - 1) nfsPrior = parseInt(r[2]);
    }
  }

  // Fallback: scan for year+value pairs near the table headers
  if (homYtd === null || nfsYtd === null) {
    // Try alternate parsing: "Select Row\n2026\n4\n-33.33%"
    const allRows = [...pageText.matchAll(/Select Row\s+(\d{4})\s+(\d+)\s+[-\d.]+%/g)];
    console.log('Pittsburgh fallback rows:', allRows.map(r => `${r[1]}=${r[2]}`).join(', '));
    // First set of year rows = homicides, second set = non-fatal
    const yrRows = allRows.filter(r => parseInt(r[1]) === yr);
    const priorRows = allRows.filter(r => parseInt(r[1]) === yr - 1);
    if (yrRows.length >= 2) {
      homYtd = parseInt(yrRows[0][2]);
      nfsYtd = parseInt(yrRows[1][2]);
    }
    if (priorRows.length >= 2) {
      homPrior = parseInt(priorRows[0][2]);
      nfsPrior = parseInt(priorRows[1][2]);
    }
  }

  console.log(`Pittsburgh parsed: hom${yr}=${homYtd} nfs${yr}=${nfsYtd} hom${yr-1}=${homPrior} nfs${yr-1}=${nfsPrior}`);

  if (homYtd === null || nfsYtd === null) {
    throw new Error('Could not parse Pittsburgh homicide/NFS values from page text');
  }

  return {
    ytd:   homYtd + nfsYtd,
    prior: (homPrior !== null && nfsPrior !== null) ? homPrior + nfsPrior : null,
    asof
  };
}


// ─── Buffalo (Tableau - NY GIVE Dashboard) ────────────────────────────────────

async function fetchBuffalo() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 1024 });
  page.setDefaultTimeout(30000);

  const yr = new Date().getFullYear();

  async function forceClick(locator, timeout) {
    await locator.click({ force: true, timeout: timeout || 8000 });
  }

  // Step 1: Load dashboard
  console.log('Buffalo: loading GIVE dashboard...');
  await page.goto('https://mypublicdashboard.ny.gov/t/OJRP_PUBLIC/views/GIVEInitiative/GIVE-LandingPage', { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(10000);

  // Step 2: Click GIVE-Shooting Activity tab
  console.log('Buffalo: clicking Shooting Activity tab...');
  try {
    await forceClick(page.locator('text=GIVE-Shooting Activity').first());
    await page.waitForTimeout(8000);
    console.log('Buffalo: on Shooting Activity tab');
  } catch(e) { console.log('Buffalo: tab click failed:', e.message); }

  // Step 3: Open Jurisdiction dropdown (second (All) on page)
  console.log('Buffalo: opening Jurisdiction dropdown...');
  try {
    const allEls = page.locator('text=(All)');
    const count = await allEls.count();
    console.log('Buffalo: (All) count:', count);
    await forceClick(allEls.nth(count >= 2 ? 1 : 0));
    await page.waitForTimeout(3000);
    console.log('Buffalo: jurisdiction dropdown opened');
  } catch(e) { console.log('Buffalo: jurisdiction open failed:', e.message); }

  // Step 4: Deselect all inside dropdown
  console.log('Buffalo: deselecting all...');
  try {
    const allEls = page.locator('text=(All)');
    const count = await allEls.count();
    console.log('Buffalo: (All) count after open:', count);
    await forceClick(allEls.last());
    await page.waitForTimeout(1000);
    console.log('Buffalo: deselected all');
  } catch(e) { console.log('Buffalo: deselect all failed:', e.message); }

  // Step 5: Select Buffalo City PD
  console.log('Buffalo: selecting Buffalo City PD...');
  try {
    await forceClick(page.locator('text=Buffalo City PD').first());
    await page.waitForTimeout(1000);
    console.log('Buffalo: selected Buffalo City PD');
  } catch(e) { console.log('Buffalo: Buffalo City PD click failed:', e.message); }

  // Step 6: Click Apply
  console.log('Buffalo: clicking Apply...');
  try {
    await forceClick(page.locator('text=Apply').first());
    await page.waitForTimeout(6000);
    console.log('Buffalo: applied filter');
  } catch(e) { console.log('Buffalo: Apply failed:', e.message); }

  // Step 7: Click Monthly Data toggle
  console.log('Buffalo: clicking Monthly Data...');
  try {
    await forceClick(page.locator('text=Monthly Data').first());
    await page.waitForTimeout(8000);
    console.log('Buffalo: switched to Monthly Data');
  } catch(e) { console.log('Buffalo: Monthly Data click failed:', e.message); }

  // Step 8: Click Download toolbar button
  console.log('Buffalo: clicking Download toolbar button...');
  try {
    await forceClick(page.locator('[data-tb-test-id="viz-viewer-toolbar-button-download"]').first());
    await page.waitForTimeout(2000);
    console.log('Buffalo: download menu opened');
  } catch(e) { console.log('Buffalo: download button failed:', e.message); }

  // Step 9: Click Crosstab
  console.log('Buffalo: clicking Crosstab...');
  try {
    await forceClick(page.locator('text=Crosstab').first());
    await page.waitForTimeout(2000);
    console.log('Buffalo: crosstab dialog opened');
  } catch(e) { console.log('Buffalo: Crosstab click failed:', e.message); }

  // Step 10: Select Monthly Total Overview sheet
  console.log('Buffalo: selecting Monthly Total Overview...');
  try {
    await forceClick(page.locator('text=Monthly Total Overview').first(), 5000);
    await page.waitForTimeout(1000);
    console.log('Buffalo: selected Monthly Total Overview');
  } catch(e) { console.log('Buffalo: sheet selection failed:', e.message); }

  // Step 11: Select CSV
  console.log('Buffalo: selecting CSV...');
  try {
    await forceClick(page.locator('text=CSV').first(), 5000);
    await page.waitForTimeout(500);
    console.log('Buffalo: CSV selected');
  } catch(e) { console.log('Buffalo: CSV select failed:', e.message); }

  // Step 12: Download and capture file
  console.log('Buffalo: clicking Download button...');
  let csvText = null;
  try {
    const [ download ] = await Promise.all([
      page.waitForEvent('download', { timeout: 30000 }),
      forceClick(page.locator('button:has-text("Download")').last())
    ]);
    const stream = await download.createReadStream();
    const chunks = [];
    await new Promise((res, rej) => {
      stream.on('data', c => chunks.push(c));
      stream.on('end', res);
      stream.on('error', rej);
    });
    // File is UTF-16 LE with BOM, tab-delimited, long format
    const buf = Buffer.concat(chunks);
    csvText = buf.toString('utf16le').replace(/^\uFEFF/, '');
    console.log('Buffalo: CSV downloaded, bytes:', buf.length);
    console.log('Buffalo: CSV preview:', csvText.substring(0, 200));
  } catch(e) {
    console.log('Buffalo: CSV download failed:', e.message);
  }

  await browser.close();

  if (!csvText) throw new Error('Buffalo: could not download CSV');

  // Format: tab-delimited, long format
  // Columns: Month | Shooting Category | Count
  // Each row is duplicated — take first occurrence only
  const janCurr  = 'Jan-' + String(yr).slice(2);
  const janPrior = 'Jan-' + String(yr - 1).slice(2);

  let victimsYtd = null, victimsPrior = null;
  let killedYtd  = null, killedPrior  = null;

  const rows = csvText.split('\n').map(function(l) { return l.replace(/\r/g, '').trim(); }).filter(Boolean);
  console.log('Buffalo: total rows:', rows.length, '| janCurr:', janCurr, '| janPrior:', janPrior);

  for (var i = 1; i < rows.length; i++) {
    var cols = rows[i].split('\t');
    if (cols.length < 3) continue;
    var month    = cols[0].trim();
    var category = cols[1].trim().toLowerCase();
    var count    = parseInt(cols[2].trim().replace(/,/g, ''));
    if (isNaN(count)) continue;

    var isVictims = category.indexOf('shooting victims') >= 0 || category.indexOf('persons hit') >= 0;
    var isKilled  = category.indexOf('individuals killed') >= 0 || category.indexOf('gun violence') >= 0;
    if (!isVictims && !isKilled) continue;

    if (month === janCurr) {
      if (isVictims && victimsYtd === null)  victimsYtd = count;
      if (isKilled  && killedYtd  === null)  killedYtd  = count;
    }
    if (month === janPrior) {
      if (isVictims && victimsPrior === null) victimsPrior = count;
      if (isKilled  && killedPrior  === null) killedPrior  = count;
    }
  }

  console.log('Buffalo parsed: victimsYtd=' + victimsYtd + ' killedYtd=' + killedYtd + ' victimsPrior=' + victimsPrior + ' killedPrior=' + killedPrior);

  if (victimsYtd === null || killedYtd === null) {
    var months = rows.slice(1).map(function(r) { return r.split('\t')[0]; });
    var unique = months.filter(function(v, i, a) { return a.indexOf(v) === i; });
    throw new Error('Buffalo: could not find ' + janCurr + ' values. Last months: ' + unique.slice(-6).join(', '));
  }

  return {
    ytd:   victimsYtd + killedYtd,
    prior: (victimsPrior !== null && killedPrior !== null) ? victimsPrior + killedPrior : null,
    asof:  yr + '-01-31'
  };
}



async function fetchMiamiDade() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 768 });
  page.setDefaultTimeout(30000);

  // Load the wrapper page and find the Power BI iframe src
  const url = 'https://www.miamidade.gov/global/police/crime-stats.page';
  console.log('MiamiDade: loading wrapper page...');
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(10000);

  // Find iframe src containing powerbi
  const iframeSrc = await page.evaluate(() => {
    const frames = Array.from(document.querySelectorAll('iframe'));
    const pbi = frames.find(f => f.src && f.src.includes('powerbi'));
    return pbi ? pbi.src : null;
  });
  console.log('MiamiDade iframe src:', iframeSrc);

  if (!iframeSrc) {
    // Log page source snippet to help debug
    const src = await page.content();
    console.log('MiamiDade page source snippet:', src.substring(0, 2000));
    await browser.close();
    throw new Error('Could not find Power BI iframe on Miami-Dade page');
  }

  // Navigate directly to the Power BI embed
  console.log('MiamiDade: loading Power BI embed directly...');
  await page.goto(iframeSrc, { waitUntil: 'domcontentloaded', timeout: 60000 });
  await page.waitForTimeout(20000);

  const page1Text = await page.evaluate(() => document.body.innerText);
  console.log('MiamiDade PBI page1 sample:', page1Text.substring(0, 600));

  // Extract as-of date from "Last update date: MM/DD/YYYY"
  const dateMatch = page1Text.match(/Last update dat[ae][:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (dateMatch) {
    asof = `${dateMatch[3]}-${dateMatch[1].padStart(2,'0')}-${dateMatch[2].padStart(2,'0')}`;
  }
  console.log('MiamiDade asof:', asof);

  // Navigate to page 3 by clicking next twice
  console.log('MiamiDade: navigating to page 3...');
  for (let i = 0; i < 2; i++) {
    try {
      await page.locator('.pbi-glyph-chevronrightmedium').first().click({ force: true, timeout: 5000 });
      await page.waitForTimeout(5000);
      console.log(`MiamiDade: clicked next (${i+1}/2)`);
    } catch(e) {
      // Try aria-label next button
      try {
        await page.locator('[aria-label="Next page"]').first().click({ force: true, timeout: 3000 });
        await page.waitForTimeout(5000);
        console.log(`MiamiDade: clicked Next page button (${i+1}/2)`);
      } catch(e2) {
        console.log(`MiamiDade: nav click ${i+1} failed:`, e.message);
      }
    }
  }

  const page3Text = await page.evaluate(() => document.body.innerText);
  console.log('MiamiDade page3 sample:', page3Text.substring(0, 1000));
  await browser.close();

  // Try to get asof from page3 if page1 didn't have it
  if (!asof) {
    const dateMatch3 = page3Text.match(/Last update dat[ae][:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
    if (dateMatch3) {
      asof = `${dateMatch3[3]}-${dateMatch3[1].padStart(2,'0')}-${dateMatch3[2].padStart(2,'0')}`;
      console.log('MiamiDade asof from page3:', asof);
    }
  }

  const yr = new Date().getFullYear();
  let ytd = null, prior = null;

  // Strategy 1: find SHOOTINGS followed by number+percent pairs
  const shootMatch = page3Text.match(/SHOOTINGS[\s\S]{0,300}/i);
  if (shootMatch) {
    const nums = [...shootMatch[0].matchAll(/(\d+)\s+[-\d.]+%/g)];
    console.log('MiamiDade shootings nums:', nums.map(m => m[1]).join(', '));
    if (nums.length >= 1) ytd   = parseInt(nums[0][1]);
    if (nums.length >= 2) prior = parseInt(nums[1][1]);
  }

  // Strategy 2: line-by-line scan
  if (ytd === null) {
    const lines = page3Text.split('\n').map(l => l.trim()).filter(Boolean);
    for (let i = 0; i < lines.length; i++) {
      if (/^SHOOTINGS$/i.test(lines[i])) {
        const vals = [];
        for (let j = i + 1; j < Math.min(i + 10, lines.length); j++) {
          if (/^\d+$/.test(lines[j])) vals.push(parseInt(lines[j]));
          if (vals.length === 2) break;
        }
        console.log('MiamiDade line-scan shootings:', vals);
        if (vals.length >= 1) ytd   = vals[0];
        if (vals.length >= 2) prior = vals[1];
        break;
      }
    }
  }

  console.log(`MiamiDade parsed: ytd=${ytd} prior=${prior}`);
  if (ytd === null) throw new Error('Could not parse MiamiDade shootings from page text');

  return { ytd, prior, asof };
}


// ─── Main ─────────────────────────────────────────────────────────────────────

// ─── Omaha ─────────────────────────────────────────────────────────────────────
// Fetches the OPD Non-Fatal Shootings and Homicides PDF and sums YTD V columns

async function fetchOmaha() {
  // Use Playwright to get the rendered page (JS-rendered links)
  // The OPD site is fully JS-rendered so we can't scrape it.
  // Instead, fetch the PDF directly using the known URL pattern.
  // URL format: /images/crime-statistics-reports/2024/Website_-_Non-Fatal_Shootings_and_Homicides_MMDDYYYY.pdf
  // We try recent dates going backwards from today to find the current file.
  
  // police.cityofomaha.org returns 403 from GitHub Actions IPs.
  // Instead, commit the latest PDF to data/omaha-shootings.pdf and read it locally.
  const pdfPath = require('path').join(__dirname, '..', 'data', 'omaha-shootings.pdf');
  if (!require('fs').existsSync(pdfPath)) {
    throw new Error('Omaha PDF not found at data/omaha-shootings.pdf — please commit the latest PDF from https://police.cityofomaha.org/opd-crime-statistics');
  }
  const pdfBuf = require('fs').readFileSync(pdfPath);
  console.log('Omaha PDF loaded from local file, size:', pdfBuf.length);

  // Parse all pages to find the YTD row for current year
  let pdfjsLib;
  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }
  catch(e) { pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }

  const loadTask = pdfjsLib.getDocument({ data: new Uint8Array(pdfBuf) });
  const pdfDoc = await loadTask.promise;

  let allText = '';
  for (let p = 1; p <= pdfDoc.numPages; p++) {
    const page = await pdfDoc.getPage(p);
    const tc = await page.getTextContent();
    allText += tc.items.map(i => i.str).join(' ') + '\n';
  }

  // The PDF has rows like: 2026  NFS_I  NFS_V  HOM_I  HOM_V  (per month, then YTD at end)
  // Strategy: find the "Last update" date for asof, then find the current year YTD values
  const yr = new Date().getFullYear();

  // Extract asof from "Last update: Non-Fatal Shootings M/D/YYYY"
  const asofMatch = allText.match(/Last update[:\s]+Non-Fatal Shootings\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i)
    || allText.match(/Last update[:\s]+\S+\s+(\d{1,2})\/(\d{1,2})\/(\d{4})/i);
  let asof = null;
  if (asofMatch) {
    const [, m, d, y] = asofMatch;
    asof = `${y}-${m.padStart(2,'0')}-${d.padStart(2,'0')}`;
  }
  console.log('Omaha asof:', asof);

  // Extract tokens as numbers for parsing
  // The table structure per year row: YEAR NFS_I NFS_V HOM_I HOM_V (repeated per month) ... YTD_NFS_I YTD_NFS_V YTD_HOM_I YTD_HOM_V
  // We need the YTD column which is the last set of 4 numbers in the current year row
  // Find the year row: look for "2026" followed by sequences of numbers
  const tokens = allText.replace(/\s+/g, ' ').split(' ');
  
  // Find index of the current year
  const yrIdx = tokens.findIndex(t => t === String(yr));
  const priorYrIdx = tokens.findIndex(t => t === String(yr - 1));

  function extractYtdFromRow(startIdx, useFirst = false) {
    if (startIdx < 0) return null;
    const rowTokens = [];
    for (let i = startIdx + 1; i < tokens.length && i < startIdx + 300; i++) {
      const t = tokens[i];
      if (/^\d{4}$/.test(t) && parseInt(t) >= 2020) break;
      rowTokens.push(t);
    }
    const nums = rowTokens.filter(t => /^\d+$/.test(t)).map(Number);
    const nonZeroNums = nums.filter(n => n > 0);
    if (nonZeroNums.length >= 4) {
      const n = nonZeroNums.length;
      // For current year: last 4 non-zero = YTD (NFS_I, NFS_V, HOM_I, HOM_V)
      // For prior year: first 4 non-zero = Jan data (same-period comparison)
      if (useFirst) {
        return { nfsV: nonZeroNums[1], homV: nonZeroNums[3] };
      }
      return { nfsV: nonZeroNums[n - 3], homV: nonZeroNums[n - 1] };
    }
    return null;
  }

  const ytdData   = extractYtdFromRow(yrIdx, false);
  const priorData = extractYtdFromRow(priorYrIdx, true);

  console.log(`Omaha ytd data:`, ytdData, `prior data:`, priorData);

  if (!ytdData) throw new Error('Could not parse Omaha YTD row for ' + yr);

  return {
    ytd:   ytdData.nfsV + ytdData.homV,
    prior: priorData ? priorData.nfsV + priorData.homV : null,
    asof
  };
}


// ─── Wilmington (PDF - WPD CompStat Report) ───────────────────────────────────
// Index page: https://www.wilmingtonde.gov/government/public-safety/wilmington-police-department/compstat-reports
// Scrapes the index for the latest "WPD CompStat Report" PDF link, downloads it,
// extracts text layer to find Shooting Victims row: YTD current + YTD prior year.

async function fetchWilmington() {
  // Akamai blocks all direct requests and Playwright sessions.
  // Strategy: try Wayback Machine CDX API to find the most recent cached PDF,
  // then fetch it from the Wayback Machine's cache (which bypasses the block).

  // Use Wayback Machine CDX API to find the most recently cached PDF URL
  const cdxUrl = 'https://web.archive.org/cdx/search/cdx?url=wilmingtonde.gov/home/showpublisheddocument/*&output=json&limit=5&from=20260101&fl=original,timestamp&filter=statuscode:200&collapse=digest';
  console.log('Wilmington: querying Wayback CDX...');
  const cdxResp = await fetchUrl(cdxUrl, 15000).catch(() => null);
  
  if (cdxResp && cdxResp.status === 200) {
    const cdxText = cdxResp.body.toString('utf8');
    console.log('Wilmington: CDX response:', cdxText.substring(0, 300));
    try {
      const rows = JSON.parse(cdxText);
      // rows[0] is header ["original","timestamp"], rest are data
      // Sort by timestamp descending, pick most recent
      const dataRows = rows.slice(1).sort((a,b) => b[1].localeCompare(a[1]));
      if (dataRows.length > 0) {
        const [origUrl, ts] = dataRows[0];
        // Fetch from Wayback Machine cache
        const waybackUrl = 'https://web.archive.org/web/' + ts + 'if_/' + origUrl;
        console.log('Wilmington: fetching from Wayback:', waybackUrl);
        const pdfResp = await fetchUrl(waybackUrl, 30000);
        if (pdfResp.status === 200 && pdfResp.body[0] === 0x25 && pdfResp.body[1] === 0x50) {
          console.log('Wilmington: got PDF from Wayback, bytes:', pdfResp.body.length);
          return await parseWilmingtonPdf(pdfResp.body, origUrl);
        }
        console.log('Wilmington: Wayback fetch status:', pdfResp.status, 'magic:', pdfResp.body.slice(0,4).toString('hex'));
      }
    } catch(e) {
      console.log('Wilmington: CDX parse error:', e.message);
    }
  } else {
    console.log('Wilmington: CDX request failed, status:', cdxResp?.status);
  }

  throw new Error('Wilmington: could not retrieve PDF via Wayback Machine');
}

async function parseWilmingtonPdf(pdfBuf, sourceUrl) {
  console.log('Wilmington: parsing PDF from', sourceUrl, 'bytes:', pdfBuf.length);
  const pdfjsLib = require(require('path').join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js'));
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfBuf) }).promise;

  let allTokens = [];
  for (let p = 1; p <= Math.min(pdf.numPages, 4); p++) {
    const pg = await pdf.getPage(p);
    const tc = await pg.getTextContent();
    allTokens = allTokens.concat(tc.items.map(i => i.str.trim()).filter(Boolean));
  }
  const joined = allTokens.join(' ');
  console.log('Wilmington: PDF tokens (first 400):', joined.substring(0, 400));

  // Extract as-of date
  let asof = null;
  for (const re of [
    /[Ww]eek\s+[Ee]nding[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/,
    /[Tt]hrough[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/,
    /[Aa]s\s+[Oo]f[:\s]+(\d{1,2})\/(\d{1,2})\/(\d{4})/,
    /(\d{1,2})\/(\d{1,2})\/(\d{4})/,
  ]) {
    const m = joined.match(re);
    if (m) { asof = m[3] + '-' + m[1].padStart(2,'0') + '-' + m[2].padStart(2,'0'); break; }
  }
  console.log('Wilmington: asof:', asof);

  // Find Shooting Victims row
  const shootMatch = joined.match(/Shooting\s+Victim[s]?[\s\S]{0,300}?(?=[A-Z][a-z]+\s+[A-Z]|Homicide|Murder|Robbery|Assault|$)/i);
  if (!shootMatch) {
    console.log('Wilmington: full token text:', joined.substring(0, 800));
    throw new Error('Wilmington: Shooting Victims row not found');
  }
  console.log('Wilmington: shooting row:', shootMatch[0].substring(0, 200));

  const nums = [...shootMatch[0].matchAll(/(\d+)/g)].map(m => parseInt(m[1]));
  console.log('Wilmington: nums:', nums);
  if (nums.length < 2) throw new Error('Wilmington: not enough numbers: ' + nums.join(','));

  // CompStat layout: [this week] [28-day] [YTD curr] [YTD prior]
  let ytd, prior;
  if (nums.length === 2) { [ytd, prior] = nums; }
  else if (nums.length >= 4) { ytd = nums[nums.length - 2]; prior = nums[nums.length - 1]; }
  else { ytd = nums[0]; prior = nums[1]; }

  if (ytd < 0 || ytd > 999 || prior < 0 || prior > 999) {
    throw new Error('Wilmington: implausible values ytd=' + ytd + ' prior=' + prior + ' nums=' + nums.join(','));
  }

  console.log('Wilmington: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  return { ytd, prior, asof };
}


async function main() {
  const fetchedAt = new Date().toISOString();
  const outDir = path.join(__dirname, '..', 'data');
  const outPath = path.join(outDir, 'manual-auto.json');

  let existing = {};
  try { existing = JSON.parse(fs.readFileSync(outPath, 'utf8')); } catch (e) { /* first run */ }

  const results = {};

  // Helper: wrap a fetch with a timeout and catch errors without throwing
  function safe(name, fn, timeoutMs) {
    timeoutMs = timeoutMs || 120000;
    const timer = new Promise((_, reject) => setTimeout(() => reject(new Error(name + ' timed out after ' + (timeoutMs/1000) + 's')), timeoutMs));
    return Promise.race([fn(), timer])
      .then(function(r) {
        console.log('\n--- ' + name + ' OK ---');
        console.log(name + ':', { ...r, fetchedAt, ok: true });
        return { key: name.toLowerCase().replace(/[^a-z]/g,''), value: { ...r, fetchedAt, ok: true } };
      })
      .catch(function(e) {
        console.error('\n--- ' + name + ' FAILED:', e.message, '---');
        return { key: name.toLowerCase().replace(/[^a-z]/g,''), value: { ok: false, error: e.message, fetchedAt } };
      });
  }

  // Omaha is manual — preserve existing
  results.omaha = existing.omaha || { ok: false, error: 'No manual data yet' };

  // Run all fetches in parallel
  console.log('Starting all fetches in parallel...');
  const fetches = await Promise.all([
    safe('Detroit',    fetchDetroit,    60000),
    safe('Durham',     fetchDurham,     60000),
    safe('Milwaukee',  fetchMilwaukee,  60000),
    safe('Memphis',    fetchMemphis,    120000),
    safe('Hampton',    fetchHampton,    60000),
    safe('MiamiDade',  fetchMiamiDade,  120000),
    safe('Pittsburgh', fetchPittsburgh, 120000),
    safe('Portland',   fetchPortland,   240000),
    safe('Buffalo',    fetchBuffalo,    120000),
    safe('Nashville',  fetchNashville,  180000),
    safe('Hartford',   fetchHartford,   60000),
    safe('Decatur',    fetchDecatur,    60000),
    // Wilmington removed — Akamai blocks all automated access; manual entry only
  ]);

  for (const { key, value } of fetches) {
    results[key] = value;
  }

  // Write output
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log('\nWrote', outPath);
  console.log(JSON.stringify(results, null, 2));
}

main().catch(e => { console.error(e); process.exit(1); });


// ─── Portland (Tableau - PPB Shooting Incident Statistics) ───────────────────
// The YTD chart numbers are rendered as SVG — not accessible via innerText.
// Strategy: load the viz, clear the date filter so YTD shows all months,
// deselect "No Injury" from the shooting type filter, then take a screenshot
// of the YTD Comparison bar chart and send to vision API.

async function fetchPortland() {
  // Uses the Gun Violence Trends Report dashboard (YTD & Rolling Year Statistics sheet)
  // Table numbers are SVG (not DOM text), so we screenshot + vision API.
  // We crop just the YTD Statistics table to get clean numbers.
  // Row order: HomicideVictims, HomFirearmVictims, HomFirearmIncidents, NonFatalInjury, NonInjury, Total
  // We sum HomFirearmIncidents + NonFatalInjury = Shooting Incidents (Portland definition)
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 1200 });
  page.setDefaultTimeout(90000);

  const embedUrl = 'https://public.tableau.com/views/GunViolenceTrendsReport/YeartoDateRollingYearStatistics?:showVizHome=no&:embed=true';
  console.log('Portland: loading Gun Violence Trends Report YTD sheet...');
  await page.goto(embedUrl, { waitUntil: 'networkidle', timeout: 90000 }).catch(() => {});

  // Wait for table to render — poll for text content stabilizing
  let bodyText = '';
  for (var w = 0; w < 15; w++) {
    await page.waitForTimeout(4000);
    bodyText = await page.evaluate(function() { return document.body.innerText; });
    if (bodyText.includes('Total Shooting Incidents') && bodyText.length > 2000) break;
    console.log('Portland: waiting for render... attempt', (w+1), 'len:', bodyText.length);
  }
  console.log('Portland: body length after wait:', bodyText.length);
  if (bodyText.length < 3000) {
    console.log('Portland: body snippet (first 500):', bodyText.substring(0, 500));
  }

  // Parse asof from "Current Year to Date: January 1, YYYY - Month D, YYYY"
  let asof = null;
  // Date range appears as "January 1, 2026 - January 31, 2026" or with | separator
  // We want the END date (second date = last day of coverage)
  const asofMatch = bodyText.match(/Current Year to Date:[^\n]*?([A-Z][a-z]+ \d+, \d{4})\s*[-|]\s*([A-Z][a-z]+ \d+, \d{4})/);
  if (asofMatch) {
    const d = new Date(asofMatch[2]); // end date
    if (!isNaN(d)) asof = d.toISOString().slice(0,10);
  }
  if (!asof) {
    // Fallback: grab any date-like string near "to Date"
    const m2 = bodyText.match(/([A-Z][a-z]+ \d+, \d{4})\s*[-|]\s*([A-Z][a-z]+ \d+, \d{4})/);
    if (m2) { const d = new Date(m2[2]); if (!isNaN(d)) asof = d.toISOString().slice(0,10); }
  }
  if (!asof) {
    const upd = bodyText.match(/Updated:\s*(\d+)\/(\d+)\/(\d+)/);
    if (upd) asof = upd[3] + '-' + upd[1].padStart(2,'0') + '-' + upd[2].padStart(2,'0');
  }
  console.log('Portland: asof:', asof);

  // Extra wait to ensure SVG numbers have painted before screenshotting
  await page.waitForTimeout(5000);

  // Screenshot the full page for vision API
  const screenshotBuf = await page.screenshot({ fullPage: false });
  await browser.close();
  console.log('Portland: screenshot taken, size:', screenshotBuf.length, 'bytes');

  const base64Image = screenshotBuf.toString('base64');
  const promptText = [
    'This is a Portland Police Bureau "Gun Violence Trends Report" Tableau dashboard showing the Year to Date Statistics table.',
    'The table has these rows: Homicide Victims, Homicides by Firearm Victims, Homicides by Firearm Incidents, Non-Fatal Injury Shooting Incidents, Non-Injury Shooting Incidents, Total Shooting Incidents.',
    'Columns include: Current YTD Count, Previous YTD Count.',
    'Extract ONLY these two values:',
    '  - "Homicides by Firearm Incidents" Current YTD Count → call it HOM',
    '  - "Non-Fatal Injury Shooting Incidents" Current YTD Count → call it NFSI',
    '  - "Homicides by Firearm Incidents" Previous YTD Count → call it HOM_PRIOR',
    '  - "Non-Fatal Injury Shooting Incidents" Previous YTD Count → call it NFSI_PRIOR',
    'Reply ONLY in this exact format with no other text: HOM=N NFSI=N HOM_PRIOR=N NFSI_PRIOR=N'
  ].join(' ');

  async function callVision(imgB64) {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 128,
      messages: [{ role: 'user', content: [
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: imgB64 } },
        { type: 'text', text: promptText }
      ]}]
    });
    const resp = await new Promise((resolve, reject) => {
      const req = require('https').request({
        hostname: 'api.anthropic.com', path: '/v1/messages', method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY,
                   'anthropic-version': '2023-06-01', 'Content-Length': Buffer.byteLength(body) }
      }, (res) => {
        const chunks = []; res.on('data', c => chunks.push(c));
        res.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString())); } catch(e) { reject(e); } });
      });
      req.on('error', reject); req.write(body); req.end();
    });
    return (resp.content?.[0]?.text || '').trim();
  }

  let visionText = await callVision(base64Image);
  console.log('Portland vision response:', visionText);
  // Retry up to 3 times if response is empty or unparseable
  for (let retry = 1; retry <= 3 && !visionText.includes('HOM='); retry++) {
    console.log('Portland: retrying vision call attempt ' + retry + '...');
    await new Promise(r => setTimeout(r, 3000));
    visionText = await callVision(base64Image);
    console.log('Portland vision retry ' + retry + ' response:', visionText);
  }

  function extractVal(text, key) {
    var idx = text.indexOf(key + '=');
    if (idx < 0) return null;
    var m = text.slice(idx + key.length + 1).match(/^(\d+)/);
    return m ? parseInt(m[1]) : null;
  }

  const homYtd   = extractVal(visionText, 'HOM');
  const nfsiYtd  = extractVal(visionText, 'NFSI');
  const homPrior = extractVal(visionText, 'HOM_PRIOR');
  const nfsiPrior = extractVal(visionText, 'NFSI_PRIOR');

  console.log('Portland: hom=' + homYtd + ' nfsi=' + nfsiYtd + ' hom_prior=' + homPrior + ' nfsi_prior=' + nfsiPrior);

  if (homYtd === null || nfsiYtd === null || homPrior === null || nfsiPrior === null) {
    throw new Error('Portland: vision parse failed: ' + visionText);
  }

  const ytd   = homYtd + nfsiYtd;
  const prior = homPrior + nfsiPrior;
  console.log('Portland final: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  if (ytd === 0 && prior === 0) throw new Error('Portland: parsed all zeros');
  return { ytd, prior, asof };
}


// ─── Hartford (CompStat PDF) ─────────────────────────────────────────────────
// Weekly CompStat report, published with predictable URL based on week-ending Saturday.
// URL pattern: https://www.hartfordct.gov/files/assets/public/v/1/police/police-documents/compstat/{YYYY}/{MM}/we-{MM}-{DD}-{YY}.pdf
// Page 2: Citywide table with "Murder Victims" and "Non_Fatal Shooting Victims" under YTD columns.

async function fetchHartford() {
  // Generate week-ending Saturday dates to try (most recent first)
  function getWeekEndingSaturdays() {
    const dates = [];
    const now = new Date();
    for (let i = 0; i < 8; i++) {
      const d = new Date(now);
      d.setDate(d.getDate() - d.getDay() - 1 - (7 * i)); // Previous Saturday
      dates.push(d);
    }
    return dates;
  }

  // Use /v/1/ - the site redirects to the current version automatically
  // (Playwright handles the redirect; raw HTTP gets blocked by WAF with 403)
  function buildUrl(d) {
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const yy = String(yyyy).slice(-2);
    return 'https://www.hartfordct.gov/files/assets/public/v/1/police/police-documents/compstat/' + yyyy + '/' + mm + '/we-' + mm + '-' + dd + '-' + yy + '.pdf';
  }

  function fmtDate(d) {
    return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
  }

  // Use Playwright to download PDF.
  // - context.request.fetch for HEAD: server-side HTTP, bypasses CORS (page.evaluate fetch fails)
  // - acceptDownloads + waitForEvent('download'): PDF URLs trigger browser download, not navigation
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  const saturdays = getWeekEndingSaturdays();
  let pdfBuffer = null;
  let asof = null;

  try {
    for (const d of saturdays) {
      const url = buildUrl(d);
      console.log('Hartford: trying', url);
      try {
        // Server-side HEAD check — no CORS, fast 404 detection
        const probe = await context.request.fetch(url, { method: 'HEAD', timeout: 10000 }).catch(() => null);
        const probeStatus = probe ? probe.status() : 0;
        console.log('Hartford:   status=' + probeStatus);
        if (probeStatus !== 200) continue;

        // File exists — intercept the download event
        const [download] = await Promise.all([
          page.waitForEvent('download', { timeout: 30000 }),
          page.goto(url, { waitUntil: 'commit', timeout: 30000 }).catch(() => {})
        ]);
        const downloadPath = await download.path();
        if (!downloadPath) { console.log('Hartford:   download path null'); continue; }
        const body = require('fs').readFileSync(downloadPath);
        if (body.length > 10000 && body[0] === 0x25) {
          pdfBuffer = body;
          asof = fmtDate(d);
          console.log('Hartford: downloaded PDF for', asof, '(' + (body.length / 1024).toFixed(0) + ' KB)');
          break;
        }
      } catch(e) {
        console.log('Hartford:   error:', e.message);
      }
    }
  } finally {
    await browser.close();
  }

  if (!pdfBuffer) throw new Error('Hartford: could not download any recent CompStat PDF');

  // Extract tokens from page 2 (Citywide table)
  const tokens = await extractPdfTokens(pdfBuffer, 2);
  const joined = tokens.join(' ');

  // Parse a Victim Counts row by label
  // Each row has 13 values after the label:
  // CW2026 CW2025 CW+/- PW2026 PW+/- 28D2026 28D2025 28D+/- YTD2026 YTD2025 YTD+/- 2Y2024 2Y+/-
  function parseVictimRow(label) {
    var idx = joined.indexOf(label);
    if (idx === -1) return { ytd2026: 0, ytd2025: 0 };
    var afterLabel = joined.substring(idx + label.length).trim();
    var vals = afterLabel.split(/\s+/).slice(0, 13);
    function parseVal(s) {
      if (!s || s === '-') return 0;
      var n = parseInt(s.replace(/,/g, ''));
      return isNaN(n) ? 0 : n;
    }
    return { ytd2026: parseVal(vals[8]), ytd2025: parseVal(vals[9]) };
  }

  var nonfatal = parseVictimRow('Non_Fatal Shooting Victims');

  console.log('Hartford: non-fatal YTD=' + nonfatal.ytd2026 + ' prior=' + nonfatal.ytd2025);

  var ytd = nonfatal.ytd2026;
  var prior = nonfatal.ytd2025;

  // Try to get as-of from PDF text (more reliable than URL date)
  var ytdMatch = joined.match(/Year\s+to\s+Date.*?to\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  if (ytdMatch) {
    var months = {jan:1,january:1,feb:2,february:2,mar:3,march:3,apr:4,april:4,may:5,jun:6,june:6,jul:7,july:7,aug:8,august:8,sep:9,september:9,oct:10,october:10,nov:11,november:11,dec:12,december:12};
    var mo = months[ytdMatch[1].toLowerCase()];
    if (mo) asof = ytdMatch[3] + '-' + String(mo).padStart(2,'0') + '-' + String(parseInt(ytdMatch[2])).padStart(2,'0');
  }

  return { ytd: ytd, prior: prior, asof: asof };
}


// ─── Nashville (MNPD Crime Initiative Book PDF) ─────────────────────────────
// Downloads the weekly Crime Initiative Book PDF from MNPD's public SharePoint
// and extracts YTD shooting data from the "Gunshot Victims" page (~p.146).
// Uses group-based triplet validation to reliably parse (prior, current, change)
// values while naturally filtering out percentage columns.
//
// Source: https://metronashville.sharepoint.com/sites/MNPDCrimeAnalysis-Public
// PDF: YYYYMMDD_Crime_Initiative_Book.pdf (published weekly, typically Friday)
// Page: ~146 "Gunshot Victims (Homicides, Injuries, and Property Damage)"

async function fetchNashville() {

  // ── Date utilities ──
  // Reports use Saturday dates in the filename: YYYYMMDD
  function getReportDatesToTry() {
    const dates = [];
    const now = new Date();
    for (let weeksBack = 0; weeksBack <= 4; weeksBack++) {
      // Find recent Saturdays
      const sat = new Date(now);
      sat.setDate(sat.getDate() - sat.getDay() - 1 - (7 * weeksBack));
      dates.push(formatDateStr(sat));
      // Also try Fridays
      const fri = new Date(sat);
      fri.setDate(fri.getDate() - 1);
      dates.push(formatDateStr(fri));
    }
    return [...new Set(dates)];
  }

  function formatDateStr(d) {
    return d.getFullYear() + String(d.getMonth()+1).padStart(2,'0') + String(d.getDate()).padStart(2,'0');
  }

  // ── PDF download strategies ──
  const downloadDir = path.join(__dirname, '..', 'data', 'nashville-downloads');
  if (!fs.existsSync(downloadDir)) fs.mkdirSync(downloadDir, { recursive: true });

  async function downloadPdf(dateStr) {
    const year = dateStr.substring(0, 4);
    const filename = `${dateStr}_Crime_Initiative_Book.pdf`;
    const localPath = path.join(downloadDir, filename);

    // Check for already-downloaded file
    if (fs.existsSync(localPath) && fs.statSync(localPath).size > 100000) {
      console.log('Nashville: using cached PDF:', filename);
      return localPath;
    }

    // Strategy 1: Direct HTTP fetch (works if SharePoint allows anonymous access)
    const directUrls = [
      `https://metronashville.sharepoint.com/sites/MNPDCrimeAnalysis-Public/Shared%20Documents/Weekly%20Crime%20-%20Initiative%20Book/${year}/${filename}`,
      `https://metronashville.sharepoint.com/sites/MNPDCrimeAnalysis-Public/_layouts/15/download.aspx?SourceUrl=/sites/MNPDCrimeAnalysis-Public/Shared%20Documents/Weekly%20Crime%20-%20Initiative%20Book/${year}/${filename}`,
    ];

    for (const url of directUrls) {
      try {
        console.log('Nashville: trying direct URL for', dateStr, '...');
        const resp = await fetchUrl(url, 30000);
        if (resp.status === 200 && resp.body.length > 100000 && resp.body[0] === 0x25 && resp.body[1] === 0x50) {
          fs.writeFileSync(localPath, resp.body);
          console.log('Nashville: downloaded via direct URL (' + (resp.body.length / 1024 / 1024).toFixed(1) + ' MB)');
          return localPath;
        }
      } catch (e) { /* try next */ }
    }

    // Strategy 2: Playwright (navigate SharePoint UI)
    try {
      console.log('Nashville: trying Playwright for', dateStr, '...');
      const { chromium } = require('playwright');
      const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
      const page = await browser.newPage();
      await page.setViewportSize({ width: 1920, height: 1080 });

      // Try loading the direct URL in Playwright (handles JS redirects SharePoint may do)
      const spUrl = directUrls[0];
      const response = await page.goto(spUrl, { waitUntil: 'networkidle', timeout: 45000 }).catch(() => null);

      if (response) {
        const contentType = response.headers()['content-type'] || '';
        if (contentType.includes('pdf')) {
          const buffer = await response.body().catch(() => null);
          if (buffer && buffer.length > 100000) {
            fs.writeFileSync(localPath, buffer);
            await browser.close();
            console.log('Nashville: downloaded via Playwright direct (' + (buffer.length / 1024 / 1024).toFixed(1) + ' MB)');
            return localPath;
          }
        }
      }

      // Navigate the SharePoint share link folder UI
      const shareLink = 'https://metronashville.sharepoint.com/:f:/s/MNPDCrimeAnalysis-Public/Ei-WvJMw8N5OiETXZcnTwlgBlnNytrIMj_wiYADfzMln9g?e=L5g6b2';
      console.log('Nashville: navigating SharePoint folder UI...');
      await page.goto(shareLink, { waitUntil: 'networkidle', timeout: 60000 }).catch(() => {});
      await page.waitForTimeout(5000);

      // Click into the year folder
      const yearEl = await page.locator(`text=${year}`).first();
      if (await yearEl.isVisible({ timeout: 5000 }).catch(() => false)) {
        await yearEl.click();
        await page.waitForTimeout(5000);
      }

      // Click the PDF file
      const fileEl = await page.locator(`text=${dateStr}`).first();
      if (await fileEl.isVisible({ timeout: 5000 }).catch(() => false)) {
        await fileEl.click();
        await page.waitForTimeout(3000);

        // Find and click download button
        for (const sel of ['[data-automationid="downloadCommand"]', '[aria-label*="Download"]', 'button:has-text("Download")']) {
          try {
            const btn = await page.locator(sel).first();
            if (await btn.isVisible({ timeout: 3000 }).catch(() => false)) {
              const [download] = await Promise.all([
                page.waitForEvent('download', { timeout: 30000 }),
                btn.click()
              ]);
              const stream = await download.createReadStream();
              const chunks = [];
              await new Promise((res, rej) => {
                stream.on('data', c => chunks.push(c));
                stream.on('end', res);
                stream.on('error', rej);
              });
              const buf = Buffer.concat(chunks);
              if (buf.length > 100000) {
                fs.writeFileSync(localPath, buf);
                await browser.close();
                console.log('Nashville: downloaded via SharePoint UI (' + (buf.length / 1024 / 1024).toFixed(1) + ' MB)');
                return localPath;
              }
              break;
            }
          } catch (e) { /* try next selector */ }
        }
      }

      await browser.close();
    } catch (e) {
      console.log('Nashville: Playwright strategy failed:', e.message);
    }

    return null;
  }

  // ── Try to get a PDF ──
  let pdfPath = null;

  // Check for any pre-committed PDF in data/nashville-downloads/
  if (fs.existsSync(downloadDir)) {
    const existing = fs.readdirSync(downloadDir)
      .filter(f => f.endsWith('.pdf') && f.includes('Crime_Initiative_Book'))
      .sort().reverse();
    if (existing.length > 0) {
      pdfPath = path.join(downloadDir, existing[0]);
      console.log('Nashville: found local PDF:', existing[0]);
    }
  }

  // Try downloading the latest if no local file
  if (!pdfPath) {
    const datesToTry = getReportDatesToTry();
    console.log('Nashville: trying dates:', datesToTry.slice(0, 6).join(', '));
    for (const dateStr of datesToTry) {
      pdfPath = await downloadPdf(dateStr);
      if (pdfPath) break;
    }
  }

  if (!pdfPath) {
    throw new Error('Nashville: could not obtain Crime Initiative Book PDF. Place it manually in data/nashville-downloads/');
  }

  // ── Parse the PDF using pdfjs-dist (same lib used by Detroit/Durham) ──
  console.log('Nashville: parsing', path.basename(pdfPath));
  const pdfBuffer = fs.readFileSync(pdfPath);
  let pdfjsLib;
  try { pdfjsLib = require('pdfjs-dist/legacy/build/pdf.js'); }
  catch(e) { pdfjsLib = require(path.join(__dirname, '..', 'node_modules', 'pdfjs-dist', 'legacy', 'build', 'pdf.js')); }
  pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(pdfBuffer) }).promise;
  console.log('Nashville: PDF has', pdf.numPages, 'pages');

  // Extract text from each page (group by y-coordinate to preserve lines)
  const pages = [];
  for (let i = 1; i <= pdf.numPages; i++) {
    const pg = await pdf.getPage(i);
    const tc = await pg.getTextContent();
    // Group items by y-coordinate to reconstruct lines
    let lastY = null;
    let text = '';
    for (const item of tc.items) {
      const y = Math.round(item.transform[5]);
      if (lastY !== null && Math.abs(y - lastY) > 2) {
        text += '\n';
      } else if (lastY !== null) {
        text += ' ';
      }
      text += item.str;
      lastY = y;
    }
    pages.push(text);
  }

  // Find the Gunshot Victims page
  const targetIdx = findGunShotVictimsPage(pages);
  if (targetIdx === -1) {
    throw new Error('Nashville: could not find "Gunshot Victims" page in PDF');
  }
  console.log('Nashville: found Gunshot Victims page at page', targetIdx + 1);

  // Parse the page
  const pageText = pages[targetIdx];
  const parsed = parseGunShotVictimsPage(pageText);

  // Extract report date from filename
  let asof = null;
  const dateMatch = path.basename(pdfPath).match(/(\d{8})/);
  if (dateMatch) {
    const d = dateMatch[1];
    asof = d.substring(0, 4) + '-' + d.substring(4, 6) + '-' + d.substring(6, 8);
  }

  console.log('Nashville: fatal=' + parsed.fatal.current + ' (prior=' + parsed.fatal.prior + ')');
  console.log('Nashville: nonFatal=' + parsed.nonFatal.current + ' (prior=' + parsed.nonFatal.prior + ')');

  const ytd = parsed.fatal.current + parsed.nonFatal.current;
  const prior = parsed.fatal.prior + parsed.nonFatal.prior;

  console.log('Nashville: ytd=' + ytd + ' prior=' + prior + ' asof=' + asof);
  if (ytd === 0 && prior === 0) throw new Error('Nashville: parsed all zeros');

  return { ytd, prior, asof };
}

// ── Nashville PDF parser helpers ──

function findGunShotVictimsPage(pages) {
  // Search expected page 146 first, then nearby, then full scan
  const expected = 145; // 0-indexed
  const searchOrder = [expected];
  for (let offset = 1; offset <= 15; offset++) {
    searchOrder.push(expected + offset);
    searchOrder.push(expected - offset);
  }
  for (let i = 0; i < pages.length; i++) {
    if (!searchOrder.includes(i)) searchOrder.push(i);
  }

  for (const idx of searchOrder) {
    if (idx < 0 || idx >= pages.length) continue;
    const text = (pages[idx] || '').toUpperCase();
    if (text.includes('GUNSHOT VICTIMS') &&
        text.includes('COUNTY') &&
        text.includes('GUNSHOT HOMICIDE') &&
        text.includes('GUNSHOT INJURY')) {
      return idx;
    }
  }
  return -1;
}

/**
 * Parse the Gunshot Victims page.
 * Uses group-based triplet validation: finds sequences of (prior, current, change)
 * where change = current - prior. The 3rd such group in each row is the YTD data.
 * Percentage values naturally don't form valid triplets, so they're skipped.
 */
function parseGunShotVictimsPage(pageText) {
  const lines = pageText.split('\n').map(function(l) { return l.trim(); }).filter(function(l) { return l.length > 0; });

  const result = {
    fatal:    { current: null, prior: null, change: null },
    nonFatal: { current: null, prior: null, change: null },
    propertyDamage: { current: null, prior: null, change: null },
  };

  // Find County section (totals)
  let countyStart = -1;
  for (let i = 0; i < lines.length; i++) {
    if (/\bCounty\b/i.test(lines[i])) { countyStart = i; break; }
  }
  if (countyStart === -1) {
    console.log('Nashville: WARNING - County row not found');
    return result;
  }

  for (let i = countyStart; i < Math.min(countyStart + 8, lines.length); i++) {
    const line = lines[i];
    const upper = line.toUpperCase();
    if (/^(Information summarized|Sourced from)/i.test(line)) break;

    const nums = nashvilleExtractNumbers(line);
    const groups = nashvilleFindValidGroups(nums);

    if (upper.includes('GUNSHOT HOMICIDE') && groups.length >= 3) {
      result.fatal = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };
    } else if (upper.includes('GUNSHOT INJURY') && !upper.includes('HOMICIDE') && groups.length >= 3) {
      result.nonFatal = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };
    } else if (upper.includes('PROPERTY DAMAGE') && groups.length >= 3) {
      result.propertyDamage = { prior: groups[2].prior, current: groups[2].current, change: groups[2].change };
    }
  }

  return result;
}

function nashvilleExtractNumbers(text) {
  var results = [];
  var regex = /-?\d[\d,]*\.?\d*/g;
  var match;
  while ((match = regex.exec(text)) !== null) {
    var val = parseFloat(match[0].replace(/,/g, ''));
    if (!isNaN(val)) results.push(val);
  }
  return results;
}

/**
 * Find valid (prior, current, change) triplets where change = current - prior.
 * Percentage values naturally don't form valid triplets, so they're skipped.
 */
function nashvilleFindValidGroups(nums) {
  var groups = [];
  var i = 0;
  while (i <= nums.length - 3) {
    var v1 = nums[i], v2 = nums[i + 1], v3 = nums[i + 2];
    if (v3 === v2 - v1) {
      groups.push({ prior: v1, current: v2, change: v3 });
      i += 3;
    } else {
      i++;
    }
  }
  return groups;
}


// ─── Decatur, IL (PowerDMS — DPD Shootings Hits PDF) ─────────────────────────
// Document 2060598 is updated in-place by DPD Crime Analysis Unit.
// The PDF has a clean text layer: each month row contains
//   MONTH ATT2021..ATT2026 HIT2021..HIT2026 TOTAL_COLS...
// We sum HIT2026 across all months with data for YTD, and the same months'
// HIT2025 values for the prior-year comparison.
// Strategy: Playwright loads the PowerDMS public viewer, clicks the Download
// button, intercepts the download event, then parses with pdfjs.
async function fetchDecatur() {
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ acceptDownloads: true });
  const page = await context.newPage();

  let pdfBuffer = null;

  try {
    console.log('Decatur: loading PowerDMS viewer...');
    await page.goto('https://public.powerdms.com/Dec8366/tree/documents/2060598', {
      waitUntil: 'networkidle',
      timeout: 30000
    });

    // Try to find and click the Download button
    const [download] = await Promise.all([
      page.waitForEvent('download', { timeout: 30000 }),
      page.click('button:has-text("Download"), a:has-text("Download"), [aria-label*="Download"], [title*="Download"]')
    ]);

    const downloadPath = await download.path();
    if (!downloadPath) throw new Error('Decatur: download path null');
    pdfBuffer = require('fs').readFileSync(downloadPath);
    console.log('Decatur: downloaded PDF (' + (pdfBuffer.length / 1024).toFixed(0) + ' KB)');
  } finally {
    await browser.close();
  }

  if (!pdfBuffer || pdfBuffer[0] !== 0x25) throw new Error('Decatur: invalid PDF');

  // Parse PDF tokens from page 1
  const tokens = await extractPdfTokens(pdfBuffer, 1);

  // Month names we expect
  const MONTHS = ['JAN','FEB','MAR','APR','MAY','JUNE','JULY','AUG','SEPT','OCT','NOV','DEC'];

  // Each month row (after the label) has 6 ATTEMPTS + 6 HITS + many TOTAL cols.
  // HIT2025 = token index 5 (0-based after month label), HIT2026 = index 6 ... wait:
  // Actually: ATT2021 ATT2022 ATT2023 ATT2024 ATT2025 ATT2026 HIT2021 HIT2022 HIT2023 HIT2024 HIT2025 HIT2026
  // So after month label: [0..5] = attempts, [6..11] = hits. HIT2025=idx 10, HIT2026=idx 11.
  const HIT2025_IDX = 10;
  const HIT2026_IDX = 11;

  let ytd2026 = 0;
  let ytd2025 = 0;
  let latestMonth = null;
  let i = 0;

  while (i < tokens.length) {
    const tok = tokens[i].toUpperCase().trim();
    if (MONTHS.includes(tok)) {
      // Collect up to 26 numeric tokens for this row
      const nums = [];
      let j = i + 1;
      while (nums.length < 26 && j < tokens.length) {
        const n = parseInt(tokens[j]);
        if (!isNaN(n)) nums.push(n);
        else if (MONTHS.includes(tokens[j].toUpperCase()) || tokens[j].toUpperCase() === 'TOTAL') break;
        j++;
      }
      // Full row with 2026 data: 6 ATT + 6 HIT + 12 TOTAL = 24 nums
      // Row without 2026 data:   5 ATT + 5 HIT + 12 TOTAL = 22 nums
      // Only process rows where 2026 is present (24+ nums)
      if (nums.length >= 24) {
        ytd2026 += nums[11]; // HIT2026
        ytd2025 += nums[10]; // HIT2025
        latestMonth = tok;
      }
      i = j;
    } else {
      i++;
    }
  }

  // Build asof from latest month found + current year
  const MONTH_MAP = {JAN:'01',FEB:'02',MAR:'03',APR:'04',MAY:'05',JUNE:'06',
    JULY:'07',AUG:'08',SEPT:'09',OCT:'10',NOV:'11',DEC:'12'};
  const MONTH_LAST = {JAN:'31',FEB:'28',MAR:'31',APR:'30',MAY:'31',JUNE:'30',
    JULY:'31',AUG:'31',SEPT:'30',OCT:'31',NOV:'30',DEC:'31'};
  const year = new Date().getFullYear();
  const asof = latestMonth
    ? year + '-' + MONTH_MAP[latestMonth] + '-' + MONTH_LAST[latestMonth]
    : null;

  console.log('Decatur: ytd=' + ytd2026 + ' prior=' + ytd2025 + ' asof=' + asof);
  return { ytd: ytd2026, prior: ytd2025, asof: asof };
}
