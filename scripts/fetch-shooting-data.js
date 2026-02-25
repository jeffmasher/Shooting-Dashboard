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
  await pg1.render({ canvasContext: ctx, viewport }).promise;
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
  if (!m2026) throw new Error('Could not parse Durham chart values. Response: ' + responseText);

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
  // The index page is blocked by Akamai for headless browsers.
  // Strategy: try direct fetchUrl on the index page (sometimes works without UA spoofing),
  // then try the CivicPlus document API, then scan known doc IDs.

  const listUrl = 'https://www.wilmingtonde.gov/government/public-safety/wilmington-police-department/compstat-reports';
  let html = null;

  // Try 1: plain HTTP fetch (bypasses some bot detectors that target browser TLS fingerprints)
  console.log('Wilmington: trying direct fetch...');
  const direct = await fetchUrl(listUrl).catch(() => null);
  if (direct && direct.status === 200) {
    html = direct.body.toString('utf8');
    console.log('Wilmington: direct fetch OK, length:', html.length);
  }

  // Try 2: CivicPlus document list partial URL
  if (!html) {
    for (const apiPath of [
      '/Home/Components/DocumentList/DocumentList/211/30',
      '/government/public-safety/wilmington-police-department/compstat-reports?format=rss',
    ]) {
      const apiResp = await fetchUrl('https://www.wilmingtonde.gov' + apiPath).catch(() => null);
      if (apiResp && apiResp.status === 200) {
        html = apiResp.body.toString('utf8');
        console.log('Wilmington: API fetch OK at', apiPath, 'length:', html.length);
        break;
      }
    }
  }

  // Try to find PDF path from HTML if we got any
  let pdfPath = null;
  if (html && html.includes('showpublisheddocument')) {
    // Anchor text contains "CompStat"
    const anchorMatch = html.match(/href="(\/home\/showpublisheddocument\/[^"]+)"[^>]*>[^<]*[Cc]omp[Ss]tat[^<]*/i);
    if (anchorMatch) { pdfPath = anchorMatch[1]; console.log('Wilmington: PDF via anchor:', pdfPath); }

    // Any showpublisheddocument link, highest ID = most recent
    if (!pdfPath) {
      const allLinks = [...html.matchAll(/href="(\/home\/showpublisheddocument\/(\d+)\/[^"]+)"/gi)];
      if (allLinks.length > 0) {
        allLinks.sort((a, b) => parseInt(b[2]) - parseInt(a[2]));
        pdfPath = allLinks[0][1];
        console.log('Wilmington: PDF via highest doc ID in HTML:', pdfPath);
      }
    }
  }

  // If we found a path, fetch and parse it
  if (pdfPath) {
    const pdfUrl = 'https://www.wilmingtonde.gov' + pdfPath;
    console.log('Wilmington: fetching PDF:', pdfUrl);
    const pdfResp = await fetchUrl(pdfUrl);
    if (pdfResp.status === 200) return await parseWilmingtonPdf(pdfResp.body, pdfUrl);
    console.log('Wilmington: PDF fetch returned', pdfResp.status);
  }

  // Fallback: scan doc IDs near the known working one (8310 = week of 2026-02-19)
  // CivicPlus showpublisheddocument/{id} without ticks should redirect to latest version.
  // Try both with and without following redirects.
  console.log('Wilmington: scanning doc IDs...');
  // Scan wider range: 8310 +/- 20
  const scanIds = [];
  for (let id = 8330; id >= 8290; id--) scanIds.push(id);

  for (const docId of scanIds) {
    // Try showpublisheddocument without ticks (CivicPlus canonical URL)
    const url1 = 'https://www.wilmingtonde.gov/home/showpublisheddocument/' + docId;
    const r1 = await fetchUrl(url1, 10000).catch(() => null);
    if (r1) {
      console.log('Wilmington: docId', docId, 'status:', r1.status, 'bytes:', r1.body.length, 'magic:', r1.body.slice(0,4).toString('hex'));
      if (r1.status === 200 && r1.body[0] === 0x25 && r1.body[1] === 0x50) {
        console.log('Wilmington: found PDF at', url1);
        return await parseWilmingtonPdf(r1.body, url1);
      }
    }
  }

  throw new Error('Wilmington: could not find PDF via index page or doc ID scan (tried 8290-8330)');
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
    safe('Portland',   fetchPortland,   120000),
    safe('Buffalo',    fetchBuffalo,    120000),
    safe('Nashville',  fetchNashville,  180000),
    safe('Wilmington', fetchWilmington, 60000),
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
  const browser = await chromium.launch({ headless: true });
  const page    = await browser.newPage();
  await page.setViewportSize({ width: 1536, height: 1200 });
  page.setDefaultTimeout(30000);

  const yr = new Date().getFullYear();
  const embedUrl = 'https://public.tableau.com/views/GunViolenceTrendsReport/YeartoDateRollingYearStatistics?:showVizHome=no&:embed=true';
  console.log('Portland: loading Gun Violence Trends Report YTD sheet...');
  await page.goto(embedUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });

  // Wait for table to render — poll for text content stabilizing
  let bodyText = '';
  for (var w = 0; w < 10; w++) {
    await page.waitForTimeout(3000);
    bodyText = await page.evaluate(function() { return document.body.innerText; });
    if (bodyText.includes('Total Shooting Incidents') && bodyText.length > 2000) break;
    console.log('Portland: waiting for render... attempt', (w+1), 'len:', bodyText.length);
  }
  console.log('Portland: body length after wait:', bodyText.length);

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

  const claudeData = await new Promise((resolve, reject) => {
    const body = JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 128,
      messages: [{ role: 'user', content: [
        { type: 'image', source: { type: 'base64', media_type: 'image/png', data: base64Image } },
        { type: 'text', text: promptText }
      ]}]
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
  console.log('Portland vision response:', visionText);

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


// ─── Nashville (Tableau REST API - direct CSV export) ────────────────────────
// Tableau Server exposes a CSV export endpoint: /views/ViewName/SheetName.csv
// We need a valid session first (load the viz page via Playwright to get cookies),
// then hit the CSV endpoint with those cookies. No toolbar interaction needed.
// Full dataset → count by year client-side.

async function fetchNashville() {
  function countVictims(csvText, targetYear, mmddCutoff) {
    var lines = csvText.split('\n').map(l => l.trim()).filter(Boolean);
    var header = lines[0].replace(/^\uFEFF/, '');
    var sep = header.includes('\t') ? '\t' : ',';
    var cols = header.split(sep).map(c => c.replace(/"/g,'').trim());
    var rptdtIdx = cols.indexOf('I Rptdt');
    if (rptdtIdx < 0) throw new Error('I Rptdt not found. Header: ' + header.substring(0,100));
    var count = 0, maxDate = null, lastAsof = null;
    for (var i = 1; i < lines.length; i++) {
      var parts = lines[i].split(sep);
      if (parts.length <= rptdtIdx) continue;
      var rptdt = parts[rptdtIdx].replace(/"/g,'').trim();
      if (!rptdt) continue;
      var dp = rptdt.split(' ')[0].split('/');
      if (dp.length < 3) continue;
      var rowYr = parseInt(dp[2]);
      var rowMm = dp[0].padStart(2,'0');
      var rowDd = dp[1].padStart(2,'0');
      if (rowYr !== targetYear) continue;
      if (mmddCutoff && (rowMm + '/' + rowDd) > mmddCutoff) continue;
      count++;
      var d = new Date(rowYr, parseInt(dp[0])-1, parseInt(dp[1]));
      if (!maxDate || d > maxDate) { maxDate = d; lastAsof = rowYr + '-' + rowMm + '-' + rowDd; }
    }
    return { count, asof: lastAsof };
  }

  const now = new Date();
  const curYr = now.getFullYear();
  const priorYr = curYr - 1;
  const mmdd = (now.getMonth()+1).toString().padStart(2,'0') + '/' + now.getDate().toString().padStart(2,'0');

  // Step 1: Load the viz page via Playwright to establish a Tableau session cookie
  const { chromium } = require('playwright');
  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const page = await browser.newPage();
  page.setDefaultTimeout(30000);

  console.log('Nashville: loading viz page to get session cookie...');

  // Intercept network requests to discover the actual CSV download URL pattern
  const csvRequests = [];
  page.on('request', req => {
    const url = req.url();
    if (url.includes('.csv') || url.includes('crosstab') || url.includes('download')) {
      csvRequests.push(url);
    }
  });

  await page.goto(
    'https://policepublicdata.nashville.gov/t/Police/views/GunshotInjury/GunshotInjuries?:embed=y&:showVizHome=no',
    { waitUntil: 'domcontentloaded', timeout: 60000 }
  );
  await page.waitForTimeout(5000);

  // Step 2: Extract cookies from the browser context
  const cookies = await page.context().cookies();
  await browser.close();

  const cookieHeader = cookies.map(c => c.name + '=' + c.value).join('; ');
  console.log('Nashville: got', cookies.length, 'cookies:', cookies.map(c=>c.name).join(', '));
  if (csvRequests.length > 0) console.log('Nashville: observed CSV/download requests:', csvRequests);

  // Step 3: Hit the CSV export endpoint directly
  // Tableau Server CSV export: /t/{site}/views/{workbook}/{sheet}.csv
  // :refresh=y forces a fresh query; :format=csv is implicit with .csv extension
  // Try multiple CSV endpoint patterns — Tableau sheet name may vary
  const csvPaths = [
    '/t/Police/views/GunshotInjury/Map.csv?:embed=y&:showVizHome=no',
    '/t/Police/views/GunshotInjury/GunshotInjuries.csv?:embed=y&:showVizHome=no',
    '/t/Police/views/GunshotInjury/GunshotInjuriesMap.csv?:embed=y&:showVizHome=no',
    '/t/Police/views/GunshotInjury/Map.csv?:refresh=y&:embed=y',
  ];

  let csvResp = null;
  for (const csvPath of csvPaths) {
    console.log('Nashville: trying CSV path:', csvPath);
    const resp = await new Promise((resolve, reject) => {
      const req = require('https').request({
        hostname: 'policepublicdata.nashville.gov',
        path: csvPath,
        method: 'GET',
        headers: {
          'Cookie': cookieHeader,
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Accept': 'text/csv,text/plain,*/*',
          'Referer': 'https://policepublicdata.nashville.gov/t/Police/views/GunshotInjury/GunshotInjuries',
          'X-Requested-With': 'XMLHttpRequest',
        }
      }, (res) => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode, body: Buffer.concat(chunks), path: csvPath }));
        res.on('error', reject);
      });
      req.on('error', reject);
      req.end();
    });
    console.log('Nashville: status', resp.status, 'for', csvPath);
    if (resp.status === 200) {
      const preview = resp.body.toString('utf8').substring(0, 100);
      if (preview.includes(',') || preview.includes('\t')) {
        csvResp = resp;
        console.log('Nashville: found working CSV at', csvPath);
        break;
      }
      console.log('Nashville: 200 but not CSV, preview:', preview.substring(0,80));
    }
  }

  if (!csvResp) {
    // Log all cookies for diagnosis
    console.log('Nashville: cookies:', cookies.map(c => c.name + '=' + c.value.substring(0,20)).join('; '));
    throw new Error('Nashville: no working CSV endpoint found. Tried: ' + csvPaths.join(', '));
  }

  const buf = csvResp.body;
  const csvData = (buf[0] === 0xFF && buf[1] === 0xFE) ? buf.toString('utf16le') : buf.toString('utf8');
  console.log('Nashville: CSV bytes:', buf.length, 'preview:', csvData.substring(0, 200));

  if (!csvData.includes('I Rptdt')) {
    throw new Error('Nashville: wrong CSV format (no I Rptdt column). Preview: ' + csvData.substring(0, 200));
  }

  // Step 4: Count victims by year
  const rCurr  = countVictims(csvData, curYr,   null);
  const rPrior = countVictims(csvData, priorYr, mmdd);
  console.log('Nashville: ytd=' + rCurr.count + ' prior=' + rPrior.count + ' asof=' + rCurr.asof);

  if (rCurr.count === 0 && rPrior.count === 0) {
    throw new Error('Nashville: both counts are zero — CSV may be empty or wrong sheet');
  }

  return { ytd: rCurr.count, prior: rPrior.count, asof: rCurr.asof };
}
