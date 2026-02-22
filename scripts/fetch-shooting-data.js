/**
 * fetch-shooting-data.js
 * Runs via GitHub Actions to fetch blocked city data server-side
 * and write results to data/manual-auto.json
 */

const https = require('https');
const http  = require('http');
const fs    = require('fs');
const path  = require('path');

// ─── Helpers ─────────────────────────────────────────────────────────────────

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

function todayStr() {
  const d = new Date();
  return d.getFullYear() + '-' +
    String(d.getMonth()+1).padStart(2,'0') + '-' +
    String(d.getDate()).padStart(2,'0');
}

// ─── PDF parsing with pdf.js via pdfjs-dist ───────────────────────────────────

async function getPdfjsLib() {
  let pdfjsLib;
  try { pdfjsLib = require("pdfjs-dist/legacy/build/pdf.js"); } catch(e) {
    try { pdfjsLib = require("pdfjs-dist"); } catch(e2) { pdfjsLib = require("pdfjs-dist/build/pdf.js"); }
  }
  if (pdfjsLib.GlobalWorkerOptions) pdfjsLib.GlobalWorkerOptions.workerSrc = false;
  return pdfjsLib;
}

async function extractPdfTokens(buffer) {
  const pdfjsLib = await getPdfjsLib();
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(1);
  const tc = await page.getTextContent();
  return tc.items.map(i => i.str.trim()).filter(s => s.length > 0);
}

// For PDFs with character-level tokens, group by Y position then X position
// to reconstruct words and rows
async function extractPdfRows(buffer) {
  const pdfjsLib = await getPdfjsLib();
  const pdf = await pdfjsLib.getDocument({ data: new Uint8Array(buffer) }).promise;
  const page = await pdf.getPage(1);
  const tc = await page.getTextContent();

  // Group items by rounded Y coordinate (row)
  const rowMap = {};
  for (const item of tc.items) {
    if (!item.str) continue;
    const y = Math.round(item.transform[5]);
    if (!rowMap[y]) rowMap[y] = [];
    rowMap[y].push({ x: item.transform[4], str: item.str });
  }

  // Sort rows top-to-bottom (descending Y in PDF coords), items left-to-right
  const rows = Object.keys(rowMap)
    .sort((a, b) => b - a)
    .map(y => {
      const items = rowMap[y].sort((a, b) => a.x - b.x);
      // Join items that are close together into words
      let text = '';
      let lastX = null;
      let lastW = 0;
      for (const item of items) {
        if (lastX !== null && item.x - (lastX + lastW) > 3) {
          text += ' ';
        }
        text += item.str;
        lastX = item.x;
        lastW = item.str.length * 6; // rough char width estimate
      }
      return text.trim();
    })
    .filter(r => r.length > 0);

  return rows;
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

  const rows = await extractPdfRows(resp.body);
  console.log('Detroit rows:', rows.slice(0, 20));

  // Find date row: "Thursday,February19,2026" (letters and digits joined, no spaces)
  let asof = null;
  for (const row of rows) {
    // Match: weekday + month-name + digits + year (all possibly without spaces)
    const dateMatch = row.match(/(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\w*,?\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*(\d{1,2}),?\s*(\d{4})/i);
    if (dateMatch) {
      const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
      const mo = months[dateMatch[1].toLowerCase()];
      if (mo) {
        asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;
        break;
      }
    }
  }
  // Fallback: look for M/D/YYYY pattern in any row
  if (!asof) {
    for (const row of rows) {
      const m = row.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
      if (m) { asof = `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`; break; }
    }
  }
  console.log('Detroit asof:', asof);

  // Find Non-Fatal Shooting row (words may be joined without space)
  const nfsRow = rows.find(r => r.match(/Non-?Fatal\s*Shooting/i));
  if (!nfsRow) throw new Error('Non-Fatal Shooting row not found. Rows: ' + rows.join(' | '));

  // Extract numbers from the row: [priorDay, prior7Days, ytd26, ytd25, change, ...]
  const nums = [...nfsRow.matchAll(/-?[\d,]+/g)]
    .map(m => parseInt(m[0].replace(/,/g,'')))
    .filter(n => !isNaN(n));

  console.log('Detroit NFS row:', nfsRow, 'nums:', nums);
  if (nums.length < 4) throw new Error(`Not enough numbers in NFS row: ${nums.join(',')} | row: ${nfsRow}`);
  return { ytd: nums[2], prior: nums[3], asof };
}

// ─── Durham ───────────────────────────────────────────────────────────────────

async function fetchDurham() {
  // Scrape archive page to find latest ADID
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

  // Date: "Year-to-Date through February 14, 2026"
  const dateMatch = text.match(/through\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})/i);
  if (!dateMatch) throw new Error('Date not found. Tokens: ' + tokens.slice(0,30).join('|'));
  const months = {january:1,february:2,march:3,april:4,may:5,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
  const mo = months[dateMatch[1].toLowerCase()];
  const asof = `${dateMatch[3]}-${String(mo).padStart(2,'0')}-${String(parseInt(dateMatch[2])).padStart(2,'0')}`;

  // All numbers in the chart appear left-to-right across all groups:
  // Shootings: 84(2024) 77(2025) 53(2026)
  // Persons Shot: 16(2024) 21(2025) 18(2026)
  // Fatal: 4(2024) 3(2025) 6(2026)
  // Non-Fatal: 12(2024) 18(2025) 12(2026)
  // We want Fatal[2026] + NonFatal[2026] = indices [8] and [11] in the number sequence
  // And Fatal[2025] + NonFatal[2025] = indices [7] and [10]

  const allNums = tokens
    .filter(t => /^\d+$/.test(t) && parseInt(t) >= 1 && parseInt(t) <= 500)
    .map(Number);

  console.log('Durham all nums:', allNums);

  // Need at least 12 numbers (4 groups × 3 years)
  if (allNums.length < 12) throw new Error('Not enough chart numbers: ' + allNums.join(','));

  // Numbers come out in column order (by group across years):
  // [Shoot24, PShot24, Fatal24, NF24, Shoot25, PShot25, Fatal25, NF25, Shoot26, PShot26, Fatal26, NF26]
  const chartNums = allNums.slice(0, 12);
  console.log('Durham chart nums:', chartNums);
  const ytd   = chartNums[10] + chartNums[11]; // Fatal2026 + NonFatal2026
  const prior = chartNums[6]  + chartNums[7];  // Fatal2025 + NonFatal2025

  return { ytd, prior, asof, adid: latestAdid };
}

// ─── Wilmington ───────────────────────────────────────────────────────────────

async function fetchWilmington() {
  const pageUrl = 'https://www.wilmingtonde.gov/government/public-safety/wilmington-police-department/compstat-reports';
  console.log('Wilmington page URL:', pageUrl);
  const pageResp = await fetchUrl(pageUrl);
  if (pageResp.status !== 200) throw new Error(`Wilmington page HTTP ${pageResp.status}`);

  const html = pageResp.body.toString('utf8');

  // Debug: show snippet of HTML to find PDF link pattern
  console.log('Wilmington HTML snippet:', html.slice(0, 2000));

  // Find PDF link - look for showpublisheddocument links
  const pdfMatch = html.match(/href="(\/home\/showpublisheddocument\/[^"]+)"/i)
    || html.match(/href="(https?:\/\/[^"]*showpublisheddocument[^"]+)"/i)
    || html.match(/(https?:\/\/[^"'\s]*\.pdf[^"'\s]*)/i);
  if (!pdfMatch) throw new Error('No PDF link found on Wilmington page. HTML length: ' + html.length + ' snippet: ' + html.slice(0, 800));

  const pdfUrl = 'https://www.wilmingtonde.gov' + pdfMatch[1];
  console.log('Wilmington PDF URL:', pdfUrl);

  const pdfResp = await fetchUrl(pdfUrl);
  if (pdfResp.status !== 200) throw new Error(`Wilmington PDF HTTP ${pdfResp.status}`);

  const rows = await extractPdfRows(pdfResp.body);
  console.log('Wilmington rows:', rows.slice(0, 25));

  // Extract date from "Report Covering the Week MM/DD/YY Through MM/DD/YY"
  // or "02/09/26 Through 02/15/26"
  let asof = null;
  for (const row of rows) {
    const m = row.match(/through\s+(\d{1,2})\/(\d{1,2})\/(\d{2,4})/i);
    if (m) {
      const yr = m[3].length === 2 ? '20' + m[3] : m[3];
      asof = `${yr}-${String(m[1]).padStart(2,'0')}-${String(m[2]).padStart(2,'0')}`;
      break;
    }
  }

  // Find Shooting Victims row
  // Row format: "Shooting Victims  0  0  *  4  3  33%  6  3  100%  ..."
  const svRow = rows.find(r => r.match(/Shooting\s*Victims/i));
  if (!svRow) throw new Error('Shooting Victims row not found. Rows: ' + rows.join(' | '));
  console.log('Wilmington SV row:', svRow);

  // Extract numbers: columns are [2026_7d, 2025_7d, %chg, 2026_28d, 2025_28d, %chg, 2026_ytd, 2025_ytd, ...]
  const nums = [...svRow.matchAll(/-?[\d,]+/g)]
    .map(m => parseInt(m[0].replace(/,/g, '')))
    .filter(n => !isNaN(n));
  console.log('Wilmington SV nums:', nums);

  // YTD columns are at index 6 (2026) and 7 (2025)
  if (nums.length < 8) throw new Error(`Not enough numbers: ${nums.join(',')}`);

  return { ytd: nums[6], prior: nums[7], asof };
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

  // Wilmington
  try {
    console.log('\n--- Fetching Wilmington ---');
    results.wilmington = { ...(await fetchWilmington()), fetchedAt, ok: true };
    console.log('Wilmington:', results.wilmington);
  } catch (e) {
    console.error('Wilmington error:', e.message);
    results.wilmington = { ok: false, error: e.message, fetchedAt };
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
