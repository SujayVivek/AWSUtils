const fs = require('fs');
const path = require('path');

// Paths
const INPUT = path.resolve(__dirname, 'input.csv');
const OUTPUT = path.resolve(__dirname, 'cleaned.csv');

function cleanZohoPath(p) {
  if (!p) return '';
  // Normalize to backslashes and split
  const parts = String(p).replace(/\//g, '\\').split('\\').filter(s => s.length > 0);
  // Remove first 3 segments (e.g., Z:, General, date)
  const kept = parts.slice(3);
  return kept.length ? ('\\' + kept.join('\\')) : '';
}

function cleanS3Path(p) {
  if (!p) return '';
  // Remove the first segment if it is exactly 's3' (case-insensitive), keep rest
  const parts = String(p).split('/').filter(s => s.length > 0);
  if (parts.length === 0) return '';
  const first = parts[0];
  const rest = (first.toLowerCase() === 's3') ? parts.slice(1) : parts;
  return rest.join('/');
}

function run() {
  if (!fs.existsSync(INPUT)) {
    console.error('input.csv not found at', INPUT);
    process.exit(1);
  }
  const raw = fs.readFileSync(INPUT, 'utf8');
  const lines = raw.split(/\r?\n/).filter(l => l.length > 0);
  if (lines.length === 0) {
    console.error('input.csv is empty');
    process.exit(1);
  }

  const header = lines[0];
  const out = [header]; // keep the original header line

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    const idx = line.indexOf(',');
    if (idx === -1) continue; // skip malformed lines
    const zoho = line.slice(0, idx).trim();
    const s3 = line.slice(idx + 1).trim();

    const newZoho = cleanZohoPath(zoho);
    const newS3 = cleanS3Path(s3);
    out.push(`${newZoho},${newS3}`);
  }

  fs.writeFileSync(OUTPUT, out.join('\n'));
  console.log('✔ Cleaned CSV written to:', OUTPUT);
}

run();
