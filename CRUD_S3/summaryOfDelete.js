const fs = require('fs');
const path = require('path');

// Edit this path to point to the CSV you want to summarize
// The CSV must have a header: s3_uri,uploaded_at
// and rows like: s3://<bucket>/<key>,<ISO date>
const CSV_PATH = path.resolve(__dirname, 'candidates-chunked-rules-repository-2025-12-20T15-11-18-159Z.csv');

function parseS3Uri(uri) {
  const m = String(uri).trim().match(/^s3:\/\/([^/]+)\/?(.*)$/);
  if (!m) return null;
  return { bucket: m[1], key: m[2] || '' };
}

function summarizeCsv(csvFilePath) {
  if (!fs.existsSync(csvFilePath)) {
    throw new Error(`CSV not found: ${csvFilePath}`);
  }
  const text = fs.readFileSync(csvFilePath, 'utf8');
  const lines = text.split(/\r?\n/).filter(Boolean);
  if (lines.length <= 1) {
    throw new Error('CSV appears empty or missing data rows');
  }

  // Skip header
  const dataLines = lines.slice(1);

  const bucketTotals = new Map(); // bucket -> count
  const folderTotals = new Map(); // `${bucket}/path/to/folder` -> count

  for (const line of dataLines) {
    // Robust split: take first comma as separator between s3_uri and uploaded_at
    const commaIdx = line.indexOf(',');
    if (commaIdx < 0) continue;
    let s3uri = line.slice(0, commaIdx).trim();
    // remove surrounding quotes if present
    if (s3uri.startsWith('"') && s3uri.endsWith('"')) s3uri = s3uri.slice(1, -1);

    const parsed = parseS3Uri(s3uri);
    if (!parsed) continue;

    const { bucket, key } = parsed;
    bucketTotals.set(bucket, (bucketTotals.get(bucket) || 0) + 1);

    if (key) {
      // Count every directory level present in the key
      const segments = key.split('/').filter(Boolean);
      // If key ends with '/', treat all segments as directories; otherwise exclude last (file)
      const isDirKey = key.endsWith('/');
      const dirSegments = isDirKey ? segments : segments.slice(0, Math.max(segments.length - 1, 0));

      if (dirSegments.length > 0) {
        let prefix = '';
        for (const seg of dirSegments) {
          prefix = prefix ? `${prefix}/${seg}` : seg;
          const k = `${bucket}/${prefix}`;
          folderTotals.set(k, (folderTotals.get(k) || 0) + 1);
        }
      }
    }
  }

  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const summaryPath = path.resolve(__dirname, `summary-${ts}.txt`);

  const out = [];
  // Print bucket totals first
  for (const [bucket, count] of bucketTotals.entries()) {
    out.push(`s3://${bucket}: ${count} objects`);
  }
  // Then folder totals (sorted for readability)
  const sortedFolders = Array.from(folderTotals.entries()).sort((a, b) => a[0].localeCompare(b[0]));
  for (const [folderKey, count] of sortedFolders) {
    out.push(`s3://${folderKey}: ${count} objects`);
  }

  fs.writeFileSync(summaryPath, out.join('\n') + '\n');
  console.log(`Summary written to: ${summaryPath}`);
}

function main() {
  try {
    summarizeCsv(CSV_PATH);
  } catch (err) {
    console.error('Error:', err.message || err);
    console.log('Edit CSV_PATH in this file to point to your candidates or deletions CSV.');
  }
}

main();
