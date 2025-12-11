const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Load env from project root
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

// AWS config
const accessKeyId = process.env.AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_KEY;
const region = process.env.AWS_REGION || 'us-east-1';
// Bucket overridden by user instruction
const BUCKET = process.env.AWS_BUCKET_NAME || 'dummy-bucket12340901';

const s3 = new S3Client({
  region,
  credentials: { accessKeyId, secretAccessKey },
});

// Prefix drive for local files
const LOCAL_PREFIX = process.env.LOCAL_PREFIX || "F:\\02-12-2025"; // e.g., F:\

// Helpers
function csvIter(filePath) {
  const input = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input, crlfDelay: Infinity });
  let isHeader = true;
  return {
    [Symbol.asyncIterator]: async function* () {
      for await (const line of rl) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        if (isHeader) { isHeader = false; continue; }
        const idx = trimmed.indexOf(',');
        if (idx === -1) continue;
        const zohoPath = trimmed.slice(0, idx).trim();
        const s3Path = trimmed.slice(idx + 1).trim();
        yield { zohoPath, s3Path };
      }
    }
  };
}

function buildLocalFolder(zohoPath) {
  // zohoPath like: \BANKING REGULATIONS\UAE
  const norm = zohoPath.replace(/\\+/g, '\\');
  // Ensure leading backslash is not duplicated
  const rel = norm.startsWith('\\') ? norm : ('\\' + norm);
  // Combine with prefix (drive root)
  return path.join(LOCAL_PREFIX, rel);
}

function findPdfFilesRecursive(dir) {
  const results = [];
  const stack = [dir];
  while (stack.length) {
    const current = stack.pop();
    let entries;
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch (e) {
      // skip inaccessible directories
      continue;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
      } else {
        if (entry.name.toLowerCase().endsWith('.pdf')) {
          results.push(full);
        }
      }
    }
  }
  return results;
}

async function uploadPdf(localFile, s3KeyBase) {
  const fileName = path.basename(localFile);
  const key = path.posix.join(s3KeyBase.replace(/\\/g, '/'), fileName);
  const body = fs.readFileSync(localFile);
  const cmd = new PutObjectCommand({
    Bucket: BUCKET,
    Key: key,
    Body: body,
    ContentType: 'application/pdf',
  });
  await s3.send(cmd);
  return `s3://${BUCKET}/${key}`;
}

async function run() {
  const csvPath = path.resolve(__dirname, 'cleaned.csv');
  if (!fs.existsSync(csvPath)) {
    console.error('cleaned.csv not found at', csvPath);
    process.exit(1);
  }

  console.log('Starting local-to-S3 upload...');
  console.log('Bucket:', BUCKET);
  console.log('Local prefix:', LOCAL_PREFIX);

  let totalFolders = 0;
  let totalPdfs = 0;
  let uploaded = 0;
  let failed = 0;

  for await (const { zohoPath, s3Path } of csvIter(csvPath)) {
    totalFolders++;
    const localFolder = buildLocalFolder(zohoPath);
    if (!fs.existsSync(localFolder)) {
      console.warn(`Local folder not found for '${zohoPath}': ${localFolder}`);
      continue;
    }
    const pdfs = findPdfFilesRecursive(localFolder);
    totalPdfs += pdfs.length;
    if (pdfs.length === 0) {
      console.warn(`No PDFs found under ${localFolder}`);
      continue;
    }
    for (const pdf of pdfs) {
      try {
        const location = await uploadPdf(pdf, s3Path);
        uploaded++;
        console.log(`Uploaded: ${pdf} -> ${location}`);
      } catch (e) {
        failed++;
        console.error(`Failed upload: ${pdf} -> ${s3Path} :: ${e.message}`);
      }
    }
  }

  console.log('Summary:');
  console.log(`- Folders processed: ${totalFolders}`);
  console.log(`- PDFs discovered: ${totalPdfs}`);
  console.log(`- Uploaded: ${uploaded}`);
  console.log(`- Failed: ${failed}`);
}

run().catch(err => {
  console.error('Fatal:', err.message);
  if (err.stack) console.error(err.stack);
  process.exit(1);
});
