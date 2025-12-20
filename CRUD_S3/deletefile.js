const path = require('path');
try {
	// Load .env from workspace root (one level up)
	require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
} catch {}

const {
	S3Client,
	ListObjectsV2Command,
	DeleteObjectsCommand,
} = require('@aws-sdk/client-s3');
const fs = require('fs');
const readline = require('readline');

// Region and credentials consistent with other scripts
const region = process.env.AWS_REGION || 'us-east-1';
const accessKeyId = process.env.AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_KEY;

const s3 = new S3Client({
	region,
	credentials: { accessKeyId, secretAccessKey },
});

function normalizePrefix(dir) {
	if (!dir) return undefined;
	let p = String(dir).replace(/\\/g, '/');
	p = p.replace(/^\/+/, '');
	if (p && !p.endsWith('/')) p += '/';
	return p || undefined;
}

function formatS3Uri(bucket, key) {
	return `s3://${bucket}/${key}`;
}

function writeCandidatesLog(bucket, prefix, start, end, candidates) {
	// Timestamped log file per run
	const tsFile = new Date().toISOString().replace(/[:.]/g, '-');
	const logFile = path.resolve(__dirname, `log-${bucket}-${tsFile}.txt`);
	const ts = new Date().toISOString();
	const lines = [];
	lines.push(`\n=== Deletion preview at ${ts} ===`);
	lines.push(`Bucket: ${bucket}`);
	lines.push(`Prefix: ${prefix || '(none)'}`);
	lines.push(`Range: ${start.toISOString()} -> ${end.toISOString()}`);
	lines.push(`Candidates (${candidates.length}):`);
	for (const c of candidates) {
		lines.push(`${c.LastModified}\t${formatS3Uri(bucket, c.Key)}`);
	}
	fs.writeFileSync(logFile, lines.join('\n') + '\n');
	return logFile;
}

function writeCandidatesCsv(bucket, candidates) {
	const ts = new Date().toISOString().replace(/[:.]/g, '-');
	const csvFile = path.resolve(__dirname, `candidates-${bucket}-${ts}.csv`);
	const header = 's3_uri,uploaded_at\n';
	const rows = candidates.map(c => `${formatS3Uri(bucket, c.Key)},${c.LastModified}`).join('\n');
	fs.writeFileSync(csvFile, header + rows + '\n');
	return csvFile;
}

async function promptYesConfirm(message) {
	const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
	const answer = await new Promise(resolve => rl.question(message, resolve));
	rl.close();
	return String(answer).trim().toLowerCase() === 'yes';
}

function parseEnv() {
	const dryRun = process.env.DRY_RUN === 'true';

	const bucket = process.env.AWS_BUCKET_NAME;
	const startDateStr = process.env.START_DATE;
	const endDateStr = process.env.END_DATE;
	const directoryEnv = process.env.DIRECTORY;
	const prefix = normalizePrefix(directoryEnv);

	if (!bucket) {
		throw new Error('Bucket name is required via env AWS_BUCKET_NAME');
	}
	if (!startDateStr || !endDateStr) {
		throw new Error('Start and end dates are required via env START_DATE and END_DATE (YYYY-MM-DD or ISO)');
	}

	const startDate = new Date(startDateStr);
	const endDate = new Date(endDateStr);

	if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
		throw new Error('Invalid date format. Use YYYY-MM-DD or ISO 8601.');
	}
	if (endDate < startDate) {
		throw new Error('END_DATE must be greater than or equal to START_DATE.');
	}

	// Normalize to inclusive boundaries
	const start = new Date(startDate);
	const end = new Date(endDate);
	// Make end inclusive through end of day if only a date string was provided
	if (/^\d{4}-\d{2}-\d{2}$/.test(endDateStr)) {
		end.setHours(23, 59, 59, 999);
	}

	return { bucket, start, end, dryRun, prefix };
}

async function collectKeysInDateRange(bucket, start, end, prefix) {
	let ContinuationToken = undefined;
	const keysToDelete = [];
	let scanned = 0;

	console.log(`Scanning bucket "${bucket}" ${prefix ? `under "${prefix}" ` : ''}for objects between ${start.toISOString()} and ${end.toISOString()}...`);

		do {
			const resp = await s3.send(new ListObjectsV2Command({
				Bucket: bucket,
				ContinuationToken,
				MaxKeys: 2000,
				Prefix: prefix,
			}));

		const contents = resp.Contents || [];
		for (const obj of contents) {
			scanned++;
			const lm = obj.LastModified instanceof Date ? obj.LastModified : new Date(obj.LastModified);
			if (!isNaN(lm.getTime()) && lm >= start && lm <= end) {
				keysToDelete.push({ Key: obj.Key, LastModified: lm.toISOString() });
			}
		}

		if (scanned % 5000 === 0) {
			console.log(`Scanned ${scanned} objects... matched ${keysToDelete.length}`);
		}

		ContinuationToken = resp.NextContinuationToken;
	} while (ContinuationToken);

	console.log(`Finished scanning. Total scanned: ${scanned}. Candidates to delete: ${keysToDelete.length}.`);
	return keysToDelete;
}

async function deleteInBatches(bucket, objects, dryRun, logFilePath) {
	if (objects.length === 0) {
		console.log('No objects to delete in the specified date range.');
		return { deleted: 0, errors: 0 };
	}

	const BATCH_SIZE = 1000; // S3 deleteObjects max
	let deleted = 0;
	let errors = 0;
	// Use the same timestamped log file as preview, or create one if not provided
	const logFile = logFilePath || path.resolve(__dirname, `log-${bucket}-${new Date().toISOString().replace(/[:.]/g, '-')}.txt`);
	const logStream = dryRun ? null : fs.createWriteStream(logFile, { flags: 'a' });
	if (!dryRun) {
		logStream.write(`\n=== Deletion run at ${new Date().toISOString()} ===\n`);
		logStream.write(`Bucket: ${bucket}\n`);
	}

	for (let i = 0; i < objects.length; i += BATCH_SIZE) {
		const batch = objects.slice(i, i + BATCH_SIZE);
		console.log(`Processing batch ${Math.floor(i / BATCH_SIZE) + 1} (${batch.length} objects)...`);

		if (dryRun) {
			// Log keys that would be deleted
			const sample = batch.slice(0, Math.min(5, batch.length)).map(o => o.Key);
			console.log(`DRY RUN: Would delete ${batch.length} objects. Sample:`, sample);
			deleted += batch.length; // count as planned deletions
			continue;
		}

		try {
			const resp = await s3.send(new DeleteObjectsCommand({
				Bucket: bucket,
				Delete: { Objects: batch.map(b => ({ Key: b.Key })), Quiet: true },
			}));

			const succeeded = (resp.Deleted || []).length;
			const failed = (resp.Errors || []).length;
			deleted += succeeded;
			errors += failed;

			if (failed > 0) {
				console.warn('Some deletions failed:', resp.Errors);
			}

			// Write successful deletions to log with LastModified
			if (succeeded > 0 && logStream) {
				// Build a map for quick lookup of LastModified by Key in this batch
				const meta = new Map(batch.map(b => [b.Key, b.LastModified]));
				for (const d of resp.Deleted || []) {
					const key = d.Key;
					const lm = meta.get(key) || 'unknown-last-modified';
					logStream.write(`${lm}\t${formatS3Uri(bucket, key)}\n`);
				}
			}
		} catch (err) {
			errors += batch.length;
			console.error('DeleteObjects batch failed:', err.message || err);
		}
	}

	if (logStream) {
		logStream.end();
		console.log(`Deletion log written to: ${logFile}`);
	}

	return { deleted, errors };
}

async function main() {
	try {
		const { bucket, start, end, dryRun, prefix } = parseEnv();
		console.log(`Region: ${region}. Bucket: ${bucket}. Prefix: ${prefix || '(none)'} . DryRun: ${dryRun ? 'ON' : 'OFF'}`);

		const candidates = await collectKeysInDateRange(bucket, start, end, prefix);
        
		// Write preview to log and CSV
		const logPath = writeCandidatesLog(bucket, prefix, start, end, candidates);
		const csvPath = writeCandidatesCsv(bucket, candidates);
		console.log(`Preview written to: ${logPath}`);
		console.log(`CSV of candidates: ${csvPath}`);

		if (dryRun) {
			console.log('DRY_RUN=true: Skipping deletion. This was a preview only.');
			console.log('Summary:');
			console.log(`  Matched: ${candidates.length}`);
			console.log('  Deleted: 0');
			console.log('  Errors:  0');
			return;
		}

		const proceed = await promptYesConfirm(`Proceed to delete ${candidates.length} objects? Type "yes" to confirm: `);
		if (!proceed) {
			console.log('Deletion aborted by user.');
			console.log('Summary:');
			console.log(`  Matched: ${candidates.length}`);
			console.log('  Deleted: 0');
			console.log('  Errors:  0');
			return;
		}

		const { deleted, errors } = await deleteInBatches(bucket, candidates, false, logPath);
		console.log('Summary:');
		console.log(`  Deleted: ${deleted}`);
		console.log(`  Errors:  ${errors}`);
	} catch (err) {
		console.error('Error:', err.message || err);
		console.log('\nRequired env vars:');
		console.log('  AWS_BUCKET_NAME   Bucket to operate on');
		console.log('  START_DATE        Inclusive start (YYYY-MM-DD or ISO)');
		console.log('  END_DATE          Inclusive end (YYYY-MM-DD or ISO)');
		console.log('Optional env vars:');
		console.log('  AWS_REGION        Defaults to', region);
		console.log('  DRY_RUN           Set to "true" to simulate');
		console.log('  DIRECTORY         Limit to a prefix inside the bucket');
	}
}

main();

