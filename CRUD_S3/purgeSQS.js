const path = require('path');
const readline = require('readline');
require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });

const {
  SQSClient,
  GetQueueUrlCommand,
  GetQueueAttributesCommand,
  PurgeQueueCommand,
} = require('@aws-sdk/client-sqs');

const region = process.env.AWS_REGION || 'us-east-1';
const accessKeyId = process.env.AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_KEY;
const sqs = new SQSClient({
  region,
  credentials: accessKeyId && secretAccessKey ? { accessKeyId, secretAccessKey } : undefined,
});

function createPrompt() {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  const ask = (q) => new Promise((resolve) => rl.question(q, (ans) => resolve(ans)));
  return { ask, close: () => rl.close() };
}

async function getQueueUrlByName(name) {
  const cmd = new GetQueueUrlCommand({ QueueName: name });
  const res = await sqs.send(cmd);
  return res.QueueUrl;
}

async function getApproxQueueMessageCount(queueUrl) {
  const cmd = new GetQueueAttributesCommand({
    QueueUrl: queueUrl,
    AttributeNames: ['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible'],
  });
  const res = await sqs.send(cmd);
  const visible = parseInt(res.Attributes?.ApproximateNumberOfMessages || '0', 10);
  const notVisible = parseInt(res.Attributes?.ApproximateNumberOfMessagesNotVisible || '0', 10);
  return visible + notVisible;
}

async function purgeQueue(queueUrl) {
  const cmd = new PurgeQueueCommand({ QueueUrl: queueUrl });
  await sqs.send(cmd);
}

async function waitUntilPurged(queueUrl, timeoutMs = 90000, intervalMs = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const count = await getApproxQueueMessageCount(queueUrl);
    if (count === 0) return true;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  return false;
}

async function main() {
  const prompt = createPrompt();
  try {
    console.log(`Using AWS region: ${region}`);
    const countStr = await prompt.ask('no. of sqs needs to purge: ');
    const count = parseInt(countStr, 10);
    if (!Number.isFinite(count) || count <= 0) {
      console.log('Please enter a valid positive number.');
      return;
    }

    const names = [];
    for (let i = 1; i <= count; i++) {
      const name = await prompt.ask(`sqs ${i} name: `);
      names.push(name.trim());
    }

    for (let i = 0; i < names.length; i++) {
      const idx = i + 1;
      const name = names[i];
      try {
        let queueUrl;
        // Allow full SQS QueueUrl input to skip name resolution
        if (/^https?:\/\//i.test(name)) {
          queueUrl = name;
        } else {
          try {
            queueUrl = await getQueueUrlByName(name);
          } catch (e) {
            // If not found and looks like a FIFO queue missing suffix, try appending .fifo
            if (/(does not exist|NonExistentQueue)/i.test(e?.message || '')) {
              const maybeFifo = `${name}.fifo`;
              try {
                queueUrl = await getQueueUrlByName(maybeFifo);
              } catch (e2) {
                throw e; // rethrow original for clearer context
              }
            } else {
              throw e;
            }
          }
        }
        const beforeCount = await getApproxQueueMessageCount(queueUrl);
        await purgeQueue(queueUrl);
        console.log(`\nsqs ${idx} ${name} purging is done`);

        const purged = await waitUntilPurged(queueUrl);
        const afterCount = await getApproxQueueMessageCount(queueUrl);
        const reported = purged ? beforeCount : afterCount;
        console.log(`sqs ${idx} has ${reported} no. of message - all purged`);
      } catch (err) {
        let msg = err?.message || String(err);
        if (err?.name === 'PurgeQueueInProgress') {
          msg = 'Purge already in progress; retry after 60 seconds.';
        } else if (/(does not exist|NonExistentQueue)/i.test(msg)) {
          msg = 'The specified queue does not exist or is in a different region/account. Verify exact name (include .fifo for FIFO) and AWS_REGION.';
        } else if (/(AccessDenied|not have access)/i.test(msg)) {
          msg = 'Access denied. Ensure credentials have permissions: sqs:GetQueueUrl, sqs:GetQueueAttributes, sqs:PurgeQueue.';
        }
        console.error(`\nError purging sqs ${idx} ${name}: ${msg}`);
      }
    }
  } finally {
    prompt.close();
  }
}

main().catch((e) => {
  console.error('Unexpected error:', e?.message || e);
});
