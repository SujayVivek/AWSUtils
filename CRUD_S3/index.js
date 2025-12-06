const {S3Client, GetObjectCommand, PutObjectCommand} = require('@aws-sdk/client-s3');
const {getSignedUrl} = require('@aws-sdk/s3-request-presigner');
const fs = require('fs');

try {
    require('dotenv').config();
} catch {}

const accessKeyId = process.env.AWS_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY;
const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || process.env.AWS_SECRET_KEY;
const region = process.env.AWS_REGION || 'eu-north-1';

const s3Client = new S3Client({
    region,
    credentials: {
        accessKeyId,
        secretAccessKey,
    },
});

async function getObject(key){
    const command = new GetObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: key,
    })
    const url = await getSignedUrl(s3Client, command, {expiresIn: 3600});
    return url;
}

// get presigned URL for frontend/postman upload PUT
async function getPutUrl(key, contentType){
    const command = new PutObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: key,
        ContentType: contentType,
    });
    const url = await getSignedUrl(s3Client, command, {expiresIn: 3600});
    return url;
}

// Upload a local file directly(server side upload)
async function putObjectFromFile(key, filePath, contentType){
    const body = fs.readFileSync(filePath);
    const command = new PutObjectCommand({
        Bucket: process.env.AWS_BUCKET_NAME,
        Key: key,
        Body: body,
        ContentType: contentType,
    });
    await s3Client.send(command);
    return `s3://${process.env.AWS_BUCKET_NAME}/${key}`;
}

async function init(){
    console.log('Starting S3 utils...');
    // console.log("GET OBJECT URL:", await getObject('Auralis_pitchdeck.pdf'));
    
    // Example: generate presigned URL for client-side PUT
    // console.log("URL FROM PUT OBJECT:", await getPutUrl('images/sujay/trial.txt', 'text/plain'));

    // Example: upload local file directly (no presigned URL)
    const location = await putObjectFromFile('images/sujay/triaL.txt', './trial.txt', 'text/plain');
    console.log('Uploaded to:', location);
}


init();
