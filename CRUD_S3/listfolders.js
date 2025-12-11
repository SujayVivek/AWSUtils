const XLSX = require("xlsx");
const fs = require("fs");
const path = require('path');
const readline = require('readline');

require('dotenv').config({ path: path.resolve(__dirname, '..', '.env') });
const { S3Client, ListObjectsV2Command } = require("@aws-sdk/client-s3");

const s3 = new S3Client({
    region: process.env.AWS_REGION || "us-east-1",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    }
});

const bucket = "rules-repository";

async function listFilesAndFolders() {
    let ContinuationToken = undefined;
    const folders = new Set();
    const files = [];

    let total = 0;
    console.log("Listing S3 bucket contents...");

    do {
        const response = await s3.send(new ListObjectsV2Command({
            Bucket: bucket,
            ContinuationToken,
            MaxKeys: 2000
        }));

        const contents = response.Contents || [];

        contents.forEach(obj => {
            const key = obj.Key;
            const parts = key.split("/");

            // Folders
            for (let i = 1; i < parts.length; i++) {
                folders.add(parts.slice(0, i).join("/") + "/");
            }

            // Files
            if (!key.endsWith("/")) {
                files.push(key);
            }

            total++;
        });

        console.log(`Fetched ${total} keys so far...`);

        ContinuationToken = response.NextContinuationToken;

    } while (ContinuationToken);

    console.log("Finished scanning S3. Creating Excel file...");

    // Convert to arrays for Excel
    const folderArray = [...folders].sort().map(f => ({ Folder: f }));
    const fileArray = files.sort().map(f => ({ File: f }));

    // Create workbook
    const wb = XLSX.utils.book_new();

    const folderSheet = XLSX.utils.json_to_sheet(folderArray);
    const fileSheet = XLSX.utils.json_to_sheet(fileArray);

    XLSX.utils.book_append_sheet(wb, folderSheet, "Folders");
    XLSX.utils.book_append_sheet(wb, fileSheet, "Files");

    XLSX.writeFile(wb, "s3_export.xlsx");

    console.log("✨ Excel file saved as s3_export.xlsx");
    console.log(`Total folders: ${folderArray.length}`);
    console.log(`Total files: ${fileArray.length}`);
}

listFilesAndFolders();
