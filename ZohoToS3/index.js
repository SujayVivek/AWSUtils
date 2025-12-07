const fs = require("fs");
const path = require("path");

// ---------------- CLEAN ZOHO PATH ----------------
function cleanZohoPath(p) {
    if (!p) return "";

    // Split by backslash
    const parts = p.split("\\");

    // Remove first 3 segments (Z:, General, date/folder/etc.)
    const cleaned = parts.slice(3).join("\\");

    // Add leading slash
    return "\\" + cleaned;
}

// ---------------- CLEAN S3 PATH ----------------
function cleanS3Path(p) {
    if (!p) return "";

    // Remove s3/ or S3/
    return p.replace(/^s3\//i, "");
}

// ---------------- MAIN SCRIPT ----------------
function cleanCSV(inputFile, outputFile) {
    const file = fs.readFileSync(inputFile, "utf8").trim();
    const lines = file.split("\n");

    // Header stays same
    let output = [lines[0]];

    for (let i = 1; i < lines.length; i++) {
        let [zoho, s3] = lines[i].split(",");

        const newZoho = cleanZohoPath(zoho);
        const newS3   = cleanS3Path(s3);

        output.push(`${newZoho},${newS3}`);
    }

    fs.writeFileSync(outputFile, output.join("\n"));
    console.log("✔ CSV cleaned and saved to:", outputFile);
}

// ---------------- RUN ----------------
cleanCSV("input.csv", "cleaned.csv");
