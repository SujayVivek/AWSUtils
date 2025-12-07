/**************************************************************************
 * Zoho WorkDrive – Team Folder Recursive Downloader
 * Works for Team Folders ONLY, not Workspaces.
 * Uses correct endpoints:
 *   - List files/folders:  GET /workdrive/api/v1/files/{folderId}/files
 *   - Download file:       GET https://download.zoho.com/v1/workdrive/download/file/{fileId}
 **************************************************************************/

const fs = require("fs");
const path = require("path");
const axios = require("axios");

// Load env from parent folder
const envPath = path.resolve(__dirname, "..", ".env");
require("dotenv").config({ path: envPath });

const PARENT_ID = process.env.ZOHO_PARENT_FOLDER_ID;
const CLIENT_ID = process.env.ZOHO_CLIENT_ID;
const CLIENT_SECRET = process.env.ZOHO_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.ZOHO_REFRESH_TOKEN;

if (!PARENT_ID) {
  console.error("❌ Missing ZOHO_PARENT_FOLDER_ID in .env");
  process.exit(1);
}

console.log("Loaded Parent Folder:", PARENT_ID);

// Token cache
let accessToken = null;

// Get new access token
async function getAccessToken() {
  if (accessToken) return accessToken;

  const tokenURL = "https://accounts.zoho.com/oauth/v2/token";
  const params = new URLSearchParams({
    grant_type: "refresh_token",
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    refresh_token: REFRESH_TOKEN,
  });

  const resp = await axios.post(tokenURL, params.toString(), {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  });

  accessToken = resp.data.access_token;
  return accessToken;
}

// API client for metadata calls (not download)
async function apiClient() {
  const token = await getAccessToken();
  return axios.create({
    baseURL: "https://www.zohoapis.com/workdrive/api/v1",
    headers: {
      Authorization: `Zoho-oauthtoken ${token}`,
    },
  });
}

// List items inside a Team Folder
async function listChildren(folderId) {
  const api = await apiClient();
  const url = `/files/${folderId}/files`;

  try {
    const resp = await api.get(url);
    return resp.data.data || [];
  } catch (err) {
    throw new Error(
      `List error for ${folderId}: ${err.response?.status} ${JSON.stringify(
        err.response?.data
      )}`
    );
  }
}

// Download single file
async function downloadFile(fileId, destPath) {
  const token = await getAccessToken();
  const url = `https://download.zoho.com/v1/workdrive/download/file/${fileId}`;

  const writer = fs.createWriteStream(destPath);

  const resp = await axios.get(url, {
    responseType: "stream",
    headers: { Authorization: `Zoho-oauthtoken ${token}` },
  });

  return new Promise((resolve, reject) => {
    resp.data.pipe(writer);
    writer.on("finish", resolve);
    writer.on("error", reject);
  });
}

// Recursively download folder tree
async function downloadTree(folderId, localPath) {
  // Ensure folder exists
  fs.mkdirSync(localPath, { recursive: true });

  const children = await listChildren(folderId);

  for (const item of children) {
    const name = item.attributes.name;
    const id = item.id;
    const isFolder =
      item.attributes.is_folder ||
      item.attributes.type === "folder" ||
      item.type === "files";

    const itemPath = path.join(localPath, name);

    if (isFolder) {
      console.log("📁 Folder:", name);
      await downloadTree(id, itemPath);
    } else {
      console.log("⬇️  File:", name);
      await downloadFile(id, itemPath);
    }
  }
}

// Read CSV
function readCSV(csvPath) {
  const txt = fs.readFileSync(csvPath, "utf8").trim();
  const lines = txt.split(/\r?\n/);
  const out = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;

    const idx = line.indexOf(",");
    if (idx === -1) continue;

    const zohoPath = line.slice(0, idx).trim();
    out.push(zohoPath);
  }

  return out;
}

// Resolve ZohoPath → folderId
async function resolveFolderByPath(zohoPath) {
  let parts = zohoPath.replace(/\\+/g, "\\").split("\\").filter(Boolean);
  let current = PARENT_ID;

  for (const part of parts) {
    const children = await listChildren(current);

    const match = children.find(
      (c) =>
        c.attributes.name &&
        c.attributes.name.toLowerCase() === part.toLowerCase()
    );

    if (!match) {
      throw new Error(`Folder not found: '${part}' under parent ${current}`);
    }

    current = match.id;
  }

  return current;
}

async function run() {
  const csvPath = path.resolve(__dirname, "cleaned.csv");
  const rows = readCSV(csvPath);

  console.log("Loaded", rows.length, "paths from CSV\n");

  for (const zohoPath of rows) {
    console.log(`=== Downloading: ${zohoPath} ===`);

    try {
      const folderId = await resolveFolderByPath(zohoPath);
      const localPath = path.join(__dirname, "downloads", zohoPath);
      await downloadTree(folderId, localPath);
      console.log(`✔ Done: ${zohoPath}\n`);
    } catch (err) {
      console.log(`❌ Failed: ${zohoPath}`, err.message, "\n");
    }
  }

  console.log("All done.");
}

run();
