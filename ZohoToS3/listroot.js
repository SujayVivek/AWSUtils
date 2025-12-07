// listRoot.js
const axios = require("axios");
require("dotenv").config({ path: "../.env" });

async function main() {
  const tokenResp = await axios.post(
    "https://accounts.zoho.com/oauth/v2/token",
    new URLSearchParams({
      grant_type: "refresh_token",
      client_id: process.env.ZOHO_CLIENT_ID,
      client_secret: process.env.ZOHO_CLIENT_SECRET,
      refresh_token: process.env.ZOHO_REFRESH_TOKEN
    })
  );

  const token = tokenResp.data.access_token;

  const url = `https://www.zohoapis.com/workdrive/api/v1/files/${process.env.ZOHO_PARENT_FOLDER_ID}/files`;

  const resp = await axios.get(url, {
    headers: { Authorization: `Zoho-oauthtoken ${token}` }
  });

  console.log("Folders under parent root:");
  for (let f of resp.data.data) {
    console.log("-", f.attributes.name, "→ id:", f.id);
  }
}

main();
