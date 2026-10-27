import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { Vonage } from '@vonage/server-sdk';

dotenv.config();

const keyPath = path.resolve(process.env.VONAGE_PRIVATE_KEY_PATH);
console.log('Resolved Key Path:', keyPath);

if (!fs.existsSync(keyPath)) {
  console.error('FATAL: private.key file does not exist at path!');
  process.exit(1);
}

const privateKey = fs.readFileSync(keyPath, 'utf8');

if (!privateKey.includes('-----BEGIN PRIVATE KEY-----')) {
  console.error('FATAL: Key file does not start with -----BEGIN PRIVATE KEY-----');
  process.exit(1);
}

// Pass all 4 credentials if available
const vonage = new Vonage({
  apiKey: process.env.VONAGE_API_KEY,         // Grab from Dashboard -> API Settings
  apiSecret: process.env.VONAGE_API_SECRET,   // Grab from Dashboard -> API Settings
  applicationId: process.env.VONAGE_APPLICATION_ID,
  privateKey: privateKey
});

async function run() {
  try {
    console.log('Attempting createSession with App ID:', process.env.VONAGE_APPLICATION_ID);
    const session = await vonage.video.createSession({ mediaMode: 'routed' });
    console.log('SUCCESS! Session ID:', session.sessionId);
  } catch (err) {
    console.error('Status:', err.status || err.response?.status);
    console.error('Message:', err.message);
    if (err.response?.data) {
      console.error('Response Data:', JSON.stringify(err.response.data, null, 2));
    }
  }
}

run();