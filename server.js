import express from 'express';
import http from 'http';
import cors from 'cors';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { Vonage } from '@vonage/server-sdk';
import { setupAudioConnectorWebSocket } from './websocket.js';

dotenv.config();

const app = express();
const server = http.createServer(app);

const ALLOWED_ORIGIN = process.env.FRONTEND_ORIGIN || '*';
app.use(cors({
  origin: ALLOWED_ORIGIN,
  methods: ['GET', 'POST']
}));

app.use(express.json());

const applicationId = process.env.VONAGE_APPLICATION_ID;
const privateKeyPath = path.resolve(process.env.VONAGE_PRIVATE_KEY_PATH);

if (!applicationId || !privateKeyPath) {
  console.error("Missing required environment variables.");
  process.exit(1);
}

const privateKey = fs.readFileSync(privateKeyPath, 'utf8');
const vonage = new Vonage({ applicationId, privateKey });

// In-memory session lookup: roomName -> sessionId
const roomSessions = new Map();

app.post('/api/session/join', async (req, res) => {
  const { roomName, userId } = req.body;

  if (!roomName || !userId) {
    return res.status(400).json({ error: 'roomName and userId are required' });
  }

  try {
    let sessionId = roomSessions.get(roomName);

    if (!sessionId) {
      const session = await vonage.video.createSession({ mediaMode: 'routed', archiveMode: 'always' });
      sessionId = session.sessionId;
      roomSessions.set(roomName, sessionId);
      console.log(`[Session Created] Room: ${roomName} -> Session: ${sessionId}`);
    }

    const connectionData = JSON.stringify({ userId: String(userId) });

    const token = vonage.video.generateClientToken(sessionId, {
      role: 'publisher',
      expireTime: Math.floor(Date.now() / 1000) + (24 * 3600),
      data: connectionData
    });

    res.json({ applicationId, sessionId, token, userId });
  } catch (err) {
    console.error('Session join failure:', err);
    res.status(500).json({
      error: 'Failed to join session',
      details: err.message,
      stack: process.env.NODE_ENV !== 'production' ? err.stack : undefined
    });
  }
});

app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    uptime: process.uptime(),
    timestamp: Date.now()
  });
});

setupAudioConnectorWebSocket(server);

server.listen(process.env.PORT || 5000, () => {
  console.log('Server listening on port', process.env.PORT || 5000);
});