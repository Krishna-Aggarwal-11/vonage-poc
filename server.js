import express from 'express';
import http from 'http';
import cors from 'cors';
import dotenv from 'dotenv';
import { setupAudioConnectorWebSocket } from './websocket.js';

dotenv.config();

const app = express();
const server = http.createServer(app);

const MAX_LOG_ENTRIES = 500;
const logEntries = [];
const logClients = new Set();

function publishLog(level, args) {
  const entry = {
    timestamp: new Date().toISOString(),
    level,
    message: args.map((value) => {
      if (typeof value === 'string') return value;
      if (value instanceof Error) return value.stack || value.message;
      try {
        return JSON.stringify(value);
      } catch {
        return String(value);
      }
    }).join(' ')
  };

  logEntries.push(entry);
  if (logEntries.length > MAX_LOG_ENTRIES) logEntries.shift();

  const payload = `data: ${JSON.stringify(entry)}\n\n`;
  for (const client of logClients) client.write(payload);
}

for (const level of ['log', 'info', 'warn', 'error']) {
  const original = console[level].bind(console);
  console[level] = (...args) => {
    original(...args);
    publishLog(level, args);
  };
}

const ALLOWED_ORIGIN = process.env.FRONTEND_ORIGIN || '*';
app.use(cors({
  origin: ALLOWED_ORIGIN,
  methods: ['GET', 'POST']
}));

app.use(express.json());

app.use((req, res, next) => {
  const startedAt = Date.now();
  res.on('finish', () => {
    console.log(`${req.method} ${req.originalUrl} ${res.statusCode} ${Date.now() - startedAt}ms`);
  });
  next();
});

app.get('/', (req, res) => {
  res.type('html').send(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Server Logs</title>
  <style>
    body { margin: 0; padding: 24px; background: #111827; color: #e5e7eb; font: 14px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; }
    header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }
    h1 { margin: 0; font: 600 20px system-ui, sans-serif; }
    #status { color: #86efac; font: 13px system-ui, sans-serif; }
    #logs { white-space: pre-wrap; overflow-wrap: anywhere; }
    .entry { padding: 5px 8px; border-bottom: 1px solid #273244; }
    .time { color: #9ca3af; }
    .warn { color: #fbbf24; }
    .error { color: #f87171; }
  </style>
</head>
<body>
  <header><h1>Server Logs</h1><span id="status">Connecting…</span></header>
  <main id="logs" aria-live="polite"></main>
  <script>
    const logs = document.getElementById('logs');
    const status = document.getElementById('status');
    function addEntry(entry) {
      const row = document.createElement('div');
      row.className = 'entry ' + entry.level;
      const time = document.createElement('span');
      time.className = 'time';
      time.textContent = new Date(entry.timestamp).toLocaleTimeString() + ' ';
      const message = document.createElement('span');
      message.textContent = '[' + entry.level.toUpperCase() + '] ' + entry.message;
      row.append(time, message);
      logs.appendChild(row);
      while (logs.childElementCount > ${MAX_LOG_ENTRIES}) logs.firstElementChild.remove();
      window.scrollTo(0, document.body.scrollHeight);
    }
    fetch('/logs').then((response) => response.json()).then((entries) => entries.forEach(addEntry));
    const stream = new EventSource('/logs/stream');
    stream.onopen = () => { status.textContent = 'Live'; };
    stream.onerror = () => { status.textContent = 'Reconnecting…'; };
    stream.onmessage = (event) => addEntry(JSON.parse(event.data));
  </script>
</body>
</html>`);
});

app.get('/logs', (req, res) => {
  res.json(logEntries);
});

app.get('/logs/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();
  logClients.add(res);
  req.on('close', () => logClients.delete(res));
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
