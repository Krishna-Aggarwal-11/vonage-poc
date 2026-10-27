import { WebSocketServer } from 'ws';

export function setupAudioConnectorWebSocket(server) {
  const wss = new WebSocketServer({ noServer: true });

  server.on('upgrade', (request, socket, head) => {
    const url = new URL(request.url, `http://${request.headers.host}`);
    if (url.pathname === '/media/stream') {
      wss.handleUpgrade(request, socket, head, (ws) => {
        wss.emit('connection', ws, request);
      });
    } else {
      socket.destroy();
    }
  });

  wss.on('connection', (ws, req) => {
    console.log('[AudioConnector] Client connected from:', req.socket.remoteAddress);
    let sessionMetadata = null;

    ws.on('message', (data, isBinary) => {
      if (!isBinary) {
        // Text messages: Metadata, handshake, or control signals
        try {
          const message = JSON.parse(data.toString());
          console.log('[AudioConnector] Control/Metadata event:', message);

          // Example handshake message contains event info (e.g., websocket connected/session info)
          if (message.event === 'start' || message.sessionId) {
            sessionMetadata = message;
          }
        } catch (err) {
          console.error('[AudioConnector] Failed to parse text message:', err);
        }
      } else {
        // Binary frames: 16-bit signed PCM audio at 16,000 Hz
        // data is a Buffer containing raw audio samples
        handleIncomingAudioChunk(data, sessionMetadata);
      }
    });

    ws.on('close', (code, reason) => {
      console.log(`[AudioConnector] Disconnected: ${code} - ${reason.toString()}`);
    });

    ws.on('error', (err) => {
      console.error('[AudioConnector] Socket error:', err);
    });
  });
}

function handleIncomingAudioChunk(pcmChunk, metadata) {
  // Pass chunk to your STT transcription stream (see Step 4)
}