import { WebSocketServer } from 'ws';
import { finishVadStream, handleIncomingAudioChunk } from './vad.js';
import { finishAudioRecording, handleIncomingAudioChunk as recordFullAudioChunk } from './audio-recorder.js';

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
    const recordingId = `${Date.now()}-${Math.random()}`;
    let writeQueue = Promise.resolve();
    let recordingFailed = false;

    ws.on('message', (data, isBinary) => {
      if (!isBinary) {
        try {
          const message = JSON.parse(data.toString());
          console.log('[AudioConnector] Control/Metadata event:', message);
          if (message.event === 'start' || message.sessionId) {
            sessionMetadata = message;
          }
        } catch (err) {
          console.error('[AudioConnector] Failed to parse text message:', err);
        }
      } else {
        // AudioConnector binary frames are signed 16-bit PCM at 16 kHz.
        writeQueue = writeQueue.then(async () => {
          if (recordingFailed) return;
          await recordFullAudioChunk(data, sessionMetadata, recordingId);
          await handleIncomingAudioChunk(data, sessionMetadata, recordingId);
        }).catch((err) => {
          recordingFailed = true;
          console.error('[AudioConnector] Failed to process audio chunk:', err);
          ws.close(1011, 'VAD processing failed');
        });
      }
    });

    ws.on('close', (code, reason) => {
      console.log(`[AudioConnector] Disconnected: ${code} - ${reason.toString()}`);
      writeQueue.then(async () => {
        const fullPath = await finishAudioRecording(recordingId);
        const segmentPaths = await finishVadStream(recordingId);
        return { fullPath, segmentPaths };
      }).then(({ fullPath, segmentPaths }) => {
        if (fullPath) console.log('[AudioConnector] Full recording finalized:', fullPath);
        if (segmentPaths.length) console.log('[AudioConnector] Final VAD segments:', segmentPaths);
      }).catch((err) => {
        console.error('[AudioConnector] Failed to finalize VAD stream:', err);
      });
    });

    ws.on('error', (err) => {
      console.error('[AudioConnector] Socket error:', err);
    });
  });
}
