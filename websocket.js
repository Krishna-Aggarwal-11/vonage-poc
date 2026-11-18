import { WebSocketServer } from 'ws';
import { finishVadStream, handleIncomingAudioChunk } from './vad.js';
import { finishAudioRecording, handleIncomingAudioChunk as recordFullAudioChunk } from './audio-recorder.js';
import { removeLocalFile, uploadAudioFile } from './cloudinary-storage.js';

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
    // Keep a distinct buffer for every socket, while namespacing it by the
    // AudioConnector x-user-id so different callers never share audio state.
    const connectionId = `${Date.now()}-${Math.random()}`;
    let recordingId = connectionId;
    let writeQueue = Promise.resolve();
    let recordingFailed = false;
    let connectionEnded = false;
    let finalizationPromise = null;

    const finalizeConnection = (reason) => {
      if (finalizationPromise) return finalizationPromise;
      connectionEnded = true;
      finalizationPromise = writeQueue.then(async () => {
        console.log(`[AudioConnector] Finalizing recording after ${reason}`);
        const [fullResult, vadResult] = await Promise.allSettled([
          finishAudioRecording(recordingId),
          finishVadStream(recordingId)
        ]);

        if (fullResult.status === 'fulfilled' && fullResult.value) {
          try {
            const uploaded = await uploadAudioFile(fullResult.value);
            await removeLocalFile(fullResult.value);
            console.log('[AudioConnector] Full recording uploaded:', uploaded.url);
          } catch (err) {
            console.error('[AudioConnector] Failed to upload full recording:', err);
          }
        } else if (fullResult.status === 'rejected') {
          console.error('[AudioConnector] Failed to finalize full recording:', fullResult.reason);
        }

        if (vadResult.status === 'fulfilled' && vadResult.value?.transcriptUrl) {
          console.log('[AudioConnector] Transcript JSON uploaded:', vadResult.value.transcriptUrl);
        } else if (vadResult.status === 'rejected') {
          console.error('[AudioConnector] Failed to finalize VAD transcripts:', vadResult.reason);
        }
      }).catch((err) => {
        console.error('[AudioConnector] Failed to finalize recordings:', err);
      });
      return finalizationPromise;
    };

    ws.on('message', (data, isBinary) => {
      if (!isBinary) {
        try {
          const message = JSON.parse(data.toString());
          console.log('[AudioConnector] Control/Metadata event:', message);
          const userId = message['x-user-id'] ?? message['×-user-id'];
          if (userId) {
            sessionMetadata = { ...sessionMetadata, ...message, userId };
            // The user id scopes this connection's buffers; connectionId keeps
            // simultaneous sockets for the same user isolated as well.
            recordingId = `${String(userId).replace(/[^a-zA-Z0-9_-]/g, '_')}:${connectionId}`;
          }
          if (message.event === 'start' || message.sessionId) {
            sessionMetadata = { ...sessionMetadata, ...message };
          }
        } catch (err) {
          console.error('[AudioConnector] Failed to parse text message:', err);
        }
      } else {
        // AudioConnector binary frames are signed 16-bit PCM at 16 kHz.
        if (connectionEnded || recordingFailed) return;
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
      finalizeConnection(`WebSocket close ${code}`);
    });

    ws.on('error', (err) => {
      console.error('[AudioConnector] Socket error:', err);
      finalizeConnection('WebSocket error');
    });
  });
}
