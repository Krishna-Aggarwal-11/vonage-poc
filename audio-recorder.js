import { createWriteStream } from 'node:fs';
import { mkdir, open } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const SAMPLE_RATE = 16_000;
const CHANNELS = 1;
const BITS_PER_SAMPLE = 16;
// Vercel's deployed app directory is read-only. Use its writable temp area;
// set RECORDINGS_DIR to use another writable mounted volume.
const RECORDINGS_DIR = process.env.RECORDINGS_DIR || join(tmpdir(), 'recording');
const recordings = new Map();

// Save one complete PCM segment as a standard mono 16-bit WAV file.
export async function writePcmWaveFile(pcm, fileId) {
  await mkdir(RECORDINGS_DIR, { recursive: true });
  const path = join(RECORDINGS_DIR, `${fileId}-${Date.now()}.wav`);
  const output = createWriteStream(path);
  await new Promise((resolve, reject) => {
    output.once('open', resolve);
    output.once('error', reject);
  });
  output.write(makeWaveHeader(pcm.length));
  output.end(pcm);
  await new Promise((resolve, reject) => {
    output.once('finish', resolve);
    output.once('error', reject);
  });
  return path;
}

// Append one raw 16-bit PCM chunk to this connection's WAV recording.
export function handleIncomingAudioChunk(pcmChunk, metadata, recordingId) {
  let recording = recordings.get(recordingId);
  if (!recording) {
    recording = createPcmWaveRecording(metadata?.sessionId);
    recordings.set(recordingId, recording);
  }

  return recording.write(pcmChunk);
}

// Call when the WebSocket closes so RIFF and data sizes are written correctly.
export async function finishAudioRecording(recordingId) {
  const recording = recordings.get(recordingId);
  if (!recording) return;
  recordings.delete(recordingId);
  await recording.close();
  return recording.path;
}

function createPcmWaveRecording(sessionId) {
  const safeId = String(sessionId || 'recording').replace(/[^a-zA-Z0-9_-]/g, '_');
  const path = join(RECORDINGS_DIR, `${safeId}-full-${Date.now()}.wav`);
  let dataBytes = 0;
  let closed = false;
  let output;
  let resolveReady;
  let rejectReady;
  const ready = new Promise((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });
  const initialized = mkdir(RECORDINGS_DIR, { recursive: true }).then(() => {
    output = createWriteStream(path);
    output.once('open', resolveReady);
    output.once('error', rejectReady);
    output.write(makeWaveHeader(0));
  }).catch(rejectReady);

  return {
    path,
    async write(chunk) {
      await initialized;
      await ready;
      if (closed) return;
      const pcm = Buffer.from(chunk);
      const usableLength = pcm.length - (pcm.length % 2);
      if (!usableLength) return;
      dataBytes += usableLength;
      if (!output.write(pcm.subarray(0, usableLength))) {
        await new Promise((resolve, reject) => {
          output.once('drain', resolve);
          output.once('error', reject);
        });
      }
    },
    async close() {
      await initialized;
      await ready;
      if (closed) return;
      closed = true;
      output.end();
      await new Promise((resolve, reject) => {
        output.once('finish', resolve);
        output.once('error', reject);
      });
      const file = await open(path, 'r+');
      try {
        await file.write(makeWaveHeader(dataBytes), 0, 44, 0);
      } finally {
        await file.close();
      }
    }
  };
}

function makeWaveHeader(dataBytes) {
  const header = Buffer.alloc(44);
  const byteRate = SAMPLE_RATE * CHANNELS * BITS_PER_SAMPLE / 8;
  const blockAlign = CHANNELS * BITS_PER_SAMPLE / 8;
  header.write('RIFF', 0);
  header.writeUInt32LE(36 + dataBytes, 4);
  header.write('WAVE', 8);
  header.write('fmt ', 12);
  header.writeUInt32LE(16, 16);
  header.writeUInt16LE(1, 20);
  header.writeUInt16LE(CHANNELS, 22);
  header.writeUInt32LE(SAMPLE_RATE, 24);
  header.writeUInt32LE(byteRate, 28);
  header.writeUInt16LE(blockAlign, 32);
  header.writeUInt16LE(BITS_PER_SAMPLE, 34);
  header.write('data', 36);
  header.writeUInt32LE(dataBytes, 40);
  return header;
}
