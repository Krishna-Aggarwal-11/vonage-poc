import { createVad } from '@fluidinference/fluidvad';
import { writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { writePcmWaveFile } from './audio-recorder.js';
import { removeLocalFile, uploadAudioFile, uploadJsonFile } from './cloudinary-storage.js';
import { transcribeAudioFile } from './openai-transcription.js';

const SAMPLE_RATE = 16_000;
const PADDING_SECONDS = 0.15;
const SILENCE_SECONDS = 1;
const streams = new Map();

async function createStream(sessionId) {
  const vad = await createVad({
    threshold: 0.5,
    minSilenceDuration: SILENCE_SECONDS,
    speechPadding: PADDING_SECONDS,
    maxSpeechDuration: Infinity
  });
  const safeId = String(sessionId || 'recording').replace(/[^a-zA-Z0-9_-]/g, '_');
  return {
    vad,
    safeId,
    chunks: [],
    totalSamples: 0,
    bufferStartSample: 0,
    speechStart: null,
    segmentNumber: 0,
    transcriptEntries: [],
    jobs: []
  };
}

// Feed each WebSocket PCM chunk into one persistent VAD stream per connection.
export async function handleIncomingAudioChunk(pcmChunk, metadata, streamId) {
  let state = streams.get(streamId);
  if (!state) {
    const pending = createStream(metadata?.sessionId);
    streams.set(streamId, pending);
    state = await pending;
    streams.set(streamId, state);
  } else {
    state = await state;
  }

  const pcm = Buffer.from(pcmChunk);
  const usableLength = pcm.length - (pcm.length % 2);
  if (!usableLength) return;
  const chunk = pcm.subarray(0, usableLength);
  state.chunks.push(chunk);
  state.totalSamples += usableLength / 2;

  // PCM from AudioConnector is signed 16-bit little-endian, mono, 16 kHz.
  const samples = new Float32Array(usableLength / 2);
  for (let i = 0; i < samples.length; i++) {
    samples[i] = chunk.readInt16LE(i * 2) / 32768;
  }

  for (const event of state.vad.push(samples)) {
    if (event.isStart) {
      state.speechStart = Math.max(0, event.sampleIndex);
      console.log(`[VAD] Speech started at ${event.timeSeconds.toFixed(2)}s`);
    } else if (state.speechStart !== null) {
      const endSample = Math.min(state.totalSamples, event.sampleIndex);
      await saveSegment(state, state.speechStart, endSample);
      state.speechStart = null;
      console.log(`[VAD] Speech ended at ${event.timeSeconds.toFixed(2)}s`);
    }
  }
  if (state.speechStart === null) pruneAudio(state, Math.max(0, state.totalSamples - SAMPLE_RATE / 2));
}

// Flush a final active utterance when the WebSocket closes.
export async function finishVadStream(streamId) {
  const pending = streams.get(streamId);
  if (!pending) return [];
  const state = await pending;
  if (state.speechStart !== null) {
    await saveSegment(state, state.speechStart, state.totalSamples);
  }

  // Transcriptions run concurrently while audio keeps arriving. On socket
  // close, retain the job and finish only after every chunk settles.
  await Promise.allSettled(state.jobs);
  const jsonPath = join(tmpdir(), `${state.safeId}-${Date.now()}-transcript.json`);
  await writeFile(jsonPath, JSON.stringify(state.transcriptEntries, null, 2));
  try {
    const uploaded = await uploadJsonFile(jsonPath);
    await removeLocalFile(jsonPath);
    console.log('[Transcript] JSON file completed:', uploaded.url);
    return { transcriptUrl: uploaded.url, entries: state.transcriptEntries };
  } finally {
    streams.delete(streamId);
  }
}

async function saveSegment(state, startSample, endSample) {
  if (endSample <= startSample) return null;
  // Drop already-finished audio from the connection buffer to bound memory.
  const allPcm = Buffer.concat(state.chunks);
  const startByte = Math.max(0, startSample - state.bufferStartSample) * 2;
  const endByte = (Math.min(state.totalSamples, endSample) - state.bufferStartSample) * 2;
  if (endByte <= startByte) return null;
  const audio = allPcm.subarray(startByte, endByte);
  if (!audio.length) return null;

  state.segmentNumber += 1;
  const fileId = `${state.safeId}-segment-${String(state.segmentNumber).padStart(3, '0')}`;
  const path = await writePcmWaveFile(audio, fileId);
  const entry = {
    startTime: Number((startSample / SAMPLE_RATE).toFixed(3)),
    endTime: Number((endSample / SAMPLE_RATE).toFixed(3)),
    transcript: ''
  };
  state.transcriptEntries.push(entry);

  // Start OpenAI and Cloudinary requests together. Do not block VAD or the
  // next incoming WebSocket chunk while either network request is running.
  const job = (async () => {
    const [transcription, upload] = await Promise.allSettled([
      transcribeAudioFile(path),
      uploadAudioFile(path)
    ]);
    if (transcription.status === 'fulfilled') {
      entry.transcript = transcription.value;
    } else {
      entry.transcriptionError = transcription.reason?.message || String(transcription.reason);
      console.error('[Transcript] Chunk transcription failed:', entry.transcriptionError);
    }
    if (upload.status === 'fulfilled') {
      entry.audioUrl = upload.value.url;
      console.log('[VAD] Uploaded segment:', upload.value.url);
    } else {
      entry.audioUploadError = upload.reason?.message || String(upload.reason);
      console.error('[VAD] Segment upload failed:', entry.audioUploadError);
    }
    await removeLocalFile(path);
  })();
  state.jobs.push(job);

  // Retain 150 ms before the next utterance for its configured speech padding.
  pruneAudio(state, Math.max(state.bufferStartSample, endSample - SAMPLE_RATE * PADDING_SECONDS));
  state.speechStart = null;
  console.log(`[VAD] Segment ${state.segmentNumber} queued for transcription`);
  return entry;
}

function pruneAudio(state, beforeSample) {
  if (beforeSample <= state.bufferStartSample) return;
  const allPcm = Buffer.concat(state.chunks);
  const dropBytes = Math.min(allPcm.length, (beforeSample - state.bufferStartSample) * 2);
  state.chunks = [allPcm.subarray(dropBytes)];
  state.bufferStartSample += dropBytes / 2;
}
