import { readFile } from 'node:fs/promises';
import { basename } from 'node:path';

const TRANSCRIPTION_URL = 'https://api.openai.com/v1/audio/transcriptions';

// Call OpenAI's transcription REST endpoint directly, without the SDK.
export async function transcribeAudioFile(path) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('OPENAI_API_KEY is not configured');

  const audio = await readFile(path);
  const form = new FormData();
  form.append('file', new Blob([audio], { type: 'audio/wav' }), basename(path));
  form.append('model', process.env.OPENAI_TRANSCRIPTION_MODEL || 'gpt-transcribe');

  const response = await fetch(TRANSCRIPTION_URL, {
    method: 'POST',
    headers: { Authorization: `Bearer ${apiKey}` },
    body: form
  });
  const result = await response.json();
  if (!response.ok) {
    throw new Error(`OpenAI transcription failed (${response.status}): ${result.error?.message || JSON.stringify(result)}`);
  }
  return result.text || '';
}
