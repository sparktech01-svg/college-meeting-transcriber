'use strict';

/**
 * College Meeting Transcriber & Document Generator
 * Integrated Express HTTP engine + unified WebSocket server.
 *
 * Pipeline:
 *   Browser (MediaRecorder) -> ws /stream -> Deepgram live WS -> transcript frames
 *   -> relayed back to browser + cached server-side.
 *
 * Endpoints:
 *   GET  /                  -> serves public/index.html
 *   POST /api/transcript    -> returns raw timestamped transcript text
 *   POST /api/summary/pdf   -> Gemini summary rendered to PDF via PDFKit
 *   POST /api/summary/text  -> Gemini summary returned as markdown text
 *   POST /api/process-link  -> stub for "Paste Video Link" (server-side fetch + Deepgram pre-recorded)
 *   POST /api/process-file  -> accepts an uploaded video file, extracts audio, transcribes via Deepgram pre-recorded
 *   WS   /stream            -> live audio streaming socket
 */

const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const { URL } = require('url');
const WebSocket = require('ws');
const PDFDocument = require('pdfkit');
const { YoutubeTranscript } = require('youtube-transcript');

// Auto-load local .env file if present
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  const envContent = fs.readFileSync(envPath, 'utf8');
  for (const line of envContent.split('\n')) {
    const trimmed = line.trim();
    if (trimmed && !trimmed.startsWith('#') && trimmed.includes('=')) {
      const idx = trimmed.indexOf('=');
      const key = trimmed.slice(0, idx).trim();
      const val = trimmed.slice(idx + 1).trim().replace(/^["']|["']$/g, '');
      if (key && !process.env[key]) {
        process.env[key] = val;
      }
    }
  }
}

process.env.DEEPGRAM_API_KEY = process.env.DEEPGRAM_API_KEY || '';
process.env.GEMINI_API_KEY = process.env.GEMINI_API_KEY || '';



const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json({ limit: '50mb' }));
app.use('/api/process-file', express.raw({ type: () => true, limit: '500mb' }));
app.use(express.raw({ type: 'application/octet-stream', limit: '500mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---------------------------------------------------------------------------
// Session store: per-client transcript cache + metadata
// ---------------------------------------------------------------------------
const sessions = new Map(); // clientId -> { transcript: [{t, text}], startedAt, deepgramOpen }

function makeClientId() {
  return 'sess_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);
}

function ensureSession(clientId) {
  if (!sessions.has(clientId)) {
    sessions.set(clientId, {
      transcript: [],
      startedAt: new Date(),
      deepgramOpen: false,
    });
  }
  return sessions.get(clientId);
}

// ---------------------------------------------------------------------------
// Deepgram live streaming socket wrapper
// ---------------------------------------------------------------------------
function openDeepgramStream(clientWs, clientId) {
  const session = ensureSession(clientId);
  const apiKey = process.env.DEEPGRAM_API_KEY;

  if (!apiKey) {
    sendJson(clientWs, { type: 'error', message: 'DEEPGRAM_API_KEY is not configured on the server.' });
    return null;
  }

  const params = new URLSearchParams({
    model: 'nova-2',
    smart_format: 'true',
    interim_results: 'true',
    punctuate: 'true',
    encoding: 'linear16',
    sample_rate: '16000',
  });

  const dgUrl = `wss://api.deepgram.com/v1/listen?${params.toString()}`;
  const dgWs = new WebSocket(dgUrl, {
    headers: {
      Authorization: `Token ${apiKey}`,
    },
  });

  dgWs.on('open', () => {
    session.deepgramOpen = true;
    sendJson(clientWs, { type: 'status', state: 'Recording...', message: 'Deepgram stream open.' });
  });

  dgWs.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      const channel = msg.channel && msg.channel.alternatives && msg.channel.alternatives[0];
      const text = channel ? channel.transcript : '';
      if (!text) return;

      const isFinal = msg.is_final === true;
      const ts = msg.start || 0;
      const entry = { t: ts, text, isFinal };

      if (isFinal) {
        session.transcript.push({ t: ts, text });
      }
      sendJson(clientWs, { type: 'transcript', entry });
    } catch (err) {
      sendJson(clientWs, { type: 'error', message: 'Failed to parse Deepgram frame.' });
    }
  });

  dgWs.on('error', (err) => {
    sendJson(clientWs, { type: 'error', message: 'Deepgram stream error: ' + (err.message || 'unknown') });
  });

  dgWs.on('close', () => {
    session.deepgramOpen = false;
    sendJson(clientWs, { type: 'status', state: 'Streaming Completed', message: 'Deepgram stream closed.' });
  });

  return dgWs;
}
function sendJson(ws, obj) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

// ---------------------------------------------------------------------------
// Unified WebSocket server: route by URL
// ---------------------------------------------------------------------------
wss.on('connection', (ws, req) => {
  const pathname = new URL(req.url, 'http://localhost').pathname;
  if (pathname !== '/stream') {
    ws.close(1000, 'unknown route');
    return;
  }

  const clientId = makeClientId();
  ws.clientId = clientId;
  ensureSession(clientId);
  sendJson(ws, { type: 'hello', clientId, state: 'Idle', message: 'Connected to streaming server.' });

  let dgWs = null;

  ws.on('message', (payload, isBinary) => {
    if (isBinary) {
      // Raw audio chunk from the browser -> forward to Deepgram
      if (dgWs && dgWs.readyState === WebSocket.OPEN) {
        dgWs.send(payload);
      }
      return;
    }

    let msg;
    try {
      msg = JSON.parse(payload.toString());
    } catch {
      return;
    }

    if (msg.type === 'start') {
      if (!dgWs || dgWs.readyState !== WebSocket.OPEN) {
        dgWs = openDeepgramStream(ws, clientId);
      }
    } else if (msg.type === 'stop') {
      if (dgWs && dgWs.readyState === WebSocket.OPEN) {
        dgWs.close();
      }
      sendJson(ws, { type: 'status', state: 'Streaming Completed', message: 'Session stopped by client.' });
    }
  });

  ws.on('close', () => {
    if (dgWs && dgWs.readyState === WebSocket.OPEN) {
      dgWs.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Helpers: transcript formatting + Gemini summary
// ---------------------------------------------------------------------------
function formatTimestamp(seconds) {
  const s = Math.floor(seconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(h)}:${pad(m)}:${pad(sec)}`;
}

function buildRawTranscript(session) {
  if (!session || !session.transcript.length) return '';
  return session.transcript
    .map((e) => `[${formatTimestamp(e.t)}] ${e.text}`)
    .join('\n');
}

const SUMMARY_SYSTEM_PROMPT = `You are an expert academic assistant for a university meeting transcriber.
Given a raw meeting transcript, produce a clean Markdown document with EXACTLY these three sections:

## Executive Summary
A concise 3-5 sentence overview of what the meeting covered.

## Key Decisions Made
A bulleted list of the concrete decisions reached. If none, state "No formal decisions were recorded."

## Assigned Action Items
A bulleted list where each item is formatted as: "- [Owner] Task — Due: date (if mentioned)". If none, state "No action items were assigned."

Do not invent content not present in the transcript. Keep language professional and academic.`;

async function generateSummary(transcriptText) {
  if (!process.env.GEMINI_API_KEY) {
    throw new Error('GEMINI_API_KEY is not configured on the server.');
  }
  if (!transcriptText || !transcriptText.trim()) {
    throw new Error('Transcript is empty; cannot summarize.');
  }

  const modelsToTry = [process.env.GEMINI_MODEL, 'gemini-3.6-flash', 'gemini-2.5-flash', 'gemini-flash-latest'].filter(Boolean);
  let lastError = null;

  for (const model of modelsToTry) {
    try {
      const res = await fetch(`https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${process.env.GEMINI_API_KEY}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          systemInstruction: { parts: [{ text: SUMMARY_SYSTEM_PROMPT }] },
          contents: [{ role: 'user', parts: [{ text: `Transcript:\n\n${transcriptText}` }] }],
          generationConfig: { temperature: 0.3 },
        }),
      });

      if (!res.ok) {
        const detail = await res.text();
        if (res.status === 404 && modelsToTry.indexOf(model) < modelsToTry.length - 1) {
          console.warn(`Gemini model ${model} returned 404, trying fallback...`);
          continue;
        }
        throw new Error(`Gemini request failed (${res.status}): ${detail.slice(0, 200)}`);
      }
      const data = await res.json();
      const parts = data.candidates && data.candidates[0] && data.candidates[0].content && data.candidates[0].content.parts;
      if (!parts || !parts.length) {
        throw new Error('Gemini returned no content.');
      }
      return parts.map((p) => p.text || '').join('').trim();
    } catch (err) {
      lastError = err;
      if (err.message.includes('404') && modelsToTry.indexOf(model) < modelsToTry.length - 1) {
        continue;
      }
      throw err;
    }
  }

  throw lastError || new Error('All Gemini model requests failed.');
}

// ---------------------------------------------------------------------------
// REST API routes
// ---------------------------------------------------------------------------

// Resolve the client id from the request body or header
function getClientId(req) {
  return (req.body && req.body.clientId) || req.get('X-Client-Id') || null;
}

// Raw timestamped transcript download
app.post('/api/transcript', (req, res) => {
  const clientId = getClientId(req);
  if (!clientId || !sessions.has(clientId)) {
    return res.status(404).json({ error: 'Session not found.' });
  }
  const text = buildRawTranscript(sessions.get(clientId));
  res.setHeader('Content-Type', 'text/plain; charset=utf-8');
  res.setHeader('Content-Disposition', 'attachment; filename="meeting-transcript.txt"');
  res.send(text || '(No transcript captured.)');
});

// Summary as markdown text
app.post('/api/summary/text', async (req, res) => {
  const clientId = getClientId(req);
  if (!clientId || !sessions.has(clientId)) {
    return res.status(404).json({ error: 'Session not found.' });
  }
  try {
    const transcript = buildRawTranscript(sessions.get(clientId));
    const summary = await generateSummary(transcript);
    res.json({ summary });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Summary rendered to PDF
app.post('/api/summary/pdf', async (req, res) => {
  const clientId = getClientId(req);
  if (!clientId || !sessions.has(clientId)) {
    return res.status(404).json({ error: 'Session not found.' });
  }
  try {
    const transcript = buildRawTranscript(sessions.get(clientId));
    const summary = await generateSummary(transcript);

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', 'attachment; filename="meeting-summary.pdf"');

    const doc = new PDFDocument({ bufferPages: true, size: 'LETTER', margins: { top: 72, bottom: 72, left: 72, right: 72 } });
    doc.pipe(res);

    // Title block
    doc.fontSize(20).font('Helvetica-Bold').fillColor('#1e3a5f').text('College Meeting Summary', { align: 'center' });
    doc.moveDown(0.5);
    doc.fontSize(10).font('Helvetica').fillColor('#64748b').text(`Generated ${new Date().toLocaleString()}`, { align: 'center' });
    doc.moveDown(1.5);
    doc.moveTo(72, doc.y).lineTo(doc.page.width - 72, doc.y).lineWidth(1).strokeColor('#cbd5e1').stroke();
    doc.moveDown(1);

    // Render the markdown summary into the PDF
    renderMarkdownToPdf(doc, summary);

    // Page numbers
    const range = doc.bufferedPageRange();
    for (let i = range.start; i < range.start + range.count; i++) {
      doc.switchToPage(i);
      doc.fontSize(9).font('Helvetica').fillColor('#94a3b8').text(`Page ${i + 1} of ${range.count}`, 72, doc.page.height - 50, { align: 'center' });
    }

    doc.end();
  } catch (err) {
    if (!res.headersSent) {
      res.status(500).json({ error: err.message });
    } else {
      res.end();
    }
  }
});

// Minimal markdown -> PDF renderer (headings, bullets, paragraphs, bold)
function renderMarkdownToPdf(doc, md) {
  const lines = md.split(/\r?\n/);
  const bodyFont = 'Helvetica';
  const boldFont = 'Helvetica-Bold';
  const primary = '#1e3a5f';
  const accent = '#2563eb';
  const text = '#0f172a';

  for (const raw of lines) {
    const line = raw.trimEnd();
    if (!line.trim()) {
      doc.moveDown(0.5);
      continue;
    }
    if (line.startsWith('## ')) {
      doc.moveDown(0.5).fontSize(14).font(boldFont).fillColor(primary).text(line.replace(/^##\s+/, ''), { underline: false });
      doc.moveDown(0.25);
    } else if (line.startsWith('# ')) {
      doc.moveDown(0.5).fontSize(18).font(boldFont).fillColor(primary).text(line.replace(/^#\s+/, ''));
      doc.moveDown(0.25);
    } else if (line.startsWith('- ') || line.startsWith('* ')) {
      const content = line.replace(/^[-*]\s+/, '');
      doc.moveDown(0.2).font(bodyFont).fontSize(11).fillColor(accent).text('•  ', { continued: true });
      writeRichText(doc, content, bodyFont, boldFont, text);
    } else {
      doc.moveDown(0.2).font(bodyFont).fontSize(11).fillColor(text);
      writeRichText(doc, line, bodyFont, boldFont, text);
    }
  }
}

// Writes a line with inline **bold** support
function writeRichText(doc, line, bodyFont, boldFont, color) {
  const parts = line.split(/(\*\*[^*]+\*\*)/g).filter(Boolean);
  doc.fillColor(color);
  for (const part of parts) {
    if (part.startsWith('**') && part.endsWith('**')) {
      doc.font(boldFont).text(part.slice(2, -2), { continued: true });
    } else {
      doc.font(bodyFont).text(part, { continued: true });
    }
  }
  doc.text('', { continued: false }); // end the line
}

const { spawn, execFile } = require('child_process');
const { promisify } = require('util');
const execFileAsync = promisify(execFile);

function getYtDlpPath() {
  const customPath = 'C:\\Users\\admin\\.node\\yt-dlp.exe';
  if (fs.existsSync(customPath)) return customPath;
  return 'yt-dlp';
}

function downloadMediaWithYtDlp(url) {
  return new Promise((resolve, reject) => {
    const ytDlpPath = getYtDlpPath();
    const proc = spawn(ytDlpPath, ['-o', '-', '-f', 'ba[ext=webm]/ba/b', '--no-playlist', url]);
    const chunks = [];
    let errOutput = '';
    proc.stdout.on('data', chunk => chunks.push(chunk));
    proc.stderr.on('data', data => { errOutput += data.toString(); });
    proc.on('close', code => {
      if (code === 0 && chunks.length > 0) {
        resolve({ mediaBuf: Buffer.concat(chunks), contentType: 'audio/webm' });
      } else {
        reject(new Error(`yt-dlp process failed (code ${code}): ${errOutput.slice(0, 150)}`));
      }
    });
    proc.on('error', err => reject(err));
  });
}

async function fetchMediaFromUrl(targetUrl) {
  let finalUrl = targetUrl.trim();

  // 1. Google Drive link handling
  if (finalUrl.includes('drive.google.com') || finalUrl.includes('docs.google.com')) {
    const driveMatch = finalUrl.match(/file\/d\/([a-zA-Z0-9_-]+)/) || finalUrl.match(/id=([a-zA-Z0-9_-]+)/);
    if (driveMatch && driveMatch[1]) {
      finalUrl = `https://docs.google.com/uc?export=download&confirm=no_antivirus&id=${driveMatch[1]}`;
    }
  }

  // 2. Dropbox link handling
  if (finalUrl.includes('dropbox.com')) {
    finalUrl = finalUrl.replace(/\?dl=0/, '?raw=1').replace(/([?&])dl=0/, '$1raw=1');
    if (!finalUrl.includes('raw=1') && !finalUrl.includes('dl=1')) {
      finalUrl += (finalUrl.includes('?') ? '&' : '?') + 'raw=1';
    }
  }

  // 3. Web video platform link detection (YouTube, Vimeo, Twitch, TikTok, etc.)
  const isVideoPlatform = /youtube\.com|youtu\.be|vimeo\.com|twitch\.tv|twitter\.com|x\.com|tiktok\.com|dailymotion\.com|soundcloud\.com/i.test(finalUrl);

  if (isVideoPlatform) {
    try {
      console.log(`[process-link] Downloading audio from video platform: ${finalUrl}`);
      return await downloadMediaWithYtDlp(finalUrl);
    } catch (e) {
      console.warn('[process-link] yt-dlp direct audio download fallback:', e.message);
    }
  }

  // 4. Direct link fetch fallback
  const mediaRes = await fetch(finalUrl);
  if (!mediaRes.ok) {
    throw new Error(`Failed to fetch media from URL (${mediaRes.status}).`);
  }
  let contentType = mediaRes.headers.get('content-type') || 'application/octet-stream';

  if (contentType.includes('text/html')) {
    try {
      console.log(`[process-link] HTML content type detected. Attempting yt-dlp extraction: ${finalUrl}`);
      return await downloadMediaWithYtDlp(finalUrl);
    } catch (e) {
      throw new Error('This URL points to a webpage (e.g. YouTube). Please use a direct media link (.mp3, .mp4, .webm, Google Drive, Dropbox) or use the "Upload Video File" tab.');
    }
  }

  const mediaBuf = Buffer.from(await mediaRes.arrayBuffer());
  if (!mediaBuf || mediaBuf.length === 0) {
    throw new Error('Could not retrieve audio/video data from the provided link.');
  }

  return { mediaBuf, contentType };
}

// ---------------------------------------------------------------------------
// "Paste Video Link" -> server-side fetch + Deepgram pre-recorded API
// ---------------------------------------------------------------------------
app.post('/api/process-link', async (req, res) => {
  const { url } = req.body || {};
  if (!url || typeof url !== 'string') {
    return res.status(400).json({ error: 'A "url" field is required.' });
  }
  if (!process.env.DEEPGRAM_API_KEY) {
    return res.status(500).json({ error: 'DEEPGRAM_API_KEY is not configured on the server.' });
  }

  try {
    const isYouTube = /youtube\.com|youtu\.be/i.test(url);
    if (isYouTube) {
      try {
        console.log(`[process-link] Fetching YouTube transcript for: ${url}`);
        const items = await YoutubeTranscript.fetchTranscript(url);
        if (items && items.length > 0) {
          const clientId = makeClientId();
          const session = ensureSession(clientId);
          const textParts = [];
          for (const item of items) {
            const cleanText = (item.text || '')
              .replace(/\n/g, ' ')
              .replace(/&amp;/g, '&')
              .replace(/&quot;/g, '"')
              .replace(/&#39;/g, "'")
              .trim();
            if (cleanText) {
              session.transcript.push({ t: (item.offset / 1000) || 0, text: cleanText });
              textParts.push(cleanText);
            }
          }
          const fullTranscript = textParts.join(' ');
          return res.json({ clientId, transcript: fullTranscript });
        }
      } catch (ytErr) {
        console.warn('[process-link] YoutubeTranscript error, falling back:', ytErr.message);
      }
    }

    const { mediaBuf, contentType } = await fetchMediaFromUrl(url);


    const dgRes = await fetch('https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true&punctuate=true', {
      method: 'POST',
      headers: {
        Authorization: `Token ${process.env.DEEPGRAM_API_KEY}`,
        'Content-Type': contentType === 'application/octet-stream' ? 'audio/webm' : contentType,
      },
      body: mediaBuf,
    });

    if (!dgRes.ok) {
      const detail = await dgRes.text();
      return res.status(502).json({ error: `Deepgram error (${dgRes.status}): ${detail.slice(0, 200)}` });
    }
    const data = await dgRes.json();
    const words = data.results && data.results.channels && data.results.channels[0].alternatives[0].words;
    const transcript =
      (data.results && data.results.channels && data.results.channels[0].alternatives[0].transcript) || '';

    // Cache as a session so the export endpoints can use it
    const clientId = makeClientId();
    const session = ensureSession(clientId);
    if (Array.isArray(words)) {
      for (const w of words) {
        session.transcript.push({ t: w.start || 0, text: w.punctuated_word || w.word || '' });
      }
    } else {
      session.transcript.push({ t: 0, text: transcript });
    }

    res.json({ clientId, transcript });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// "Upload Video File" -> accept multipart-ish raw body, transcribe via Deepgram pre-recorded
// ---------------------------------------------------------------------------
app.post('/api/process-file', async (req, res) => {
  if (!process.env.DEEPGRAM_API_KEY) {
    return res.status(500).json({ error: 'DEEPGRAM_API_KEY is not configured on the server.' });
  }
  const buf = req.body;
  if (!Buffer.isBuffer(buf) || !buf.length) {
    return res.status(400).json({ error: 'No file body received or file body is empty.' });
  }
  let contentType = req.get('Content-Type') || '';
  if (!contentType || contentType === 'application/octet-stream') {
    contentType = 'video/mp4';
  }

  try {
    const dgRes = await fetch('https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true&punctuate=true', {
      method: 'POST',
      headers: {
        Authorization: `Token ${process.env.DEEPGRAM_API_KEY}`,
        'Content-Type': contentType,
      },
      body: buf,
    });
    if (!dgRes.ok) {
      const detail = await dgRes.text();
      return res.status(502).json({ error: `Deepgram error (${dgRes.status}): ${detail.slice(0, 200)}` });
    }
    const data = await dgRes.json();
    const words = data.results && data.results.channels && data.results.channels[0].alternatives[0].words;
    const transcript =
      (data.results && data.results.channels && data.results.channels[0].alternatives[0].transcript) || '';

    const clientId = makeClientId();
    const session = ensureSession(clientId);
    if (Array.isArray(words)) {
      for (const w of words) {
        session.transcript.push({ t: w.start || 0, text: w.punctuated_word || w.word || '' });
      }
    } else {
      session.transcript.push({ t: 0, text: transcript });
    }

    res.json({ clientId, transcript });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------
if (require.main === module) {
  const PORT = process.env.PORT || 3000;
  server.listen(PORT, () => {
    console.log(`College Meeting Transcriber running on http://localhost:${PORT}`);
    if (!process.env.DEEPGRAM_API_KEY) console.warn('  WARNING: DEEPGRAM_API_KEY is not set. Live streaming will not work.');
    if (!process.env.GEMINI_API_KEY) console.warn('  WARNING: GEMINI_API_KEY is not set. Summarization will not work.');
  });
}

module.exports = app;

