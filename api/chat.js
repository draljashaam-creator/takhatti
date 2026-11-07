// Takhatti chat endpoint: forwards the conversation to Claude and streams the reply back.
// The API key lives only in Vercel (Settings → Environment Variables → ANTHROPIC_API_KEY).
const MODEL = process.env.ANTHROPIC_MODEL || 'claude-haiku-5-5';
const MAX_MESSAGES = 20;          // keep each request small
const MAX_CHARS = 4000;           // per message
const MAX_SYSTEM_CHARS = 12000;
const RATE_LIMIT = 40;            // messages per visitor…
const RATE_WINDOW_MS = 10 * 60 * 1000; // …every 10 minutes (best effort)
const hits = new Map();

// Only the تخطّي app itself may use this endpoint (blocks casual misuse of the API credit).
function allowedOrigin(req) {
  const src = req.headers.origin || req.headers.referer || '';
  let host = '';
  try { host = new URL(src).hostname; } catch { return false; }
  const extra = (process.env.ALLOWED_HOSTS || '').split(',').map(s => s.trim()).filter(Boolean);
  return host === 'takhatti.vercel.app' || /^takhatti(-[a-z0-9-]+)?\.vercel\.app$/.test(host) || host === 'localhost' || extra.includes(host);
}
function rateLimited(req) {
  const ip = String(req.headers['x-forwarded-for'] || req.socket?.remoteAddress || 'x').split(',')[0].trim();
  const now = Date.now();
  const list = (hits.get(ip) || []).filter(t => now - t < RATE_WINDOW_MS);
  list.push(now); hits.set(ip, list);
  if (hits.size > 5000) hits.clear();
  return list.length > RATE_LIMIT;
}

export default async function handler(req, res) {
  if (req.method !== 'POST') { res.status(405).json({ error: 'POST only' }); return; }
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) { res.status(500).json({ error: 'missing key', code: 'sampling_disabled' }); return; }
  if (!allowedOrigin(req)) { res.status(403).json({ error: 'forbidden', code: 'error' }); return; }
  if (rateLimited(req)) { res.status(429).json({ error: 'slow down', code: 'rate_limited' }); return; }

  let body = req.body;
  if (typeof body === 'string') { try { body = JSON.parse(body); } catch { body = {}; } }
  const system = String((body && body.system) || '').slice(0, MAX_SYSTEM_CHARS);
  let messages = Array.isArray(body && body.messages) ? body.messages : [];
  messages = messages
    .filter(m => m && (m.role === 'user' || m.role === 'assistant') && typeof m.content === 'string')
    .slice(-MAX_MESSAGES)
    .map(m => ({ role: m.role, content: m.content.slice(0, MAX_CHARS) }));
  while (messages.length && messages[0].role !== 'user') messages.shift();
  if (!messages.length) { res.status(400).json({ error: 'no messages' }); return; }
  if (!system.includes('تخطّي')) { res.status(403).json({ error: 'forbidden', code: 'error' }); return; }

  const upstream = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({ model: MODEL, max_tokens: 1024, system: system || undefined, messages, stream: true }),
  });

  if (!upstream.ok || !upstream.body) {
    const status = upstream.status;
    res.status(status === 429 ? 429 : 502).json({ error: 'upstream ' + status, code: status === 429 ? 'rate_limited' : 'error' });
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
  const reader = upstream.body.getReader();
  const dec = new TextDecoder();
  let buf = '';
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += dec.decode(value, { stream: true });
    let i;
    while ((i = buf.indexOf('\n')) >= 0) {
      const line = buf.slice(0, i).trim();
      buf = buf.slice(i + 1);
      if (!line.startsWith('data:')) continue;
      try {
        const ev = JSON.parse(line.slice(5));
        if (ev.type === 'content_block_delta' && ev.delta && ev.delta.type === 'text_delta') res.write(ev.delta.text);
      } catch { /* ignore keep-alives */ }
    }
  }
  res.end();
}
