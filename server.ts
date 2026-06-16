import express from "express";
import path from "path";
import { createServer as createViteServer } from "vite";
import { Agent, setGlobalDispatcher } from "undici";
import dns from "dns";

// Prefer IPv4 during DNS resolution to avoid timeout/failure on hybrid dual-stack container interfaces
dns.setDefaultResultOrder("ipv4first");

// Set high-performance public DNS servers (Cloudflare and Google DNS) to optimize resolution speed
try {
  dns.setServers([
    "1.1.1.1", // Cloudflare DNS (Primary)
    "8.8.8.8", // Google DNS (Primary)
    "1.0.0.1", // Cloudflare DNS (Secondary)
    "8.8.4.4"  // Google DNS (Secondary)
  ]);
  console.log("⚡ High-speed DNS configured globally: Cloudflare & Google DNS are active.");
} catch (dnsErr) {
  console.warn("⚠️ Custom DNS server setup warning, falling back to default DNS:", dnsErr);
}

// Disable SSL certificate verification to prevent issues on custom IPTV CDN streams with untrusted or expired certificates
process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

// Configure undici globally to ignore self-signed certificates or expired TLS handshakes
const undiciAgent = new Agent({
  connect: {
    rejectUnauthorized: false,
  }
});
setGlobalDispatcher(undiciAgent);

// Helper for robust stream fetching with cascading header configurations and fallback strategies on connection failures
async function robustFetch(targetUrl: string, timeoutMs = 8000): Promise<Response> {
  const isGpcdn = targetUrl.includes('gpcdn.net') || targetUrl.includes('akash') || targetUrl.includes('toffee') || targetUrl.includes('bpk-tv');
  const isAynaott = targetUrl.includes('aynaott.com') || targetUrl.includes('aynaott');

  const configs = [
    // Configuration 1: Best guess based on target URL domain
    {
      'User-Agent': isGpcdn 
        ? 'Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Mobile Safari/537.36'
        : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      ...(isGpcdn ? {
        'Referer': 'https://toffeelive.com/',
        'Origin': 'https://toffeelive.com'
      } : isAynaott ? {
        'Referer': 'https://aynaott.com/',
        'Origin': 'https://aynaott.com'
      } : {
        'Referer': targetUrl
      })
    },
    // Configuration 2: Plain minimalist browser headers (bypasses referer check strictness)
    {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
      'Accept': '*/*',
      'Accept-Language': 'en-US,en;q=0.9',
    },
    // Configuration 3: Firefox safe headers
    {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0',
      'Referer': isGpcdn ? 'https://aynaott.com/' : 'https://toffeelive.com/',
      'Origin': isGpcdn ? 'https://aynaott.com' : 'https://toffeelive.com'
    }
  ];

  let lastError: any = null;
  for (let i = 0; i < configs.length; i++) {
    try {
      const response = await fetch(targetUrl, {
        signal: AbortSignal.timeout(timeoutMs),
        headers: configs[i] as any,
        dispatcher: undiciAgent
      } as any);
      if (response.ok) {
        return response;
      }
      lastError = new Error(`HTTP status ${response.status} ${response.statusText}`);
    } catch (err: any) {
      lastError = err;
      console.log(`info: Proxy stream request status update - hop ${i + 1}`);
    }
  }
  throw lastError || new Error(`Could not load stream from target`);
}

async function startServer() {
  const app = express();
  const PORT = 3000;

  // Add robust streaming live proxy route to bypass HTTPS Mixed Content blocks
  app.get("/api/stream-proxy", async (req, res) => {
    const targetUrl = req.query.url as string;
    if (!targetUrl) {
      res.status(400).send("Missing target url parameter");
      return;
    }

    try {
      // Fetch stream chunk or m3u8 playlist using the robust cascading proxy fetcher
      const response = await robustFetch(targetUrl, 8000);

      const finalUrl = response.url || targetUrl;
      const contentType = response.headers.get('content-type') || '';
      const isM3U8 = targetUrl.toLowerCase().includes('.m3u8') || 
                     finalUrl.toLowerCase().includes('.m3u8') || 
                     contentType.includes('mpegurl') || 
                     contentType.includes('mpegURL');

      const origin = req.headers.origin || '*';
      res.setHeader('Access-Control-Allow-Origin', origin);
      res.setHeader('Access-Control-Allow-Credentials', 'true');
      res.setHeader('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Range');

      if (isM3U8) {
        const text = await response.text();
        // Resolve lines
        const lines = text.split(/\r?\n/);
        const rewrittenLines = lines.map(line => {
          const trimmed = line.trim();
          if (trimmed === '') return line;

          if (!trimmed.startsWith('#')) {
            try {
              const resolved = new URL(trimmed, finalUrl).toString();
              return `/api/stream-proxy?url=${encodeURIComponent(resolved)}`;
            } catch (err) {
              return line;
            }
          }

          // Handle any #EXT directives that contain a URI attribute, such as #EXT-X-KEY, #EXT-X-MAP, #EXT-X-MEDIA, etc.
          if (trimmed.startsWith('#EXT')) {
            return trimmed.replace(/URI="([^"]+)"/g, (_, p1) => {
              try {
                const resolved = new URL(p1, finalUrl).toString();
                return `URI="/api/stream-proxy?url=${encodeURIComponent(resolved)}"`;
              } catch (err) {
                return `URI="${p1}"`;
              }
            });
          }

          return line;
        });

        res.setHeader('Content-Type', 'application/vnd.apple.mpegurl');
        res.send(rewrittenLines.join('\n'));
      } else {
        // Direct media pipeline (TS fragments, chunks, audio, subtitles keys, etc.)
        if (response.headers.get('content-type')) {
          res.setHeader('Content-Type', response.headers.get('content-type')!);
        }
        if (response.headers.get('content-length')) {
          res.setHeader('Content-Length', response.headers.get('content-length')!);
        }

        // Stream reader loop to pipe response chunks
        if (response.body) {
          const reader = (response.body as any).getReader();
          let closed = false;

          req.on('close', () => {
            closed = true;
            try {
              reader.cancel();
            } catch (e) {}
          });

          while (!closed) {
            const { done, value } = await reader.read();
            if (done || closed) break;
            res.write(value);
          }
          if (!closed) {
            res.end();
          }
        } else {
          res.status(502).send("Active content stream empty");
        }
      }
    } catch (err: any) {
      if (res.headersSent) {
        console.log(`info: Stream proxy transmission completed or client disconnected.`);
        try {
          res.end();
        } catch (_) {}
        return;
      }
      const isTimeout = err.name === 'TimeoutError' || err.message?.includes('timeout') || err.message?.includes('Timeout');
      if (isTimeout) {
        console.log(`info: Connection limit exceeded while fetching stream content for target URL`);
        res.status(504).send(`Error: Stream connection timed out (Offline feed).`);
      } else {
        console.log(`info: Stream source retrieval finished.`);
        res.status(502).send(`Error: Failed to stream from host (${err.message || 'unknown'}).`);
      }
    }
  });

  // Health check endpoint
  app.get("/api/health", (req, res) => {
    res.json({ status: "ok" });
  });

  // Vite development middleware vs Static Production server
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server starting on port ${PORT}`);
  });
}

startServer();
