/**
 * XHTTPRelayECO v2.0 - Working V2Ray/VLESS Relay
 * Vercel Edge Function for proper protocol support
 * 
 * Pre-configured for:
 * - Target: http://vercel.parsashonam.sbs:2096
 * - Path: /p4r34m
 */

import { PassThrough, Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";

export const config = {
  api: { bodyParser: false },
  supportsResponseStreaming: true,
  maxDuration: 10,
};

// Configuration
const TARGET_BASE = "http://vercel.parsashonam.sbs:2096";
const RELAY_PATH = "/p4r34m";
const RELAY_KEY = process.env.RELAY_KEY || ""; // Optional authentication
const UPSTREAM_TIMEOUT_MS = 30000;
const MAX_INFLIGHT = 16;
const MAX_UP_BPS = 1048576; // 2.5 MB/s upload
const MAX_DOWN_BPS = 1048576; // 2.5 MB/s download

const PLATFORM_HEADER_PREFIX = "x-vercel-";
const ALLOWED_METHODS = new Set(["GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"]);

const FORWARD_HEADER_EXACT = new Set([
  "accept",
  "accept-encoding",
  "accept-language",
  "cache-control",
  "content-length",
  "content-type",
  "pragma",
  "range",
  "referer",
  "user-agent",
  "upgrade",
  "sec-websocket-key",
  "sec-websocket-version",
  "sec-websocket-extensions",
  "sec-websocket-protocol",
]);

const FORWARD_HEADER_PREFIXES = ["sec-ch-", "sec-fetch-", "sec-websocket-"];

const STRIP_HEADERS = new Set([
  "host",
  "connection",
  "proxy-connection",
  "keep-alive",
  "via",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "forwarded",
  "x-forwarded-host",
  "x-forwarded-proto",
  "x-forwarded-port",
]);

let inFlight = 0;
const GLOBAL_UPLOAD_LIMITER = createGlobalLimiter(MAX_UP_BPS);
const GLOBAL_DOWNLOAD_LIMITER = createGlobalLimiter(MAX_DOWN_BPS);

export default async function handler(req, res) {
  const requestId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  const startedAt = Date.now();
  let slotAcquired = false;

  try {
    // Get request path
    const host = req.headers.host || "localhost";
    const url = new URL(req.url || "/", `https://${host}`);
    const pathname = normalizeIncomingPath(url.pathname);

    // Check if path matches /p4r34m or /p4r34m/*
    if (!isAllowedRelayPath(pathname)) {
      res.statusCode = 404;
      return res.end("Not Found");
    }

    // Check method
    if (!ALLOWED_METHODS.has(req.method)) {
      res.statusCode = 405;
      res.setHeader("allow", "GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH");
      return res.end("Method Not Allowed");
    }

    // Check authentication if RELAY_KEY is set
    if (RELAY_KEY && RELAY_KEY.length >= 16) {
      const token = (req.headers["x-relay-key"] || "").toString();
      if (token !== RELAY_KEY) {
        res.statusCode = 403;
        return res.end("Forbidden");
      }
    }

    // Check inflight limit
    if (!tryAcquireSlot()) {
      res.statusCode = 503;
      res.setHeader("retry-after", "1");
      return res.end("Server Busy");
    }
    slotAcquired = true;

    // Build upstream URL
    const upstreamPath = mapPublicPathToRelayPath(pathname);
    const targetUrl = `${TARGET_BASE}${upstreamPath}${url.search || ""}`;

    // Build headers
    const headers = {};
    const clientIp = toHeaderValue(req.headers["x-real-ip"] || req.headers["x-forwarded-for"]);
    
    for (const key of Object.keys(req.headers)) {
      const lower = key.toLowerCase();
      const value = req.headers[key];
      
      if (STRIP_HEADERS.has(lower)) continue;
      if (lower.startsWith(PLATFORM_HEADER_PREFIX)) continue;
      if (lower === "x-relay-key") continue;
      if (!shouldForwardHeader(lower)) continue;
      
      const normalizedValue = toHeaderValue(value);
      if (normalizedValue) headers[lower] = normalizedValue;
    }
    
    if (clientIp) headers["x-forwarded-for"] = clientIp;

    // Handle request body
    const hasBody = req.method !== "GET" && req.method !== "HEAD";
    const abortCtrl = new AbortController();
    let hitUpstreamTimeout = false;
    
    const timeoutRef = setTimeout(() => {
      hitUpstreamTimeout = true;
      try {
        abortCtrl.abort();
      } catch {}
    }, UPSTREAM_TIMEOUT_MS);

    let requestErrorHandler = null;
    let uploadErrorHandler = null;
    let uploadNodeStream = null;

    try {
      const fetchOpts = {
        method: req.method,
        headers,
        redirect: "manual",
        signal: abortCtrl.signal,
      };

      if (hasBody) {
        uploadNodeStream = GLOBAL_UPLOAD_LIMITER
          ? req.pipe(createThrottleTransform(GLOBAL_UPLOAD_LIMITER))
          : req;

        requestErrorHandler = (streamErr) => {
          if (isUpstreamTimeoutError(streamErr)) return;
          console.error("Upload request error:", {
            requestId,
            method: req.method,
            error: String(streamErr),
          });
        };
        req.on("error", requestErrorHandler);

        uploadErrorHandler = (streamErr) => {
          if (isUpstreamTimeoutError(streamErr)) return;
          console.error("Upload stream error:", {
            requestId,
            method: req.method,
            error: String(streamErr),
          });
        };
        
        if (uploadNodeStream && uploadNodeStream !== req) {
          uploadNodeStream.on("error", uploadErrorHandler);
        }

        fetchOpts.body = Readable.toWeb(uploadNodeStream);
        fetchOpts.duplex = "half";
      }

      // Fetch from upstream
      const upstream = await fetch(targetUrl, fetchOpts);

      // Copy response status and headers
      res.statusCode = upstream.status;
      for (const [headerName, headerValue] of upstream.headers) {
        const k = headerName.toLowerCase();
        if (k === "transfer-encoding" || k === "connection") continue;
        try {
          res.setHeader(headerName, headerValue);
        } catch {}
      }

      // Stream response body
      if (!upstream.body) {
        res.end();
      } else {
        const upstreamNode = Readable.fromWeb(upstream.body);
        const downloadStream = GLOBAL_DOWNLOAD_LIMITER
          ? upstreamNode.pipe(createThrottleTransform(GLOBAL_DOWNLOAD_LIMITER))
          : upstreamNode;
        await pipeline(downloadStream, res);
      }

      const durationMs = Date.now() - startedAt;
      console.log("Relay success:", {
        requestId,
        path: pathname,
        upstreamPath,
        method: req.method,
        status: upstream.status,
        durationMs,
      });
    } finally {
      clearTimeout(timeoutRef);
      if (requestErrorHandler) req.off("error", requestErrorHandler);
      if (uploadNodeStream && uploadNodeStream !== req && uploadErrorHandler) {
        uploadNodeStream.off("error", uploadErrorHandler);
      }
    }
  } catch (err) {
    const durationMs = Date.now() - startedAt;
    
    if (hitUpstreamTimeout || isUpstreamTimeoutError(err)) {
      console.error("Relay timeout:", {
        requestId,
        method: req.method,
        durationMs,
        timeoutMs: UPSTREAM_TIMEOUT_MS,
      });
      if (!res.headersSent) {
        res.statusCode = 504;
        return res.end("Gateway Timeout");
      }
      return;
    }

    console.error("Relay error:", {
      requestId,
      method: req.method,
      durationMs,
      error: String(err),
      stack: err.stack,
    });
    
    if (!res.headersSent) {
      res.statusCode = 502;
      return res.end("Bad Gateway");
    }
  } finally {
    if (slotAcquired) releaseSlot();
  }
}

function shouldForwardHeader(headerName) {
  if (FORWARD_HEADER_EXACT.has(headerName)) return true;
  for (const prefix of FORWARD_HEADER_PREFIXES) {
    if (headerName.startsWith(prefix)) return true;
  }
  return false;
}

function isAllowedRelayPath(pathname) {
  return pathname === RELAY_PATH || pathname.startsWith(`${RELAY_PATH}/`);
}

function mapPublicPathToRelayPath(pathname) {
  if (pathname === RELAY_PATH) return RELAY_PATH;
  return pathname; // Keep the full path including /p4r34m
}

function normalizeIncomingPath(pathname) {
  if (!pathname) return "/";
  let normalized = String(pathname).replace(/\/{2,}/g, "/");
  if (!normalized.startsWith("/")) normalized = `/${normalized}`;
  if (normalized.length > 1 && normalized.endsWith("/")) {
    normalized = normalized.slice(0, -1);
  }
  return normalized;
}

function toHeaderValue(value) {
  if (!value) return "";
  return Array.isArray(value) ? value.join(", ") : String(value);
}

function tryAcquireSlot() {
  if (inFlight >= MAX_INFLIGHT) return false;
  inFlight += 1;
  return true;
}

function releaseSlot() {
  inFlight = Math.max(0, inFlight - 1);
}

function isUpstreamTimeoutError(err) {
  if (!err) return false;
  if (err?.name === "AbortError") return true;
  if (err?.code === "ABORT_ERR") return true;
  if (err?.message === "upstream_timeout") return true;
  if (err?.cause?.message === "upstream_timeout") return true;
  if (typeof err === "string" && err === "upstream_timeout") return true;
  return false;
}

function createGlobalLimiter(bytesPerSecond) {
  if (!Number.isFinite(bytesPerSecond) || bytesPerSecond <= 0) return null;

  const burstCap = Math.max(bytesPerSecond, 262144);
  let tokens = burstCap;
  let lastRefill = Date.now();
  const queue = [];
  let timer = null;

  function refill() {
    const now = Date.now();
    const elapsedMs = now - lastRefill;
    if (elapsedMs <= 0) return;
    const refillAmount = (elapsedMs * bytesPerSecond) / 1000;
    tokens = Math.min(burstCap, tokens + refillAmount);
    lastRefill = now;
  }

  function tryDrain() {
    refill();
    while (queue.length > 0 && tokens >= 1) {
      const item = queue[0];
      const grant = Math.min(item.maxBytes, Math.max(1, Math.floor(tokens)));
      if (grant < 1) break;
      tokens -= grant;
      queue.shift();
      item.resolve(grant);
    }
  }

  function schedule() {
    if (timer) return;
    timer = setTimeout(() => {
      timer = null;
      tryDrain();
      if (queue.length > 0) schedule();
    }, 5);
  }

  return {
    acquire(maxBytes) {
      const requested = Math.max(1, Math.trunc(maxBytes || 1));
      return new Promise((resolve) => {
        queue.push({ maxBytes: requested, resolve });
        tryDrain();
        if (queue.length > 0) schedule();
      });
    },
  };
}

function createThrottleTransform(limiter) {
  if (!limiter) return new PassThrough();

  return new Transform({
    transform(chunk, _encoding, callback) {
      if (!chunk || chunk.length === 0) {
        callback();
        return;
      }

      (async () => {
        let offset = 0;
        while (offset < chunk.length) {
          const maxBytes = chunk.length - offset;
          const grant = await limiter.acquire(maxBytes);
          const piece = chunk.subarray(offset, offset + grant);
          offset += grant;
          this.push(piece);
        }
      })()
        .then(() => callback())
        .catch((err) => callback(err));
    },
  });
}

// Made with Bob
