import 'dotenv/config';
import express from 'express';
import { createServer as createViteServer } from 'vite';
import path from 'path';
import WebSocket from 'ws';
import crypto from 'crypto';

const app = express();
const PORT = 3000;

// ==========================================
// CONFIGURATION
// ==========================================
const MIN_API_GATE_SIZE = 5000;
const URGENCY_WINDOW_HOURS = 24;
const LOW_PROB_MAX = 0.40;
const HIGH_PROB_MIN = 0.85;
const NEW_ACCOUNT_DAYS = 7;
const TOP_MARKETS = 50;
const BATCH_SIZE = 100;

const DISCORD_WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || "";
const POLYGONSCAN_API_KEY = process.env.POLYGONSCAN_API_KEY || "";

// ==========================================
// STATE
// ==========================================
const ageCache = new Map<string, number>();
const txCache = new Map<string, string>();

class RateLimiter {
  private queue: (() => Promise<void>)[] = [];
  private isProcessing = false;
  private delayMs: number;

  constructor(requestsPerSecond: number) {
    this.delayMs = 1000 / requestsPerSecond;
  }

  async enqueue<T>(fn: () => Promise<T>): Promise<T> {
    return new Promise((resolve, reject) => {
      this.queue.push(async () => {
        try {
          resolve(await fn());
        } catch (e) {
          reject(e);
        }
      });
      this.processQueue();
    });
  }

  private async processQueue() {
    if (this.isProcessing) return;
    this.isProcessing = true;
    while (this.queue.length > 0) {
      const task = this.queue.shift();
      if (task) {
        await task();
        await new Promise(r => setTimeout(r, this.delayMs));
      }
    }
    this.isProcessing = false;
  }
}

// Polygonscan free tier allows 5 requests per second. We use 4 to be safe.
const polygonscanLimiter = new RateLimiter(4);

let assetsToMonitor: string[] = [];
let marketNames = new Map<string, string>();
let marketEndDates = new Map<string, number>();
let activeWs: WebSocket | null = null;
let isShuttingDown = false;
let isMonitoring = true;
let refreshInterval: NodeJS.Timeout | null = null;
let stats = { totalEvents: 0, tradeEvents: 0, activeAssets: 0 };

// Store recent alerts for new SSE clients
const recentAlerts: any[] = [];
const MAX_RECENT_ALERTS = 100;

// SSE Clients
const sseClients = new Set<express.Response>();

function broadcastAlert(alert: any) {
  const existingIdx = recentAlerts.findIndex(a => a.id === alert.id);
  if (existingIdx >= 0) {
    recentAlerts[existingIdx] = { ...recentAlerts[existingIdx], ...alert };
  } else {
    recentAlerts.unshift(alert);
    if (recentAlerts.length > MAX_RECENT_ALERTS) {
      recentAlerts.pop();
    }
  }
  
  const data = `data: ${JSON.stringify(alert)}\n\n`;
  for (const client of sseClients) {
    client.write(data);
  }
}

function broadcastStatus(status: { connected: boolean, message: string }) {
  const data = `event: status\ndata: ${JSON.stringify(status)}\n\n`;
  for (const client of sseClients) {
    client.write(data);
  }
}

function broadcastStats() {
  const data = `event: stats\ndata: ${JSON.stringify(stats)}\n\n`;
  for (const client of sseClients) {
    client.write(data);
  }
}

// ==========================================
// HELPERS
// ==========================================

async function sendDiscordAlert(message: string) {
  if (!DISCORD_WEBHOOK_URL) return;
  try {
    await fetch(DISCORD_WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: message }),
      signal: AbortSignal.timeout(5000)
    });
  } catch (e) {
    console.error(`  ⚠️  Discord error:`, e);
  }
}

async function unmaskWallet(txHash: string, retries = 3): Promise<string> {
  if (!txHash || !POLYGONSCAN_API_KEY) return "Unknown";
  if (txCache.has(txHash)) return txCache.get(txHash)!;

  const url = `https://api.etherscan.io/v2/api?chainid=137&module=proxy&action=eth_getTransactionByHash&txhash=${txHash}&apikey=${POLYGONSCAN_API_KEY}`;
  
  for (let i = 0; i < retries; i++) {
    try {
      const res = await polygonscanLimiter.enqueue(() => fetch(url, { signal: AbortSignal.timeout(10000) }));
      const data = await res.json() as any;
      
      if (data?.result?.from) {
        txCache.set(txHash, data.result.from);
        return data.result.from;
      }
      
      // If result is null, the transaction might not be indexed by Polygonscan yet.
      // Wait 2 seconds before retrying.
      if (i < retries - 1) {
        await new Promise(r => setTimeout(r, 2000));
      }
    } catch (e) {
      // On network errors or timeouts, wait and retry
      if (i < retries - 1) {
        await new Promise(r => setTimeout(r, 2000));
      }
    }
  }
  
  return "Unknown";
}

async function getWalletAge(addr: string): Promise<number> {
  if (!addr || addr === "Unknown" || !POLYGONSCAN_API_KEY) return 999;
  if (ageCache.has(addr)) return ageCache.get(addr)!;
  
  const url = `https://api.etherscan.io/v2/api?chainid=137&module=account&action=txlist&address=${addr}&startblock=0&endblock=99999999&page=1&offset=1&sort=asc&apikey=${POLYGONSCAN_API_KEY}`;
  try {
    const res = await polygonscanLimiter.enqueue(() => fetch(url, { signal: AbortSignal.timeout(10000) }));
    const data = await res.json() as any;
    if (data?.status === "1" && data?.result?.length > 0) {
      const firstTxTime = parseInt(data.result[0].timeStamp, 10);
      const age = (Date.now() / 1000 - firstTxTime) / 86400;
      ageCache.set(addr, age);
      return age;
    }
  } catch (e) {
    // Ignore
  }
  return 999;
}

async function getBookAbsorption(assetId: string, tradeSide: string, tradeSize: number): Promise<[number, number]> {
  const url = `https://clob.polymarket.com/book?token_id=${assetId}`;
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(5000) });
    const data = await res.json() as any;
    const targetBook = tradeSide.toUpperCase() === "BUY" ? data.asks : data.bids;
    if (!targetBook || !Array.isArray(targetBook)) return [0.0, 0.0];
    
    let currentDepth = 0;
    for (const level of targetBook) {
      currentDepth += parseFloat(level.size || "0");
    }
    const preTradeDepth = currentDepth + tradeSize;
    if (preTradeDepth === 0) return [0.0, 0.0];
    return [tradeSize / preTradeDepth, preTradeDepth];
  } catch (e) {
    return [0.0, 0.0];
  }
}

function parseIsoDate(dateStr: string): number {
  if (!dateStr) return 9999999999;
  try {
    const cleanStr = dateStr.replace("Z", "+00:00");
    return new Date(cleanStr).getTime() / 1000;
  } catch (e) {
    return 9999999999;
  }
}

// ==========================================
// MARKET FETCHING
// ==========================================

async function fetchTopAssets(n = TOP_MARKETS) {
  console.log(`[${new Date().toLocaleTimeString()}] 📡 Fetching fresh top ${n} markets from Gamma API...`);
  const newAssets: string[] = [];
  const newNames = new Map<string, string>();
  const newEndDates = new Map<string, number>();

  try {
    const url = `https://gamma-api.polymarket.com/markets?limit=${n}&active=true&closed=false&order=volume24hr&ascending=false`;
    const res = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0" }, signal: AbortSignal.timeout(10000) });
    const markets = await res.json() as any[];

    for (const m of markets) {
      const title = m.question || "Unknown Market";
      const rawIds = m.clobTokenIds || "[]";
      const endTimestamp = parseIsoDate(m.endDate);
      
      let outcomesList: string[] = ["Yes", "No"];
      try {
        outcomesList = typeof m.outcomes === 'string' ? JSON.parse(m.outcomes) : m.outcomes;
      } catch (e) {}

      let tokenIds: string[] = [];
      try {
        tokenIds = typeof rawIds === 'string' ? JSON.parse(rawIds) : rawIds;
      } catch (e) {}

      for (let i = 0; i < tokenIds.length; i++) {
        const tid = String(tokenIds[i]);
        const outcomeName = i < outcomesList.length ? outcomesList[i] : "Unknown Side";
        
        newAssets.push(tid);
        newNames.set(tid, `${title} [${outcomeName}]`);
        newEndDates.set(tid, endTimestamp);
      }
    }
    console.log(`✅ Extracted ${newAssets.length} active token IDs with outcome mapping.`);
    
    assetsToMonitor = newAssets;
    marketNames = newNames;
    marketEndDates = newEndDates;
    stats.activeAssets = newAssets.length;
    broadcastStats();
  } catch (e) {
    console.error(`⚠️  Error fetching markets:`, e);
  }
}

// ==========================================
// WORKER
// ==========================================

async function processTrade(alertId: string, assetId: string, side: string, price: number, size: number, usdAmount: number, txHash: string, marketTitle: string) {
  try {
    const [absorptionPct, preTradeDepth] = await getBookAbsorption(assetId, side, size);
    
    const wallet = await unmaskWallet(txHash);
    const age = await getWalletAge(wallet);
    
    const endTimestamp = marketEndDates.get(assetId) || 9999999999;
    const hoursToResolution = (endTimestamp - (Date.now() / 1000)) / 3600;

    if (age > NEW_ACCOUNT_DAYS) {
      console.log(`  ⏭️ Skipped: Wallet ${wallet} is ${age.toFixed(1)} days old.`);
      broadcastAlert({
        id: alertId,
        wallet,
        walletAgeDays: age,
        absorptionPct,
        hoursToResolution,
        status: 'skipped'
      });
      return;
    }

    console.log(`[${new Date().toLocaleTimeString()}] 🎯 WALLET PASSED: ${(absorptionPct*100).toFixed(1)}% of book eaten | ${side} @ ${price.toFixed(3)} | ${marketTitle}`);
    
    let score = 1;
    let timeWarning = "";
    
    if (hoursToResolution <= URGENCY_WINDOW_HOURS) {
      score += 2;
      timeWarning = `\n⏰ **URGENCY:** Market resolves in ${Math.max(0, hoursToResolution).toFixed(1)} hours!`;
    }
    
    if (absorptionPct >= 0.025) score += 1;
    if (age <= 1) score += 1;

    let header = absorptionPct >= 0.025 ? "🚨 **Urgent Liquidity Sweep**" : "🚨 **New Wallet Anomaly**";
    if (score >= 4) header = "🔥 **HIGH CONVICTION INSIDER ALERT** 🔥";

    const msg = `${header}\n` +
      `📈 **Score:** ${score}/5 Conviction Level\n` +
      `📋 **Market:** ${marketTitle}${timeWarning}\n` +
      `📊 **Absorption:** \`${(absorptionPct*100).toFixed(1)}%\` of resting order book eaten\n` +
      `💰 **Size:** $${usdAmount.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})} USDC\n` +
      `🎯 **Price:** ${price.toFixed(3)}  |  **Side:** ${side}\n` +
      `🕵️ **Wallet:** \`${wallet}\` (${age.toFixed(1)} days old)\n` +
      `🔗 **Tx:** \`${txHash}\``;
      
    await sendDiscordAlert(msg);
    console.log(`  ✅ Alert sent (Score: ${score}/5)!`);

    // Broadcast update to UI
    broadcastAlert({
      id: alertId,
      wallet,
      walletAgeDays: age,
      absorptionPct,
      score,
      hoursToResolution,
      status: 'passed'
    });

  } catch (e) {
    console.error(`  ⚠️  Worker Thread Error:`, e);
    broadcastAlert({
      id: alertId,
      status: 'skipped',
      wallet: "Error",
      walletAgeDays: 999
    });
  }
}

// ==========================================
// WEBSOCKET HANDLERS
// ==========================================

let isFirstConnect = true;

function startWebsocket() {
  if (assetsToMonitor.length === 0) return;
  if (activeWs) {
    activeWs.terminate();
  }

  const ws = new WebSocket("wss://ws-subscriptions-clob.polymarket.com/ws/market");
  activeWs = ws;
  
  let pingInterval: NodeJS.Timeout;

  ws.on('open', () => {
    console.log("🌐 Connected to Polymarket CLOB WebSocket!");
    broadcastStatus({ connected: true, message: "Connected to Polymarket" });

    const batches: string[][] = [];
    for (let i = 0; i < assetsToMonitor.length; i += BATCH_SIZE) {
      batches.push(assetsToMonitor.slice(i, i + BATCH_SIZE));
    }
    
    batches.forEach((batch, idx) => {
      setTimeout(() => {
        if (ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({ assets_ids: batch, type: "market" }));
        }
      }, idx * 100);
    });

    console.log(`📨 Subscribed to ${assetsToMonitor.length} tokens.`);
    
    if (isFirstConnect) {
      sendDiscordAlert(`🚀 **POLYMARKET WHALE MONITOR STARTED**\n🌐 Connected to Polymarket CLOB WebSocket\n📨 Subscribed to ${assetsToMonitor.length} active tokens.\n🔍 Tracking trades ≥ $${MIN_API_GATE_SIZE.toLocaleString()} USD`);
      isFirstConnect = false;
    }
    
    pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send("PING");
        broadcastStats();
      }
    }, 10000);
  });

  ws.on('message', (data) => {
    const rawMessage = data.toString();
    if (rawMessage === "PONG" || rawMessage === '"PONG"') return;

    try {
      const parsed = JSON.parse(rawMessage);
      const events = Array.isArray(parsed) ? parsed : [parsed];
      
      stats.totalEvents += events.length;

      for (const t of events) {
        if (t.event_type !== "last_trade_price") continue;
        
        stats.tradeEvents += 1;

        const price = parseFloat(t.price || "0");
        const size = parseFloat(t.size || "0");
        const usdAmount = price * size;

        const isAsymmetric = (price <= LOW_PROB_MAX) || (price >= HIGH_PROB_MIN);

        if (usdAmount < MIN_API_GATE_SIZE || !isAsymmetric) continue;

        const assetId = t.asset_id || "";
        const marketTitle = marketNames.get(assetId) || "Unknown market";
        const side = t.side || "?";
        const txHash = t.transaction_hash;

        console.log(`[${new Date().toLocaleTimeString()}] 🐳 WHALE DETECTED: $${usdAmount.toLocaleString()} | ${side} @ ${price.toFixed(3)} | ${marketTitle}`);

        const alertId = crypto.randomUUID();
        const initialAlert = {
          id: alertId,
          timestamp: Date.now(),
          marketTitle,
          assetId,
          side,
          price,
          size,
          usdAmount,
          txHash,
          wallet: "Checking...",
          walletAgeDays: null,
          absorptionPct: null,
          score: null,
          hoursToResolution: null,
          status: 'pending'
        };
        broadcastAlert(initialAlert);

        // Process asynchronously
        processTrade(alertId, assetId, side, price, size, usdAmount, txHash, marketTitle);
      }
    } catch (e) {
      // console.error(`  ⚠️  Parse error:`, e);
    }
  });

  ws.on('error', (error) => {
    console.error(`⚠️  WebSocket Error:`, error);
  });

  ws.on('close', () => {
    console.log(`🔌 Disconnected from Polymarket.`);
    clearInterval(pingInterval);
    broadcastStatus({ connected: false, message: isMonitoring ? "Disconnected. Reconnecting..." : "Monitor Stopped" });
    if (!isShuttingDown && isMonitoring) {
      setTimeout(() => {
        startWebsocket();
      }, 5000);
    }
  });
}

async function runMonitor() {
  if (!isMonitoring) return;
  console.log("🚀 POLYMARKET WHALE MONITOR — STARTING UP...");
  await fetchTopAssets();
  startWebsocket();
  
  // Refresh markets every hour
  if (refreshInterval) clearInterval(refreshInterval);
  refreshInterval = setInterval(async () => {
    if (!isMonitoring) return;
    await fetchTopAssets();
    startWebsocket(); // Reconnect with new assets
  }, 60 * 60 * 1000);
}

function stopMonitor() {
  console.log("🛑 Stopping monitor...");
  isMonitoring = false;
  if (refreshInterval) clearInterval(refreshInterval);
  if (activeWs) {
    activeWs.terminate();
    activeWs = null;
  }
  broadcastStatus({ connected: false, message: "Monitor Stopped" });
}

async function startMonitor() {
  if (isMonitoring) return;
  isMonitoring = true;
  isFirstConnect = true;
  broadcastStatus({ connected: false, message: "Starting..." });
  await runMonitor();
}

// ==========================================
// EXPRESS SERVER SETUP
// ==========================================

async function startServer() {
  // API Routes
  app.get('/api/health', (req, res) => {
    res.json({ status: 'ok' });
  });

  app.post('/api/start', async (req, res) => {
    await startMonitor();
    res.json({ status: 'started' });
  });

  app.post('/api/stop', (req, res) => {
    stopMonitor();
    res.json({ status: 'stopped' });
  });

  app.get('/api/status', (req, res) => {
    res.json({ 
      isMonitoring, 
      connected: activeWs?.readyState === WebSocket.OPEN 
    });
  });

  app.get('/api/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders();

    sseClients.add(res);

    // Send initial state
    res.write(`event: status\ndata: ${JSON.stringify({ connected: activeWs?.readyState === WebSocket.OPEN, message: isMonitoring ? (activeWs?.readyState === WebSocket.OPEN ? "Connected to Polymarket" : "Connecting...") : "Monitor Stopped" })}\n\n`);
    res.write(`event: stats\ndata: ${JSON.stringify(stats)}\n\n`);
    
    // Send recent alerts
    for (const alert of [...recentAlerts].reverse()) {
      res.write(`data: ${JSON.stringify(alert)}\n\n`);
    }

    req.on('close', () => {
      sseClients.delete(res);
    });
  });

  // Vite middleware for development
  if (process.env.NODE_ENV !== 'production') {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: 'spa',
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), 'dist');
    app.use(express.static(distPath));
    app.get('*', (req, res) => {
      res.sendFile(path.join(distPath, 'index.html'));
    });
  }

  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://localhost:${PORT}`);
    runMonitor();
  });
}

startServer();
