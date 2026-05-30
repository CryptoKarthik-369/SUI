// ============================================================
//  SUI SMC Bot — Webhook Server
//  Stack: Node.js + Express + WebSocket (ws)
//  Receives TradingView POST alerts → broadcasts to web dashboard
// ============================================================
//
//  INSTALL:
//    npm init -y
//    npm install express ws cors helmet dotenv
//
//  RUN:
//    node server.js
//
//  DEPLOY (free tier options):
//    • Railway.app  → connect GitHub repo, auto-deploy
//    • Render.com   → free Node.js service
//    • Fly.io       → fly launch
//    • VPS          → pm2 start server.js --name sui-bot
//
//  TRADINGVIEW WEBHOOK SETUP:
//    1. Open indicator alert → Notifications → Webhook URL
//    2. Paste: https://your-deployed-url.com/webhook
//    3. Alert message: use the JSON strings from the Pine Script
// ============================================================

require("dotenv").config();
const express   = require("express");
const { WebSocketServer } = require("ws");
const cors      = require("cors");
const helmet    = require("helmet");
const http      = require("http");
const crypto    = require("crypto");

const PORT         = process.env.PORT         || 3001;
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || "sui_smc_secret_2024";
const MAX_ALERTS   = 200;   // keep last N alerts in memory

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server, path: "/ws" });

// ─────────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────────
app.use(helmet({ contentSecurityPolicy: false }));
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "10kb" }));
app.use(express.urlencoded({ extended: false }));

// ─────────────────────────────────────────────
// IN-MEMORY STATE
// ─────────────────────────────────────────────
let alerts    = [];          // ring buffer of recent alerts
let botState  = {
    symbol:       "SUIUSDT",
    lastPrice:    null,
    lastAction:   null,
    lastAlert:    null,
    d1Bias:       "UNKNOWN",
    h1Zone:       "UNKNOWN",
    m5Signal:     "WATCHING",
    activeSignal: null,       // { action, entry, sl, tp1, tp2, time }
    stats: {
        totalSignals: 0,
        longSignals:  0,
        shortSignals: 0,
        watchSignals: 0,
        todaySignals: 0,
        lastReset:    new Date().toDateString(),
    }
};

// ─────────────────────────────────────────────
// ALERT ENRICHMENT
// ─────────────────────────────────────────────
function enrichAlert(raw) {
    const price    = parseFloat(raw.price)  || 0;
    const sl       = parseFloat(raw.sl)     || 0;
    const tp1      = parseFloat(raw.tp1)    || 0;
    const tp2      = parseFloat(raw.tp2)    || 0;
    const risk     = Math.abs(price - sl);
    const reward1  = Math.abs(tp1 - price);
    const rr1      = risk > 0 ? (reward1 / risk).toFixed(2) : "—";

    return {
        id:         crypto.randomUUID(),
        receivedAt: new Date().toISOString(),
        symbol:     raw.symbol     || "SUIUSDT",
        action:     raw.action     || "INFO",
        type:       raw.type       || "ALERT",
        timeframe:  raw.timeframe  || "5M",
        price:      price,
        sl:         sl,
        tp1:        tp1,
        tp2:        tp2,
        rr:         rr1,
        d1Bias:     raw["1d_bias"] || raw.d1_bias  || botState.d1Bias,
        h1Zone:     raw["1h_ob"]   || raw.h1_zone  || botState.h1Zone,
        m5Signal:   raw["5m_signal"] || raw.m5_signal || "—",
        raw:        raw,
    };
}

// ─────────────────────────────────────────────
// UPDATE BOT STATE FROM ALERT
// ─────────────────────────────────────────────
function updateState(alert) {
    botState.lastPrice  = alert.price;
    botState.lastAction = alert.action;
    botState.lastAlert  = alert.receivedAt;

    if (alert.d1Bias !== "UNKNOWN") botState.d1Bias = alert.d1Bias;
    if (alert.h1Zone !== "UNKNOWN") botState.h1Zone = alert.h1Zone;
    if (alert.m5Signal !== "—")     botState.m5Signal = alert.m5Signal;

    const today = new Date().toDateString();
    if (botState.stats.lastReset !== today) {
        botState.stats.todaySignals = 0;
        botState.stats.lastReset    = today;
    }

    botState.stats.totalSignals++;
    botState.stats.todaySignals++;

    if (alert.action === "LONG" && alert.type === "ENTRY") {
        botState.stats.longSignals++;
        botState.activeSignal = {
            direction: "LONG",
            entry:     alert.price,
            sl:        alert.sl,
            tp1:       alert.tp1,
            tp2:       alert.tp2,
            rr:        alert.rr,
            time:      alert.receivedAt,
            status:    "ACTIVE",
        };
    } else if (alert.action === "SHORT" && alert.type === "ENTRY") {
        botState.stats.shortSignals++;
        botState.activeSignal = {
            direction: "SHORT",
            entry:     alert.price,
            sl:        alert.sl,
            tp1:       alert.tp1,
            tp2:       alert.tp2,
            rr:        alert.rr,
            time:      alert.receivedAt,
            status:    "ACTIVE",
        };
    } else if (alert.action.startsWith("WATCH")) {
        botState.stats.watchSignals++;
    }

    // Trim ring buffer
    alerts.unshift(alert);
    if (alerts.length > MAX_ALERTS) alerts = alerts.slice(0, MAX_ALERTS);
}

// ─────────────────────────────────────────────
// BROADCAST TO ALL CONNECTED WS CLIENTS
// ─────────────────────────────────────────────
function broadcast(event, data) {
    const msg = JSON.stringify({ event, data, ts: Date.now() });
    wss.clients.forEach(client => {
        if (client.readyState === 1) {   // OPEN
            client.send(msg);
        }
    });
}

// ─────────────────────────────────────────────
// WEBSOCKET CONNECTIONS
// ─────────────────────────────────────────────
wss.on("connection", (ws, req) => {
    const ip = req.socket.remoteAddress;
    console.log(`[WS] Client connected: ${ip} | total: ${wss.clients.size}`);

    // Send current state snapshot immediately
    ws.send(JSON.stringify({
        event: "SNAPSHOT",
        data:  { state: botState, alerts: alerts.slice(0, 50) },
        ts:    Date.now()
    }));

    ws.on("message", (raw) => {
        try {
            const msg = JSON.parse(raw.toString());
            // Client can request: { type: "PING" } | { type: "GET_ALERTS", limit: 20 }
            if (msg.type === "PING") {
                ws.send(JSON.stringify({ event: "PONG", ts: Date.now() }));
            }
            if (msg.type === "GET_ALERTS") {
                const limit = Math.min(msg.limit || 20, MAX_ALERTS);
                ws.send(JSON.stringify({ event: "ALERTS_LIST", data: alerts.slice(0, limit), ts: Date.now() }));
            }
        } catch {}
    });

    ws.on("close",   () => console.log(`[WS] Client disconnected | total: ${wss.clients.size}`));
    ws.on("error",   (e) => console.error("[WS] Error:", e.message));
});

// ─────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────

// Health check
app.get("/", (req, res) => {
    res.json({
        status:  "SUI SMC Bot Server ONLINE",
        version: "1.0.0",
        uptime:  process.uptime().toFixed(0) + "s",
        clients: wss.clients.size,
        alerts:  alerts.length,
        state:   botState,
    });
});

// GET current bot state (for polling fallback)
app.get("/api/state", (req, res) => {
    res.json({ ok: true, state: botState, ts: Date.now() });
});

// GET recent alerts
app.get("/api/alerts", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit) || 50, MAX_ALERTS);
    res.json({ ok: true, alerts: alerts.slice(0, limit), count: alerts.length });
});

// ── WEBHOOK ENDPOINT ──────────────────────────
// TradingView POST to: POST /webhook
// Optional auth: add ?secret=YOUR_SECRET to your TV webhook URL
app.post("/webhook", (req, res) => {
    // Optional secret check
    const secret = req.query.secret || req.headers["x-webhook-secret"] || "";
    if (WEBHOOK_SECRET && secret !== WEBHOOK_SECRET) {
        console.warn("[WEBHOOK] Unauthorized attempt from", req.ip);
        return res.status(401).json({ error: "Unauthorized" });
    }

    const body = req.body;
    console.log("[WEBHOOK] Received:", JSON.stringify(body));

    if (!body || typeof body !== "object" || !body.symbol) {
        // TradingView sometimes sends plain text — try parsing
        return res.status(400).json({ error: "Invalid payload" });
    }

    try {
        const alert = enrichAlert(body);
        updateState(alert);

        // Broadcast to all WS clients
        broadcast("ALERT", alert);
        broadcast("STATE", botState);

        console.log(`[ALERT] ${alert.action} ${alert.symbol} @ ${alert.price} | type: ${alert.type}`);
        res.json({ ok: true, id: alert.id });

    } catch (err) {
        console.error("[WEBHOOK] Error processing:", err);
        res.status(500).json({ error: "Processing error" });
    }
});

// Manual test alert (dev only)
app.post("/test-alert", (req, res) => {
    if (process.env.NODE_ENV === "production") {
        return res.status(403).json({ error: "Not allowed in production" });
    }
    const testPayload = req.body.payload || {
        symbol:     "SUIUSDT",
        action:     "LONG",
        type:       "ENTRY",
        timeframe:  "5M",
        price:      "3.8240",
        sl:         "3.7750",
        tp1:        "3.9180",
        tp2:        "4.0600",
        "1d_bias":  "BULLISH",
        "1h_ob":    "DEMAND",
        "5m_signal":"CHoCH_BULL",
        time:       new Date().toISOString(),
    };
    const alert = enrichAlert(testPayload);
    updateState(alert);
    broadcast("ALERT", alert);
    broadcast("STATE", botState);
    console.log("[TEST] Fired test alert:", alert.id);
    res.json({ ok: true, alert });
});

// ─────────────────────────────────────────────
// HEARTBEAT: broadcast state every 30s so UI stays fresh
// ─────────────────────────────────────────────
setInterval(() => {
    if (wss.clients.size > 0) {
        broadcast("HEARTBEAT", {
            ts:      Date.now(),
            clients: wss.clients.size,
            state:   botState,
        });
    }
}, 30_000);

// ─────────────────────────────────────────────
// START
// ─────────────────────────────────────────────
server.listen(PORT, () => {
    console.log("═══════════════════════════════════════");
    console.log(`  SUI SMC Bot Server  →  port ${PORT}`);
    console.log(`  Webhook endpoint    →  POST /webhook`);
    console.log(`  WebSocket           →  ws://localhost:${PORT}/ws`);
    console.log(`  API state           →  GET  /api/state`);
    console.log("═══════════════════════════════════════");
});

// ─────────────────────────────────────────────
// GRACEFUL SHUTDOWN
// ─────────────────────────────────────────────
process.on("SIGTERM", () => {
    console.log("[SERVER] Shutting down gracefully...");
    wss.close();
    server.close(() => process.exit(0));
});
