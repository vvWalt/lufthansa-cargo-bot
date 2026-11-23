/**
 * ============================================================================
 * LUFTHANSA CARGO AWB TRACKER & WHATSAPP / TELEGRAM PUSH NOTIFICATION BOT
 * ============================================================================
 *
 * Features:
 * 1. Zero external npm dependencies (uses Node.js v18+ built-in http, fs, fetch).
 * 2. Interactive Bot Commands via WhatsApp Cloud API (and/or Telegram Bot API):
 *      - "track 020-12345678"  -> Registers an AWB for continuous monitoring
 *      - "status 020-12345678" -> Fetches live milestone status immediately
 *      - "list"                -> Lists all actively monitored AWBs
 *      - "stop 020-12345678"   -> Stops monitoring an AWB
 *      - "help"                -> Displays available commands
 * 3. Dual Push Architecture:
 *      - Webhook Receiver (/webhook/lufthansa) for Lufthansa's
 *        "Shipment Tracking Subscribe API" real-time push events.
 *      - Automatic Background Poller (every 15 mins) using Lufthansa's
 *        "Shipment Tracking API v4" as a reliable fallback.
 * 4. Built-in Simulation Mode (MOCK_MODE) when no API key is configured yet,
 *    allowing immediate local testing of the entire tracking & alert pipeline.
 *
 * Usage:
 *   1. Set environment variables (or edit CONFIG below).
 *   2. Run: node lufthansa_cargo_bot.js
 * ============================================================================
 */

const http = require('http');
const fs = require('fs');
const path = require('path');

// ============================================================================
// 1. CONFIGURATION (Set via Environment Variables or directly here)
// ============================================================================
const CONFIG = {
  PORT: process.env.PORT || 3000,

  // Lufthansa Cargo Developer Portal (https://developer.lufthansa-cargo.com)
  LH_API_BASE_URL: 'https://api.lufthansa-cargo.com',
  LH_API_KEY: process.env.LH_API_KEY || '', // Leave empty to run in Simulation Mode

  // Polling interval in milliseconds (Default: 15 minutes)
  POLL_INTERVAL_MS: Number(process.env.POLL_INTERVAL_MS) || 15 * 60 * 1000,

  // Meta WhatsApp Cloud API Credentials (https://developers.facebook.com)
  WHATSAPP_TOKEN: process.env.WHATSAPP_TOKEN || '',
  WHATSAPP_PHONE_NUMBER_ID: process.env.WHATSAPP_PHONE_NUMBER_ID || '',
  WHATSAPP_VERIFY_TOKEN: process.env.WHATSAPP_VERIFY_TOKEN || 'lh_cargo_verify_2026',
  // Optional: Approved Meta Utility Template name for proactive push alerts >24h
  WHATSAPP_TEMPLATE_NAME: process.env.WHATSAPP_TEMPLATE_NAME || '',

  // Optional: Telegram Bot Token (Zero-friction alternative for instant team alerts)
  TELEGRAM_BOT_TOKEN: process.env.TELEGRAM_BOT_TOKEN || '',

  // Local JSON file to persist monitored shipments across restarts
  DB_FILE: path.join(__dirname, 'tracked_shipments.json'),
};

// Standard IATA Cargo Freight Status Update (FSU) Milestone Descriptions
const IATA_MILESTONE_LABELS = {
  BKD: 'Booking Confirmed',
  RCS: 'Received from Shipper (Freight accepted at origin)',
  MAN: 'Manifested on Flight',
  DEP: 'Departed on Flight',
  ARR: 'Arrived at Airport',
  RCF: 'Received from Flight (Checked into warehouse)',
  NFD: 'Notified Consignee (Ready for pickup/customs)',
  AWD: 'Arrival Documents Delivered',
  DLV: 'Delivered to Consignee (Final)',
  DIS: 'Discrepancy / Exception Alert',
};

// ============================================================================
// 2. PERSISTENT JSON STORAGE HELPER
// ============================================================================
class ShipmentStore {
  constructor(filePath) {
    this.filePath = filePath;
    this.data = this._load();
  }

  _load() {
    try {
      if (fs.existsSync(this.filePath)) {
        const raw = fs.readFileSync(this.filePath, 'utf8');
        return JSON.parse(raw);
      }
    } catch (err) {
      console.error('[Store] Failed to load DB, initializing empty store:', err.message);
    }
    return { shipments: {} };
  }

  _save() {
    try {
      fs.writeFileSync(this.filePath, JSON.stringify(this.data, null, 2), 'utf8');
    } catch (err) {
      console.error('[Store] Failed to save DB:', err.message);
    }
  }

  /**
   * Registers a subscriber (WhatsApp phone or Telegram chat ID) for an AWB
   */
  subscribe(awbKey, subscriber) {
    if (!this.data.shipments[awbKey]) {
      this.data.shipments[awbKey] = {
        awb: awbKey,
        lastEventSignature: null,
        lastStatusSummary: 'Pending initial check...',
        isDelivered: false,
        subscribers: [],
        updatedAt: new Date().toISOString(),
      };
    }

    const record = this.data.shipments[awbKey];
    const exists = record.subscribers.some(
      (s) => s.channel === subscriber.channel && s.recipientId === subscriber.recipientId
    );

    if (!exists) {
      record.subscribers.push(subscriber);
      this._save();
    }
    return record;
  }

  /**
   * Removes a subscriber from an AWB; deletes AWB if no subscribers remain
   */
  unsubscribe(awbKey, channel, recipientId) {
    const record = this.data.shipments[awbKey];
    if (!record) return false;

    record.subscribers = record.subscribers.filter(
      (s) => !(s.channel === channel && s.recipientId === recipientId)
    );

    if (record.subscribers.length === 0) {
      delete this.data.shipments[awbKey];
    }
    this._save();
    return true;
  }

  /**
   * Lists all active shipments subscribed by a specific user
   */
  listForUser(channel, recipientId) {
    return Object.values(this.data.shipments).filter((ship) =>
      ship.subscribers.some((s) => s.channel === channel && s.recipientId === recipientId)
    );
  }

  /**
   * Returns all active (non-delivered) shipments for background polling
   */
  getActiveShipments() {
    return Object.values(this.data.shipments).filter((ship) => !ship.isDelivered);
  }

  updateShipmentState(awbKey, { eventSignature, summary, isDelivered }) {
    if (!this.data.shipments[awbKey]) return;
    this.data.shipments[awbKey].lastEventSignature = eventSignature;
    this.data.shipments[awbKey].lastStatusSummary = summary;
    this.data.shipments[awbKey].isDelivered = Boolean(isDelivered);
    this.data.shipments[awbKey].updatedAt = new Date().toISOString();
    this._save();
  }
}

const store = new ShipmentStore(CONFIG.DB_FILE);

// ============================================================================
// 3. AWB VALIDATION & LUFTHANSA CARGO API CLIENT
// ============================================================================

/**
 * Validates and normalizes an Air Waybill string (e.g. "020-12345678" or "02012345678").
 * Lufthansa Cargo's IATA airline prefix is "020", followed by an 8-digit serial number.
 */
function parseAWB(input) {
  const cleaned = String(input || '').replace(/[^0-9]/g, '');
  if (cleaned.length !== 11) {
    return null;
  }
  const prefix = cleaned.slice(0, 3);
  const serial = cleaned.slice(3, 11);

  // IATA Mod-7 Check Digit Validation on the 8-digit AWB serial number
  const firstSeven = parseInt(serial.slice(0, 7), 10);
  const checkDigit = parseInt(serial.slice(7, 8), 10);
  if (firstSeven % 7 !== checkDigit) {
    return { valid: false, reason: 'Invalid IATA Mod-7 check digit on AWB number.' };
  }

  return {
    valid: true,
    prefix,
    serial,
    formatted: `${prefix}-${serial}`,
  };
}

/**
 * Fetches live shipment status from Lufthansa Cargo Shipment Tracking API v4.
 * Falls back to realistic simulated events if LH_API_KEY is not yet configured.
 */
async function fetchLufthansaShipmentStatus(prefix, serial) {
  // Simulation Mode for immediate local testing without a live API key
  if (!CONFIG.LH_API_KEY) {
    return generateSimulatedTrackingResponse(prefix, serial);
  }

  const url = `${CONFIG.LH_API_BASE_URL}/lhcargo/handling/shipmenttracking/v4/shipment?aWBPrefix=${prefix}&aWBNumber=${serial}`;

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      APIKEY: CONFIG.LH_API_KEY,
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    const errBody = await response.text();
    throw new Error(`Lufthansa API HTTP ${response.status}: ${errBody}`);
  }

  const data = await response.json();
  return normalizeLufthansaResponse(prefix, serial, data);
}

/**
 * Parses Lufthansa Cargo's nested JSON response into a clean status object
 */
function normalizeLufthansaResponse(prefix, serial, rawData) {
  const tracking = rawData?.shipmentTrackingStatus || rawData;
  const booking = tracking?.booking || {};
  const origin = booking?.origin || 'N/A';
  const destination = booking?.destination || 'N/A';
  const pieces = booking?.pieces || tracking?.actualTotals?.[0]?.pieces || '-';
  const weight = booking?.weight || tracking?.actualTotals?.[0]?.weight || '-';

  // Extract event history array
  const events =
    tracking?.shipmentStatusEvents?.event ||
    tracking?.events?.event ||
    [];

  // Sort events chronologically so the latest event is last
  const sortedEvents = [...events].sort((a, b) => {
    const tA = new Date(a.eventTime || a.actualTime || 0).getTime();
    const tB = new Date(b.eventTime || b.actualTime || 0).getTime();
    return tA - tB;
  });

  const latestEvent = sortedEvents[sortedEvents.length - 1] || {};
  const statusCode = latestEvent.statusCode || latestEvent.eventCode || 'BKD';
  const station = latestEvent.airportCode || latestEvent.station || origin;
  const flightNo = latestEvent.flightNumber || latestEvent.flight?.flightNumber || '';
  const timestamp = latestEvent.eventTime || latestEvent.actualTime || new Date().toISOString();

  const milestoneDesc = IATA_MILESTONE_LABELS[statusCode] || statusCode;
  const eventSignature = `${statusCode}|${station}|${flightNo}|${timestamp}`;

  return {
    awb: `${prefix}-${serial}`,
    origin,
    destination,
    pieces,
    weight,
    statusCode,
    milestoneDesc,
    station,
    flightNo,
    timestamp,
    eventSignature,
    isDelivered: statusCode === 'DLV',
  };
}

/**
 * Simulates progressive cargo milestones when testing without a Lufthansa API key
 */
function generateSimulatedTrackingResponse(prefix, serial) {
  const stages = [
    { code: 'RCS', station: 'IST', flight: 'LH8391', desc: IATA_MILESTONE_LABELS.RCS },
    { code: 'DEP', station: 'IST', flight: 'LH8391', desc: IATA_MILESTONE_LABELS.DEP },
    { code: 'ARR', station: 'FRA', flight: 'LH8391', desc: IATA_MILESTONE_LABELS.ARR },
    { code: 'RCF', station: 'FRA', flight: 'LH8391', desc: IATA_MILESTONE_LABELS.RCF },
    { code: 'NFD', station: 'FRA', flight: 'LH8391', desc: IATA_MILESTONE_LABELS.NFD },
  ];
  // Rotate stage based on current minute so polling tests show live progression
  const idx = Math.floor(Date.now() / 60000) % stages.length;
  const current = stages[idx];
  const timestamp = new Date().toISOString();

  return {
    awb: `${prefix}-${serial}`,
    origin: 'IST',
    destination: 'FRA',
    pieces: '12 PCS',
    weight: '480 KG',
    statusCode: current.code,
    milestoneDesc: `${current.desc} [SIMULATED]`,
    station: current.station,
    flightNo: current.flight,
    timestamp,
    eventSignature: `${current.code}|${current.station}|${current.flight}`,
    isDelivered: current.code === 'DLV',
  };
}

/**
 * Formats a shipment status object into a clean, readable push notification message
 */
function formatStatusMessage(status, isAlert = false) {
  const header = isAlert
    ? `🚨 *LUFTHANSA CARGO STATUS UPDATE*`
    : `📦 *LUFTHANSA CARGO SHIPMENT STATUS*`;

  const flightLine = status.flightNo ? `\n✈️ *Flight:* ${status.flightNo}` : '';

  return (
    `${header}\n` +
    `━━━━━━━━━━━━━━━━━━━━\n` +
    `🔖 *AWB:* ${status.awb}\n` +
    `📍 *Status:* [${status.statusCode}] ${status.milestoneDesc}\n` +
    `🏢 *Station:* ${status.station} (${status.origin} ➔ ${status.destination})` +
    `${flightLine}\n` +
    `📦 *Cargo:* ${status.pieces} / ${status.weight}\n` +
    `🕒 *Time:* ${status.timestamp}`
  );
}

// ============================================================================
// 4. WHATSAPP CLOUD API & TELEGRAM MESSAGING SENDERS
// ============================================================================

async function sendNotification(channel, recipientId, textMessage, templateVars = null) {
  try {
    if (channel === 'whatsapp') {
      await sendWhatsAppMessage(recipientId, textMessage, templateVars);
    } else if (channel === 'telegram') {
      await sendTelegramMessage(recipientId, textMessage);
    } else {
      console.log(`[Console Push -> ${channel}:${recipientId}]\n${textMessage}\n`);
    }
  } catch (err) {
    console.error(`[Notifier] Error sending ${channel} message to ${recipientId}:`, err.message);
  }
}

async function sendWhatsAppMessage(toPhone, textBody, templateVars = null) {
  if (!CONFIG.WHATSAPP_TOKEN || !CONFIG.WHATSAPP_PHONE_NUMBER_ID) {
    console.log(`[WhatsApp Dry-Run -> ${toPhone}]\n${textBody}\n`);
    return;
  }

  const endpoint = `https://graph.facebook.com/v20.0/${CONFIG.WHATSAPP_PHONE_NUMBER_ID}/messages`;

  // Note: Meta requires a pre-approved utility template if initiating a push alert
  // more than 24 hours after the user's last inbound message.
  const payload =
    templateVars && CONFIG.WHATSAPP_TEMPLATE_NAME
      ? {
          messaging_product: 'whatsapp',
          to: toPhone,
          type: 'template',
          template: {
            name: CONFIG.WHATSAPP_TEMPLATE_NAME,
            language: { code: 'en' },
            components: [
              {
                type: 'body',
                parameters: templateVars.map((val) => ({ type: 'text', text: String(val) })),
              },
            ],
          },
        }
      : {
          messaging_product: 'whatsapp',
          to: toPhone,
          type: 'text',
          text: { body: textBody },
        };

  const res = await fetch(endpoint, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${CONFIG.WHATSAPP_TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`WhatsApp API Error ${res.status}: ${errText}`);
  }
}

async function sendTelegramMessage(chatId, textBody) {
  if (!CONFIG.TELEGRAM_BOT_TOKEN) {
    console.log(`[Telegram Dry-Run -> ${chatId}]\n${textBody}\n`);
    return;
  }

  const endpoint = `https://api.telegram.org/bot${CONFIG.TELEGRAM_BOT_TOKEN}/sendMessage`;
  const res = await fetch(endpoint, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      chat_id: chatId,
      text: textBody.replace(/\*/g, ''), // Clean markdown or use parse_mode
    }),
  });

  if (!res.ok) {
    const errText = await res.text();
    throw new Error(`Telegram API Error ${res.status}: ${errText}`);
  }
}

// ============================================================================
// 5. BOT COMMAND PROCESSOR (Handles user messages from WhatsApp or Telegram)
// ============================================================================

async function handleIncomingUserCommand(channel, recipientId, rawText) {
  const text = String(rawText || '').trim();
  const parts = text.split(/\s+/);
  const command = (parts[0] || '').toLowerCase();
  const arg = parts[1] || '';

  // Allow users to simply paste a raw AWB number directly (e.g., "020-12345678")
  const directAwb = parseAWB(command);
  if (directAwb && directAwb.valid) {
    return handleTrackCommand(channel, recipientId, directAwb);
  }

  switch (command) {
    case 'track':
    case '/track':
    case 'add': {
      const parsed = parseAWB(arg);
      if (!parsed) {
        return sendNotification(
          channel,
          recipientId,
          '❌ Please provide a valid 11-digit AWB.\nExample: *track 020-12345678*'
        );
      }
      if (!parsed.valid) {
        return sendNotification(channel, recipientId, `❌ ${parsed.reason}`);
      }
      return handleTrackCommand(channel, recipientId, parsed);
    }

    case 'status':
    case '/status':
    case 'check': {
      const parsed = parseAWB(arg);
      if (!parsed || !parsed.valid) {
        return sendNotification(
          channel,
          recipientId,
          '❌ Please provide a valid 11-digit AWB.\nExample: *status 020-12345678*'
        );
      }
      try {
        const status = await fetchLufthansaShipmentStatus(parsed.prefix, parsed.serial);
        return sendNotification(channel, recipientId, formatStatusMessage(status, false));
      } catch (err) {
        return sendNotification(
          channel,
          recipientId,
          `⚠️ Could not retrieve status for ${parsed.formatted}: ${err.message}`
        );
      }
    }

    case 'list':
    case '/list': {
      const active = store.listForUser(channel, recipientId);
      if (active.length === 0) {
        return sendNotification(
          channel,
          recipientId,
          '📭 You have no active shipments being monitored.\nSend *track 020-12345678* to start.'
        );
      }
      const lines = active.map(
        (s, i) => `${i + 1}. *${s.awb}* — ${s.lastStatusSummary}`
      );
      return sendNotification(
        channel,
        recipientId,
        `📋 *Monitored Lufthansa Cargo AWBs:*\n\n${lines.join('\n')}`
      );
    }

    case 'stop':
    case '/stop':
    case 'remove': {
      const parsed = parseAWB(arg);
      if (!parsed || !parsed.valid) {
        return sendNotification(
          channel,
          recipientId,
          '❌ Please specify the AWB to stop tracking.\nExample: *stop 020-12345678*'
        );
      }
      const removed = store.unsubscribe(parsed.formatted, channel, recipientId);
      return sendNotification(
        channel,
        recipientId,
        removed
          ? `🛑 Stopped push notifications for *${parsed.formatted}*.`
          : `ℹ️ AWB *${parsed.formatted}* was not in your active list.`
      );
    }

    default: {
      return sendNotification(
        channel,
        recipientId,
        `✈️ *Lufthansa Cargo Tracker Bot*\n\n` +
          `Send any of the following commands:\n` +
          `• *track 020-12345678* — Subscribe to push alerts\n` +
          `• *status 020-12345678* — Get instant live status\n` +
          `• *list* — Show all tracked AWBs\n` +
          `• *stop 020-12345678* — Unsubscribe from alerts`
      );
    }
  }
}

async function handleTrackCommand(channel, recipientId, parsedAwb) {
  store.subscribe(parsedAwb.formatted, { channel, recipientId });

  try {
    const status = await fetchLufthansaShipmentStatus(parsedAwb.prefix, parsedAwb.serial);
    store.updateShipmentState(parsedAwb.formatted, {
      eventSignature: status.eventSignature,
      summary: `[${status.statusCode}] ${status.milestoneDesc} (${status.station})`,
      isDelivered: status.isDelivered,
    });

    await sendNotification(
      channel,
      recipientId,
      `✅ *Push alerts activated for ${parsedAwb.formatted}!*\n` +
        `You will receive automatic updates on every milestone change.\n\n` +
        formatStatusMessage(status, false)
    );
  } catch (err) {
    await sendNotification(
      channel,
      recipientId,
      `✅ Subscribed to *${parsedAwb.formatted}*, but initial lookup returned: ${err.message}. Will keep retrying automatically.`
    );
  }
}

// ============================================================================
// 6. BACKGROUND POLLER & LUFTHANSA PUSH WEBHOOK HANDLER
// ============================================================================

async function pollAllActiveShipments() {
  const activeShipments = store.getActiveShipments();
  if (activeShipments.length === 0) return;

  console.log(`[Poller] Checking ${activeShipments.length} active AWB(s)...`);

  for (const shipment of activeShipments) {
    try {
      const [prefix, serial] = shipment.awb.split('-');
      const liveStatus = await fetchLufthansaShipmentStatus(prefix, serial);

      // Compare event signature to detect any new milestone/station change
      if (liveStatus.eventSignature !== shipment.lastEventSignature) {
        console.log(`[Poller] New milestone detected for ${shipment.awb}: ${liveStatus.eventSignature}`);

        store.updateShipmentState(shipment.awb, {
          eventSignature: liveStatus.eventSignature,
          summary: `[${liveStatus.statusCode}] ${liveStatus.milestoneDesc} (${liveStatus.station})`,
          isDelivered: liveStatus.isDelivered,
        });

        const alertMsg = formatStatusMessage(liveStatus, true);
        const templateVars = [
          liveStatus.awb,
          `${liveStatus.statusCode} - ${liveStatus.milestoneDesc}`,
          liveStatus.station,
        ];

        for (const sub of shipment.subscribers) {
          await sendNotification(sub.channel, sub.recipientId, alertMsg, templateVars);
        }
      }
    } catch (err) {
      console.error(`[Poller] Error checking AWB ${shipment.awb}:`, err.message);
    }
  }
}

// ============================================================================
// 7. HTTP SERVER (Webhooks for WhatsApp, Telegram, Lufthansa Push & Local Test)
// ============================================================================

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk.toString();
    });
    req.on('end', () => {
      if (!body) return resolve({});
      try {
        resolve(JSON.parse(body));
      } catch (e) {
        reject(e);
      }
    });
  });
}

const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  try {
    // 1. Health Check
    if (req.method === 'GET' && pathname === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(
        JSON.stringify({
          service: 'Lufthansa Cargo AWB Push Bot',
          mode: CONFIG.LH_API_KEY ? 'LIVE_API' : 'SIMULATION_MODE',
          activeShipments: store.getActiveShipments().length,
        })
      );
    }

    // 2. Meta WhatsApp Cloud API Webhook Verification (GET)
    if (req.method === 'GET' && pathname === '/webhook/whatsapp') {
      const mode = parsedUrl.searchParams.get('hub.mode');
      const token = parsedUrl.searchParams.get('hub.verify_token');
      const challenge = parsedUrl.searchParams.get('hub.challenge');

      if (mode === 'subscribe' && token === CONFIG.WHATSAPP_VERIFY_TOKEN) {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        return res.end(challenge);
      }
      res.writeHead(403);
      return res.end('Forbidden');
    }

    // 3. Meta WhatsApp Cloud API Incoming Message Webhook (POST)
    if (req.method === 'POST' && pathname === '/webhook/whatsapp') {
      const payload = await parseJsonBody(req);
      const entry = payload?.entry?.[0];
      const changes = entry?.changes?.[0]?.value;
      const message = changes?.messages?.[0];

      if (message && message.type === 'text') {
        const fromPhone = message.from;
        const userText = message.text.body;
        await handleIncomingUserCommand('whatsapp', fromPhone, userText);
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'ok' }));
    }

    // 4. Optional Telegram Bot Webhook (POST)
    if (req.method === 'POST' && pathname === '/webhook/telegram') {
      const payload = await parseJsonBody(req);
      const msg = payload?.message;
      if (msg && msg.text) {
        await handleIncomingUserCommand('telegram', String(msg.chat.id), msg.text);
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ status: 'ok' }));
    }

    // 5. Lufthansa Cargo "Shipment Tracking Subscribe API" Push Receiver (POST)
    if (req.method === 'POST' && pathname === '/webhook/lufthansa') {
      const payload = await parseJsonBody(req);
      const prefix = payload?.shipmentId?.aWBPrefix || '020';
      const serial = payload?.shipmentId?.aWBNumber;

      if (serial) {
        const awbKey = `${prefix}-${serial}`;
        const record = store.data.shipments[awbKey];
        if (record) {
          const liveStatus = normalizeLufthansaResponse(prefix, serial, payload);
          store.updateShipmentState(awbKey, {
            eventSignature: liveStatus.eventSignature,
            summary: `[${liveStatus.statusCode}] ${liveStatus.milestoneDesc} (${liveStatus.station})`,
            isDelivered: liveStatus.isDelivered,
          });
          const alertMsg = formatStatusMessage(liveStatus, true);
          for (const sub of record.subscribers) {
            await sendNotification(sub.channel, sub.recipientId, alertMsg);
          }
        }
      }

      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ received: true }));
    }

    // 6. Local Test Endpoint: Simulate sending a WhatsApp command via curl/Postman
    // Example: POST /test/command  { "phone": "+905551234567", "text": "track 020-12345675" }
    if (req.method === 'POST' && pathname === '/test/command') {
      const { phone = 'console-user', channel = 'whatsapp', text = 'help' } = await parseJsonBody(req);
      await handleIncomingUserCommand(channel, phone, text);
      res.writeHead(200, { 'Content-Type': 'application/json' });
      return res.end(JSON.stringify({ processed: true, text }));
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Not Found' }));
  } catch (err) {
    console.error('[HTTP Server Error]', err);
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: err.message }));
  }
});

// Start HTTP Server & Background Polling Loop
server.listen(CONFIG.PORT, () => {
  console.log(`=================================================================`);
  console.log(`✈️  Lufthansa Cargo Push Tracker running on port ${CONFIG.PORT}`);
  console.log(`📡 Mode: ${CONFIG.LH_API_KEY ? 'LIVE LUFTHANSA API' : 'SIMULATION MODE (Set LH_API_KEY for live data)'}`);
  console.log(`⏱️  Polling interval: ${CONFIG.POLL_INTERVAL_MS / 1000}s`);
  console.log(`=================================================================`);

  // Run an initial check on startup, then repeat on schedule
  pollAllActiveShipments();
  setInterval(pollAllActiveShipments, CONFIG.POLL_INTERVAL_MS);
});