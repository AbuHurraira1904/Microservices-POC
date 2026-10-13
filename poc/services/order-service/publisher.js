// RabbitMQ publisher for the Order service.
//
// Reliability choices (each one is a talking point for the guide):
//  - Durable topic exchange: survives a broker restart.
//  - Persistent messages (persistent: true): written to disk by the broker.
//  - Confirm channel: the broker acknowledges every message. publish() only
//    resolves once the broker has confirmed it, so a lost event is never silent.
//  - Lazy connect + reconnect: if the connection drops, the next publish reconnects.
//
// Events are NOT published from the request. Order stores each event in the
// outbox_events table in the same DB transaction as the order outcome, and a relay
// loop (outbox.js) publishes it later with its stored eventId. That closes the
// "dual-write problem": a crash can no longer leave a saved order without its event.
// A crash after publishing but before the relay marks the row sends the SAME eventId
// again, which the consumer's duplicate check (processed_events) ignores.
//
// Keep this module's require() after dotenv.config() in index.js.

const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const URL = process.env.RABBITMQ_URL;
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || 'orders';

let connection = null;
let channelPromise = null;

function getChannel() {
  if (channelPromise) return channelPromise;

  channelPromise = (async () => {
    try {
      const conn = await amqp.connect(URL);
      conn.on('error', (err) => console.error('[publisher] connection error:', err.message));
      conn.on('close', () => {
        console.warn('[publisher] connection closed; will reconnect on next publish');
        connection = null;
        channelPromise = null;
      });

      const ch = await conn.createConfirmChannel();
      ch.on('error', (err) => console.error('[publisher] channel error:', err.message));
      await ch.assertExchange(EXCHANGE, 'topic', { durable: true });

      connection = conn;
      return ch;
    } catch (err) {
      channelPromise = null;   // allow a retry on the next call
      throw err;
    }
  })();

  return channelPromise;
}

// Publishes one event. Resolves with the full event once the broker confirms it.
// Rejects if the broker is unreachable or nacks the message. The caller decides
// what to do (the outbox relay leaves the row unpublished and retries later).
//
// options.eventId / options.timestamp: pass the values stored in the outbox so a
// re-publish carries the SAME eventId (that is what lets the consumer dedupe it).
async function publishEvent(type, payload, options = {}) {
  const event = {
    eventId: options.eventId || randomUUID(),
    type,
    ...payload,   // orderId, sku, quantity, amount, transactionId
    timestamp: options.timestamp || new Date().toISOString(),
  };

  const ch = await getChannel();

  await new Promise((resolve, reject) => {
    ch.publish(
      EXCHANGE,
      type,                                   // routing key, e.g. "order.confirmed"
      Buffer.from(JSON.stringify(event)),
      { persistent: true, contentType: 'application/json', messageId: event.eventId },
      (err) => (err ? reject(err) : resolve())
    );
  });

  return event;
}

async function closePublisher() {
  if (connection) await connection.close().catch(() => {});
}

module.exports = { publishEvent, closePublisher };
