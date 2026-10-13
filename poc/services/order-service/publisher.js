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
// what to do (in Order: log it, and do not change the HTTP response).
async function publishEvent(type, payload) {
  const event = {
    eventId: randomUUID(),
    type,
    ...payload,   // orderId, sku, quantity, amount, transactionId
    timestamp: new Date().toISOString(),
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
