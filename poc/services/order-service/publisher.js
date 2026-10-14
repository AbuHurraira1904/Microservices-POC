// =============================================================================
// RABBITMQ PUBLISHER  (Order service)
// =============================================================================
// Sends events (such as "order.confirmed") to RabbitMQ, a message broker: a
// post office that holds messages until the services interested in them pick
// them up. The sender does not need to know who the receivers are, or whether
// they are running at the moment.
//
// Who calls this: only the outbox relay (outbox.js). The order request itself
// never talks to RabbitMQ, so a broker outage cannot break an order.
//
// Words used in this file
//   - Exchange: the sorting desk in the broker. A publisher hands a message to
//     an exchange, and the exchange routes it to queues.
//   - Topic exchange: routes by a label on the message (the "routing key",
//     here the event type, e.g. "order.confirmed"). Receivers subscribe to
//     patterns such as "order.*".
//   - Durable: the exchange survives a restart of RabbitMQ.
//   - Persistent message: the broker writes the message to disk.
//
// Reliability choices (each one is a talking point for the founder)
//   - Durable exchange + persistent messages: events survive a broker restart.
//     Either one alone is not enough.
//   - Confirm channel: normally a sender gets no answer after handing over a
//     message, so it cannot know whether the broker really accepted it. On a
//     confirm channel the broker answers for every message. publishEvent()
//     only finishes once that confirmation arrives, so a lost event is never
//     silent. The outbox relies on this: it marks a row "sent" only after the
//     confirmation.
//   - Lazy connect + reconnect: the connection opens on first use. If it drops,
//     the next publish opens a new one. The service therefore starts and keeps
//     working even if RabbitMQ is down at that moment.
//
// Why events are not sent straight from the order request: see outbox.js. A
// crash between saving the order and sending the event would lose the event.
// The outbox keeps the event in the database until it is delivered, and a
// resend carries the SAME eventId, which the consumer's duplicate check ignores.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Use TLS (amqps://) and a dedicated RabbitMQ user with only the rights
//     this service needs, instead of the shared demo login.
//   - Run RabbitMQ as a cluster with replicated ("quorum") queues, so one
//     machine failing loses nothing.
//   - Declare exchanges and queues in one deployment/infrastructure step,
//     not from several services at startup.
//   - Add a time limit for each confirmation, and metrics (publish count,
//     failures, time to confirm).
//   - Add a schema/version field to events, so consumers can handle old and new
//     formats during an upgrade.
//   - Close the connection cleanly on shutdown (closePublisher() exists for
//     this but nothing calls it yet).
//
// Keep this module's require() after dotenv.config() in index.js, because it
// reads its settings from process.env when it loads.
// =============================================================================

const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const URL = process.env.RABBITMQ_URL;
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || 'orders';

let connection = null;
let channelPromise = null;

// Returns the (single, shared) confirm channel, creating it on first use.
// A channel is a lightweight lane inside one connection; one connection and
// one channel are enough for this service.
function getChannel() {
  // We store the PROMISE, not the finished channel. If two publishes arrive
  // while the connection is still opening, both wait on the same promise
  // instead of each opening their own connection.
  if (channelPromise) return channelPromise;

  channelPromise = (async () => {
    try {
      const conn = await amqp.connect(URL);
      // Without an 'error' listener Node treats the event as fatal and the
      // whole service would crash. We log it instead.
      conn.on('error', (err) => console.error('[publisher] connection error:', err.message));
      // On close, forget the connection. The next publish then builds a new
      // one: this is the reconnect.
      conn.on('close', () => {
        console.warn('[publisher] connection closed; will reconnect on next publish');
        connection = null;
        channelPromise = null;
      });

      // A confirm channel makes the broker acknowledge every message.
      const ch = await conn.createConfirmChannel();
      ch.on('error', (err) => console.error('[publisher] channel error:', err.message));
      // Declare the exchange so publishing works even if nobody has created it
      // yet. This must match the receivers' declaration exactly (same name,
      // type and durable flag) or RabbitMQ refuses.
      await ch.assertExchange(EXCHANGE, 'topic', { durable: true });

      connection = conn;
      return ch;
    } catch (err) {
      // Forget the failed attempt so the next call tries again, instead of
      // every future call receiving this same failure.
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
  // The message body. eventId is the unique name of this event; the consumer
  // uses it to recognise a message it has already handled.
  const event = {
    eventId: options.eventId || randomUUID(),
    type,
    ...payload,   // orderId, sku, quantity, amount, transactionId
    timestamp: options.timestamp || new Date().toISOString(),
  };

  const ch = await getChannel();

  // The library reports the broker's confirmation through a callback
  // ("null" = accepted, an error = refused). We wrap it in a Promise so the
  // caller can simply `await` it.
  await new Promise((resolve, reject) => {
    ch.publish(
      EXCHANGE,
      type,                                   // routing key, e.g. "order.confirmed"
      Buffer.from(JSON.stringify(event)),
      // persistent: write to disk. messageId repeats the eventId in the message
      // headers, which helps when inspecting messages in the RabbitMQ UI.
      { persistent: true, contentType: 'application/json', messageId: event.eventId },
      (err) => (err ? reject(err) : resolve())
    );
  });

  return event;
}

// Closes the connection. Nothing calls this yet (see PRODUCTION CHANGES).
async function closePublisher() {
  if (connection) await connection.close().catch(() => {});
}

module.exports = { publishEvent, closePublisher };
