// =============================================================================
// NOTIFICATION SERVICE  (no web port: it only listens to RabbitMQ)
// =============================================================================
// Sends the customer a receipt when an order is confirmed, or a notice when it
// failed. It never talks to the Order service directly. Order drops an event
// ("order.confirmed" / "order.failed") into RabbitMQ and this service picks it
// up. That is the point of this service: Order does not wait for it, does not
// even know it exists, and keeps working while Notification is down. The events
// wait in a queue and are handled when it comes back.
//
// (In this demo the "sending" is just a line printed to the console.)
//
// Words used in this file
//   - Queue: a waiting line of messages inside RabbitMQ. This service owns the
//     queue "notification.orders".
//   - Binding: the rule that copies matching messages from the exchange into
//     the queue ("order.*" = every event whose name starts with "order.").
//   - Ack ("acknowledge"): telling RabbitMQ "I finished this message, delete
//     it". Until then RabbitMQ keeps it and will hand it out again.
//   - Nack: telling RabbitMQ "I could not handle this message".
//   - Idempotent: doing the same thing twice has the same effect as doing it
//     once. Here: handling the same event twice must not send two receipts.
//   - Dead-letter queue (DLQ): a side queue where rejected messages are parked
//     instead of being thrown away, so a person can look at them later.
//   - Transaction: a group of database changes that either all happen or none do.
//
// How a message is handled (the reliability chain)
//   1. The message arrives. If it is not valid JSON, or has no eventId, it is
//      rejected into the dead-letter queue.
//   2. In ONE database transaction: record the eventId in `processed_events`,
//      and do the work. If the eventId was already recorded, this is a repeat:
//      skip the work.
//   3. Only after that, acknowledge the message so RabbitMQ deletes it.
//
// Decisions made here, and why
//   - Acknowledge AFTER the work, not before. If the service crashes midway,
//     RabbitMQ still holds the message and delivers it again. Nothing is lost.
//     The price: the same message can arrive twice ("at least once" delivery).
//   - Because repeats are possible, each event carries a unique eventId, and
//     the ids already handled are stored in the DATABASE. Not in memory: memory
//     is wiped by the very restart that causes the repeat.
//   - Recording the id and doing the work happen in one transaction, so there
//     is no moment where the id is saved but the work is not (that would lose
//     the receipt) or the work is done but the id is not saved (that would
//     send a second receipt).
//   - "prefetch(1)": the service holds only one unfinished message at a time.
//     Otherwise one slow or stuck service could hoard messages other workers
//     could be handling.
//   - A rejected message goes to the dead-letter queue instead of being
//     deleted, so a malformed or failing message is never lost silently.
//   - Messages are rejected WITHOUT being put back in the queue ("requeue =
//     false"). Putting a broken message back would make it fail again, forever.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Real side effects (a sent email) cannot be rolled back. If the email goes
//     out and the commit then fails, the message is retried and a second email
//     goes out. Exactly-once effects need the email provider's own idempotency
//     key, or a "send" step that is itself safe to repeat.
//   - Retry with growing delays before giving up. Today a temporary problem
//     (for example the database being down for a minute) sends the message
//     straight to the dead-letter queue; failures that deserve a retry and
//     failures that never will succeed need separate handling.
//   - Monitor and alert on the dead-letter queue, and provide a tool to inspect
//     and replay its messages.
//   - Clean up old rows in `processed_events` after a retention period
//     (longer than RabbitMQ could ever redeliver), or the table grows forever.
//   - Create tables with migration files, and declare queues/exchanges in one
//     infrastructure step instead of from each service.
//   - Run under a supervisor (Docker restart policy, Kubernetes) so the
//     "exit when the connection drops" behaviour below restarts it
//     automatically; reconnect with backoff instead of exiting.
//   - Use TLS to RabbitMQ and a dedicated, limited RabbitMQ user.
//   - Handle several event types with a lookup table, validate each event
//     against a schema, and keep a version number on events.
// =============================================================================

// Load the settings from the .env file first.
require('dotenv').config();
const amqp = require('amqplib');
const { Pool } = require('pg');

const URL = process.env.RABBITMQ_URL;
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || 'orders';
const QUEUE = process.env.RABBITMQ_QUEUE || 'notification.orders';
const BINDING = process.env.RABBITMQ_BINDING || 'order.*';
// Dead-letter exchange and queue: where rejected messages are parked.
const DLX = process.env.RABBITMQ_DLX || 'orders.dlx';
const DLQ = process.env.RABBITMQ_DLQ || `${QUEUE}.dlq`;
// Demo helper: artificial processing time per message, so you can see
// messages "in flight" and stop the service mid-processing.
const DELAY_MS = Number(process.env.PROCESSING_DELAY_MS) || 0;
// Demo helper: pause AFTER the database commit but BEFORE the ack, so you can
// stop the service in the exact window where duplicates are born.
const ACK_DELAY_MS = Number(process.env.ACK_DELAY_MS) || 0;

// Idempotency store. A database table (not an in-memory Set) on purpose:
// memory is wiped on restart, and a restart is exactly when RabbitMQ redelivers.
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// The "business logic": a fake receipt / failure notice. In a real system this
// is where the email, SMS or push message would be sent. An event type we do
// not know is logged and counted as handled (it is not an error, and retrying
// it would never help).
function sendNotification(event) {
  if (event.type === 'order.confirmed') {
    console.log(
      `[RECEIPT] order #${event.orderId}: ${event.quantity} x ${event.sku}, ` +
      `total $${event.amount}, transaction ${event.transactionId} ` +
      `(eventId ${event.eventId})`
    );
  } else if (event.type === 'order.failed') {
    console.log(
      `[FAILURE NOTICE] order #${event.orderId}: ${event.quantity} x ${event.sku} ` +
      `could not be completed (eventId ${event.eventId})`
    );
  } else {
    console.log(`[IGNORED] unknown event type "${event.type}" (eventId ${event.eventId})`);
  }
}

// Creates the table of already-handled eventIds when the service starts.
// "IF NOT EXISTS" makes it safe to run on every start. The PRIMARY KEY on
// event_id is what makes duplicates detectable: the database refuses to store
// the same id twice.
// PRODUCTION: use migration files, and delete old rows (see above).
async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id     uuid PRIMARY KEY,
      processed_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

async function start() {
  // Connect to the two dependencies, and label each step so a failure says
  // WHICH one was unreachable (a bare "ECONNRESET" does not). The RabbitMQ
  // password is masked in the error message.
  await ensureSchema().catch((err) => {
    throw new Error(`Postgres (${process.env.DB_HOST}:${process.env.DB_PORT}) unreachable: ${err.message}`);
  });
  const connection = await amqp.connect(URL).catch((err) => {
    throw new Error(`RabbitMQ (${URL.replace(/\/\/.*@/, '//***@')}) unreachable: ${err.message}`);
  });
  // Without an 'error' listener Node would treat the event as fatal.
  connection.on('error', (err) => console.error('[consumer] connection error:', err.message));
  // If the connection is lost, stop instead of sitting there deaf. A supervisor
  // is expected to start us again (see PRODUCTION CHANGES).
  connection.on('close', () => {
    console.error('[consumer] connection closed; exiting so a supervisor can restart us');
    process.exit(1);
  });

  const channel = await connection.createChannel();

  // Declare the exchange the Order service publishes to. Must match the
  // publisher's declaration exactly (name, type, durable), or RabbitMQ rejects it.
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });

  // Dead-letter side. A fanout exchange sends every dead-lettered message to the
  // DLQ whatever its routing key. Declared BEFORE the main queue, which refers to it.
  await channel.assertExchange(DLX, 'fanout', { durable: true });
  await channel.assertQueue(DLQ, { durable: true });
  await channel.bindQueue(DLQ, DLX, '');

  // Main queue. 'x-dead-letter-exchange' tells the broker: when a consumer
  // rejects a message with nack(requeue=false), send it to the DLX instead of
  // deleting it. NOTE: RabbitMQ refuses to redeclare an existing queue with
  // different arguments, so an old "notification.orders" queue must be deleted
  // once (management UI -> Queues) before this version can start.
  // "durable" means the queue itself survives a RabbitMQ restart, so events
  // wait here safely while this service is down.
  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  // The binding: copy every message whose routing key matches "order.*" into
  // our queue.
  await channel.bindQueue(QUEUE, EXCHANGE, BINDING);
  await channel.prefetch(1);

  console.log(
    `Notification service waiting on queue "${QUEUE}" (binding "${BINDING}"), ` +
    `rejected messages go to "${DLQ}"`
  );

  // `noAck: false` = manual acknowledgement: RabbitMQ keeps each message until
  // we explicitly ack it below.
  await channel.consume(
    QUEUE,
    async (msg) => {
      if (msg === null) return;   // consumer cancelled by the broker

      // Rejection 1: the body is not valid JSON ("poison" message).
      let event;
      try {
        event = JSON.parse(msg.content.toString());
      } catch (err) {
        console.error('[consumer] unparseable message, dead-lettering:', err.message);
        channel.nack(msg, false, false);   // no requeue; would loop forever otherwise
        return;
      }

      // Rejection 2: no eventId, so we could never tell a repeat from a new event.
      // Without an eventId we cannot tell duplicates apart, so reject the message.
      if (!event || !event.eventId) {
        console.error('[consumer] message has no eventId, dead-lettering');
        channel.nack(msg, false, false);
        return;
      }

      // Step 1: record the eventId and do the work in ONE database transaction.
      //   INSERT ... ON CONFLICT DO NOTHING inserts 0 rows if the id is already
      //   stored, which means we have processed this event before.
      //   If the work throws, ROLLBACK removes the id again, so a retry is not
      //   mistaken for a duplicate.
      // An eventId that is not a valid UUID makes the INSERT fail, so that
      // message ends up in the dead-letter queue through the catch below.
      let client;
      let duplicate = false;
      try {
        client = await pool.connect();   // inside the try: a database outage must not crash the consumer
        await client.query('BEGIN');
        const res = await client.query(
          'INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT (event_id) DO NOTHING',
          [event.eventId]
        );
        if (res.rowCount === 0) {
          // Nothing was inserted: the id was already there. A repeat.
          duplicate = true;
        } else {
          if (DELAY_MS) await sleep(DELAY_MS);
          sendNotification(event);
        }
        await client.query('COMMIT');
      } catch (err) {
        // Rejection 3: processing failed. Undo the transaction (this also
        // removes the recorded id, so the event is not wrongly marked as done)
        // and park the message in the dead-letter queue.
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`[consumer] failed on eventId ${event.eventId}, dead-lettering:`, err.message);
        channel.nack(msg, false, false);
        return;
      } finally {
        // Always hand the database connection back.
        if (client) client.release();
      }

      if (duplicate) {
        console.log(`[DUPLICATE] eventId ${event.eventId} already processed, skipping`);
      }

      // Step 2: only now acknowledge. If we die before this line, RabbitMQ
      // redelivers the message and Step 1 recognises it as a duplicate.
      // (A duplicate is acknowledged too, so RabbitMQ finally deletes it.)
      if (ACK_DELAY_MS) await sleep(ACK_DELAY_MS);
      channel.ack(msg);
    },
    { noAck: false }
  );

  // Clean shutdown on Ctrl+C or a stop signal. Closing the channel makes
  // RabbitMQ put back any message we had not acknowledged, so nothing is lost.
  const shutdown = async () => {
    console.log('Shutting down...');
    await channel.close().catch(() => {});
    await connection.close().catch(() => {});
    await pool.end().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  console.error('Failed to start notification service:', err.message);
  process.exit(1);
});
