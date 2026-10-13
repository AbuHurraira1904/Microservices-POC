// Notification service: a RabbitMQ consumer with a small Postgres table for
// idempotency (no HTTP port).
//
// Reliability choices:
//  - Durable named queue: messages wait here while this service is down.
//  - Manual ack: a message is removed from the queue only after we finish with it.
//    If we crash mid-processing, RabbitMQ redelivers it (at-least-once delivery).
//  - prefetch(1): one unacknowledged message at a time, so work is not hoarded.
//  - Idempotent consumer: each eventId is stored in Postgres (processed_events)
//    in the same transaction as the work, so redelivered duplicates are skipped.
//  - Dead-letter queue: a rejected message (unparseable, no eventId, or failing)
//    is not discarded; the broker routes it to the DLQ, where it can be inspected.
//
// Known gaps: a real side effect (an email already sent) cannot be rolled back,
// so true exactly-once effects need an idempotent downstream. A failure caused by
// a temporary outage (for example Postgres down) is also dead-lettered; retry
// with backoff is a deferred item.

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

// The "business logic": a fake receipt / failure notice.
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

// Create the table on startup if it does not exist (safe to run every time).
async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS processed_events (
      event_id     uuid PRIMARY KEY,
      processed_at timestamptz NOT NULL DEFAULT now()
    )
  `);
}

async function start() {
  // Label each startup step so a failure says WHICH dependency was unreachable.
  await ensureSchema().catch((err) => {
    throw new Error(`Postgres (${process.env.DB_HOST}:${process.env.DB_PORT}) unreachable: ${err.message}`);
  });
  const connection = await amqp.connect(URL).catch((err) => {
    throw new Error(`RabbitMQ (${URL.replace(/\/\/.*@/, '//***@')}) unreachable: ${err.message}`);
  });
  connection.on('error', (err) => console.error('[consumer] connection error:', err.message));
  connection.on('close', () => {
    console.error('[consumer] connection closed; exiting so a supervisor can restart us');
    process.exit(1);
  });

  const channel = await connection.createChannel();

  // Must match the publisher's declaration exactly, or RabbitMQ rejects it.
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
  await channel.assertQueue(QUEUE, {
    durable: true,
    arguments: { 'x-dead-letter-exchange': DLX },
  });
  await channel.bindQueue(QUEUE, EXCHANGE, BINDING);
  await channel.prefetch(1);

  console.log(
    `Notification service waiting on queue "${QUEUE}" (binding "${BINDING}"), ` +
    `rejected messages go to "${DLQ}"`
  );

  await channel.consume(
    QUEUE,
    async (msg) => {
      if (msg === null) return;   // consumer cancelled by the broker

      let event;
      try {
        event = JSON.parse(msg.content.toString());
      } catch (err) {
        console.error('[consumer] unparseable message, dead-lettering:', err.message);
        channel.nack(msg, false, false);   // no requeue; would loop forever otherwise
        return;
      }

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
          duplicate = true;
        } else {
          if (DELAY_MS) await sleep(DELAY_MS);
          sendNotification(event);
        }
        await client.query('COMMIT');
      } catch (err) {
        if (client) await client.query('ROLLBACK').catch(() => {});
        console.error(`[consumer] failed on eventId ${event.eventId}, dead-lettering:`, err.message);
        channel.nack(msg, false, false);
        return;
      } finally {
        if (client) client.release();
      }

      if (duplicate) {
        console.log(`[DUPLICATE] eventId ${event.eventId} already processed, skipping`);
      }

      // Step 2: only now acknowledge. If we die before this line, RabbitMQ
      // redelivers the message and Step 1 recognises it as a duplicate.
      if (ACK_DELAY_MS) await sleep(ACK_DELAY_MS);
      channel.ack(msg);
    },
    { noAck: false }
  );

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
