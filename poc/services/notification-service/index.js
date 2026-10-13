require('dotenv').config();
const amqp = require('amqplib');

const URL = process.env.RABBITMQ_URL;
const EXCHANGE = process.env.RABBITMQ_EXCHANGE || 'orders';
const QUEUE = process.env.RABBITMQ_QUEUE || 'notification.orders';
const BINDING = process.env.RABBITMQ_BINDING || 'order.*';
const DELAY_MS = Number(process.env.PROCESSING_DELAY_MS) || 0;

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

async function start() {
  const connection = await amqp.connect(URL);
  connection.on('error', (err) => console.error('[consumer] connection error:', err.message));
  connection.on('close', () => {
    console.error('[consumer] connection closed; exiting so a supervisor can restart us');
    process.exit(1);
  });

  const channel = await connection.createChannel();

  // Must match the publisher's declaration exactly, or RabbitMQ rejects it.
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, BINDING);
  await channel.prefetch(1);

  console.log(`Notification service waiting on queue "${QUEUE}" (binding "${BINDING}")`);

  await channel.consume(
    QUEUE,
    async (msg) => {
      if (msg === null) return;   // consumer cancelled by the broker

      let event;
      try {
        event = JSON.parse(msg.content.toString());
      } catch (err) {
        console.error('[consumer] unparseable message, discarding:', err.message);
        channel.nack(msg, false, false);   // no requeue; would loop forever otherwise
        return;
      }

      try {
        if (DELAY_MS) await sleep(DELAY_MS);
        sendNotification(event);
        channel.ack(msg);                  // only now does RabbitMQ delete the message
      } catch (err) {
        console.error(`[consumer] failed on eventId ${event.eventId}, discarding:`, err.message);
        channel.nack(msg, false, false);
      }
    },
    { noAck: false }
  );

  const shutdown = async () => {
    console.log('Shutting down...');
    await channel.close().catch(() => {});
    await connection.close().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

start().catch((err) => {
  console.error('Failed to start notification service:', err.message);
  process.exit(1);
});
