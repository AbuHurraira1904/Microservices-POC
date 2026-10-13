// Transactional outbox for the Order service.
//
// The problem: updating the order in the database and publishing to RabbitMQ are
// two separate writes. A crash (or a broker outage) between them loses the event.
//
// The fix, in two halves:
//  1. index.js writes the event into the outbox_events table in the SAME database
//     transaction as the order's final status, so both happen or neither does.
//  2. This relay loop reads unpublished rows and publishes them to RabbitMQ, then
//     marks them published. If RabbitMQ is down, rows simply wait.
//
// Delivery is at-least-once: a crash after publishing but before the row is marked
// means the event is sent again. It carries the same eventId, and the consumer's
// duplicate check (processed_events) ignores it.
//
// Known limits: published rows are never deleted (no cleanup job), and a publish
// that hangs without any broker answer would stall the relay (no timeout).
//
// Keep this module's require() after dotenv.config() in index.js.

const { publishEvent } = require('./publisher');

const RELAY_INTERVAL_MS = Number(process.env.OUTBOX_RELAY_INTERVAL_MS) || 1000;
const BATCH_SIZE = 10;

// Creates the table on startup if it does not exist (safe to run every time).
async function ensureOutboxSchema(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS outbox_events (
      event_id     uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      type         text NOT NULL,
      payload      jsonb NOT NULL,
      created_at   timestamptz NOT NULL DEFAULT now(),
      published_at timestamptz
    )
  `);
  // Partial index: the relay only ever looks at rows that are still unpublished.
  await pool.query(`
    CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
      ON outbox_events (created_at) WHERE published_at IS NULL
  `);
}

let running = false;       // guard: never run two relay passes at once
let brokerDown = false;    // so a long outage logs once, not every second

async function relayOnce(pool) {
  if (running) return;
  running = true;

  let client;
  try {
    client = await pool.connect();
    await client.query('BEGIN');

    // FOR UPDATE SKIP LOCKED: rows being handled by another relay are skipped,
    // so two Order instances never publish the same row.
    const { rows } = await client.query(
      `SELECT event_id, type, payload, created_at
         FROM outbox_events
        WHERE published_at IS NULL
        ORDER BY created_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [BATCH_SIZE]
    );

    let relayed = 0;
    for (const row of rows) {
      try {
        // Reuse the stored eventId and creation time (see publisher.js).
        await publishEvent(row.type, row.payload, {
          eventId: row.event_id,
          timestamp: row.created_at.toISOString(),
        });
      } catch (err) {
        if (!brokerDown) console.error(`[outbox] cannot publish, events wait in the table: ${err.message}`);
        brokerDown = true;
        break;   // stop this pass; rows already marked below are still committed
      }
      brokerDown = false;
      await client.query('UPDATE outbox_events SET published_at = now() WHERE event_id = $1', [row.event_id]);
      relayed += 1;
    }

    await client.query('COMMIT');
    if (relayed) console.log(`[outbox] relayed ${relayed} event(s) to RabbitMQ`);
  } catch (err) {
    if (client) await client.query('ROLLBACK').catch(() => {});
    console.error('[outbox] relay pass failed, will retry:', err.message);
  } finally {
    if (client) client.release();
    running = false;
  }
}

function startRelay(pool) {
  console.log(`[outbox] relay started (every ${RELAY_INTERVAL_MS} ms)`);
  return setInterval(() => relayOnce(pool), RELAY_INTERVAL_MS);
}

module.exports = { ensureOutboxSchema, startRelay };
