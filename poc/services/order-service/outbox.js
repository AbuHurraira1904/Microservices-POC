// =============================================================================
// OUTBOX  (Order service)
// =============================================================================
// Problem this solves
//   When an order finishes, two things must happen: (1) save the result in the
//   database, and (2) tell the other services through RabbitMQ. These are two
//   separate systems, so no single "all or nothing" operation covers both. If
//   the process crashed, or RabbitMQ was down, between the two steps, the order
//   would be saved but the announcement lost forever. This is called the
//   "dual-write problem".
//
// The fix: the outbox pattern, in two halves
//   1. index.js saves the event in the table `outbox_events`, inside the SAME
//      database transaction as the order's final status. A transaction is a
//      group of changes that either all happen or none do, so the order result
//      and its event can never get out of step.
//   2. THIS file runs a small loop (the "relay") that picks up saved events that
//      have not been sent yet, sends them to RabbitMQ, and marks them as sent.
//      If RabbitMQ is down, the events simply wait in the table and go out when
//      it returns.
//
// Delivery guarantee: "at least once"
//   The relay never loses an event, but it may send one twice. For example, if
//   the process dies after RabbitMQ accepted a batch but before the batch was
//   marked as sent, the next pass sends those events again. Every resend keeps
//   the SAME eventId, and the Notification service remembers the ids it has
//   handled (idempotent consumer), so a repeat does no harm.
//
// Decisions made here, and why
//   - Events live in the database we already run (no extra system to operate).
//   - The row's own id is reused as the eventId, so a resend is recognisable.
//   - "FOR UPDATE SKIP LOCKED" lets several copies of the Order service run the
//     relay side by side without sending the same row twice (see relayOnce).
//   - A partial index covers only unsent rows, so finding work stays fast even
//     when the table holds millions of old, already-sent rows.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Delete or archive old sent rows (a cleanup job); today the table only grows.
//   - Add a time limit to each send. A send that hangs without any answer from
//     RabbitMQ would hold this loop (and its database lock) indefinitely.
//   - Do not hold a database transaction open while talking to the network:
//     claim rows first, send, then mark them in a short second transaction.
//   - Keep a retry counter per row, and park events that keep failing in a
//     "dead" state with an alert, instead of retrying them forever.
//   - Replace polling every second with a change-data-capture tool (for
//     example Debezium), which streams new rows with no polling delay or load.
//   - Add metrics (rows waiting, age of the oldest waiting row) and alerting.
//
// Keep this module's require() after dotenv.config() in index.js, because it
// reads settings from process.env when it loads.
// =============================================================================

const { publishEvent } = require('./publisher');

// How often the relay looks for unsent events. One second keeps the demo
// responsive; the value can be changed in .env.
const RELAY_INTERVAL_MS = Number(process.env.OUTBOX_RELAY_INTERVAL_MS) || 1000;
// Maximum events sent per pass, so one pass always stays short.
const BATCH_SIZE = 10;

// Creates the outbox table when the service starts. "IF NOT EXISTS" makes this
// safe to run on every start: the first start creates the table, later starts
// do nothing.
// PRODUCTION: create tables with migration files (reviewed, versioned, run
// once by a deploy step), not from application code at startup.
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
  // A "partial" index only lists the rows that match the WHERE condition, here
  // the unsent ones. The relay asks "what is not sent yet?" every second, and
  // this index answers without scanning the whole table.
  await pool.query(`
    CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
      ON outbox_events (created_at) WHERE published_at IS NULL
  `);
}

let running = false;       // guard: never run two relay passes at once
let brokerDown = false;    // so a long outage logs once, not every second

// One pass of the relay: take up to BATCH_SIZE unsent events, send them in
// order, mark each as sent.
async function relayOnce(pool) {
  // The timer fires every second even if the previous pass is still running
  // (for example during a slow publish). This guard skips that tick, so two
  // passes inside this process never overlap.
  if (running) return;
  running = true;

  let client;
  try {
    // One dedicated connection and one transaction for the whole pass, because
    // the row locks taken below only last as long as the transaction.
    client = await pool.connect();
    await client.query('BEGIN');

    // FOR UPDATE SKIP LOCKED: "lock the rows I am about to send, and skip any
    // row another relay has already locked." If two copies of the Order
    // service run, each gets a different set of rows, so no row is sent twice
    // at the same moment. ORDER BY created_at keeps events in the order they
    // happened.
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
        // Reuse the stored eventId and creation time (see publisher.js), so a
        // resend is identical to the first send and the consumer can spot it.
        await publishEvent(row.type, row.payload, {
          eventId: row.event_id,
          timestamp: row.created_at.toISOString(),
        });
      } catch (err) {
        // RabbitMQ is unreachable or refused the message. Log it once per
        // outage (not once a second), stop this pass, and leave the remaining
        // rows untouched. They are tried again on the next tick.
        if (!brokerDown) console.error(`[outbox] cannot publish, events wait in the table: ${err.message}`);
        brokerDown = true;
        break;   // stop this pass; rows already marked below are still committed
      }
      brokerDown = false;
      await client.query('UPDATE outbox_events SET published_at = now() WHERE event_id = $1', [row.event_id]);
      relayed += 1;
    }

    // The "sent" marks become permanent only here. A crash before this line
    // rolls them back, and those events are sent again next time (harmless:
    // same eventId, see "Delivery guarantee" above).
    await client.query('COMMIT');
    if (relayed) console.log(`[outbox] relayed ${relayed} event(s) to RabbitMQ`);
  } catch (err) {
    // A database problem. Undo the pass; the next tick tries again.
    if (client) await client.query('ROLLBACK').catch(() => {});
    console.error('[outbox] relay pass failed, will retry:', err.message);
  } finally {
    if (client) client.release();
    running = false;
  }
}

// Starts the relay: runs one pass per interval. Returns the timer so a caller
// could stop it (nothing does yet).
function startRelay(pool) {
  console.log(`[outbox] relay started (every ${RELAY_INTERVAL_MS} ms)`);
  return setInterval(() => relayOnce(pool), RELAY_INTERVAL_MS);
}

module.exports = { ensureOutboxSchema, startRelay };
