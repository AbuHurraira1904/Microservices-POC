// =============================================================================
// ORDER SERVICE  (port 4001)
// =============================================================================
// This is the "front door" for customers. It takes an order, then coordinates
// the other services so that the order either completes fully or is undone:
//
//   1. Reserve the stock            -> Inventory service  (HTTP/REST)
//   2. Save the order as PENDING    -> this service's own database
//   3. Charge the customer          -> Payment service    (gRPC over mTLS)
//   4. Success: make the stock sale final, mark the order CONFIRMED
//      Failure: give the stock back,       mark the order FAILED
//   5. Record an event ("order.confirmed" / "order.failed") so that other
//      services, such as Notification, can react.
//
// Words used in this file
//   - Compensation: undoing an earlier step because a later step failed
//     (for example, releasing reserved stock when the payment is declined).
//     Money and stock live in different services, so no single database
//     "rollback" can undo both; we undo each step with its own opposite action.
//   - Transaction: a group of database changes that either ALL happen or NONE
//     happen.
//   - Outbox: a table where events are saved first and sent to RabbitMQ later
//     (see outbox.js).
//   - JWT: a signed token that proves who the caller is (see auth.js).
//
// Decisions made here, and why
//   - The order flow is run by this one service ("orchestration") instead of
//     each service reacting to events ("choreography"). The whole flow is in
//     one readable place and the customer gets an immediate answer (201, 402,
//     503). The cost: if this process crashes mid-order, nothing resumes it.
//   - The price is read from Inventory and multiplied here. The client never
//     sends an amount, so a customer cannot pay less by editing the request.
//   - Stock is "reserved" first and only made final after payment succeeds.
//     Holding is cheap to undo; deducting for good is not.
//   - If payment WAS taken but the final steps fail, we do NOT give the stock
//     back: the customer has paid, so releasing it would sell it twice. The
//     order is flagged PAID_UNRECONCILED for a human to resolve.
//   - Events are not sent from inside the request. They are saved with the
//     order in one transaction and sent later by the outbox relay. This way a
//     RabbitMQ outage never breaks an order, and an order never exists without
//     its event.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Orders and reservations would be linked (a reservation tied to an order
//     id), and commit/release would be safe to repeat. Today a timeout can
//     leave stock reserved that this service does not know about.
//   - Payment would receive an idempotency key (a unique id per charge) so a
//     retry can never charge twice. Today a payment timeout is treated as a
//     failure even though the charge may have gone through.
//   - Money would be stored as whole cents (integers), not decimals.
//   - A background job would find PAID_UNRECONCILED orders and fix them.
//   - Login would be a separate identity service (see auth.js), orders would
//     record their owner, and a customer could only read their own orders.
//   - Retries with backoff and a circuit breaker (stop calling a service that
//     keeps failing) would wrap the calls to Inventory and Payment.
//   - The port would come from the PORT setting; table creation would be done
//     by migration files instead of by hand.
//   - Logs would be structured and carry a request id for tracing.
// =============================================================================

// Load the settings from the .env file FIRST. The modules required below read
// process.env at the moment they are loaded, so this line must stay on top.
require('dotenv').config();
const grpc = require('@grpc/grpc-js');
const paymentClient = require('./paymentClient');   // keep this after dotenv.config()
const { ensureOutboxSchema, startRelay } = require('./outbox');   // keep this after dotenv.config()
const { signUserToken, mintServiceToken, requireRole } = require('./auth');   // keep this after dotenv.config()
const crypto = require('crypto');

const express = require('express');
const { Pool } = require('pg');

const app = express();
// Lets Express read JSON request bodies.
app.use(express.json());

// A pool keeps a few database connections open and reuses them, which is much
// faster than opening a new connection for every request.
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// Calls Inventory's commit or release endpoint (reserve is called inline below
// because it needs its response handled differently).
//  - It sends this service's OWN short-lived "service" token, not the
//    customer's. If we forwarded the customer's token, any logged-in customer
//    could call Inventory's internal endpoints directly.
//  - The 3-second limit means a slow Inventory cannot freeze the customer's
//    request forever.
//  - It throws on any non-2xx answer, so a failed commit/release is never
//    silently ignored.
async function callInventory(action, sku, quantity) {
  const response = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/${action}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${mintServiceToken()}` },
    body: JSON.stringify({ sku, quantity }),
    signal: AbortSignal.timeout(3000),
  });
  if (!response.ok) throw new Error(`inventory ${action} failed with status ${response.status}`);
  return response.json();
}

// Saves the order's final status AND its event in ONE database transaction
// (this is the "transactional outbox").
//  - Why: previously we updated the order, then sent the event to RabbitMQ.
//    A crash between the two steps lost the event for good. Now both are
//    saved together or not at all, so an order outcome can never exist without
//    its event.
//  - Why it helps: the request no longer depends on RabbitMQ being up. The
//    relay in outbox.js delivers the event afterwards.
// Returns the updated order row; throws if the transaction fails.
async function finalizeOrder(orderId, status, transactionId, eventType, eventPayload) {
  // A transaction must run on ONE connection, so we borrow a dedicated one
  // from the pool instead of calling pool.query() twice.
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const updated = await client.query(
      'UPDATE orders SET status = $1, transaction_id = $2 WHERE id = $3 RETURNING *',
      [status, transactionId, orderId]
    );
    await client.query(
      'INSERT INTO outbox_events (type, payload) VALUES ($1, $2)',
      [eventType, eventPayload]
    );
    await client.query('COMMIT');
    return updated.rows[0];
  } catch (err) {
    // Undo both changes if either failed.
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    // Always hand the connection back, even after an error.
    client.release();
  }
}

// ---- Demo login (STUB) ------------------------------------------------------
// Two fixed demo users so the JWT checks can be demonstrated. Passwords come
// from the .env file, never from the code, so they are not in git. If a
// password is not set, that user simply cannot log in.
// PRODUCTION: users live in a database with hashed passwords (or in an
// identity provider such as Auth0 or Keycloak), and login is a separate
// service with rate limiting and lockout after repeated failures.
const DEMO_USERS = {
  customer: { password: process.env.DEMO_CUSTOMER_PASSWORD, role: 'customer' },
  admin: { password: process.env.DEMO_ADMIN_PASSWORD, role: 'admin' },
};

// Compares two passwords in a way whose duration does not depend on how many
// characters matched. A normal "===" stops at the first wrong character, and
// an attacker could measure that tiny difference to guess a password letter by
// letter. Hashing both sides first also gives the two values equal length,
// which timingSafeEqual requires.
function passwordMatches(given, expected) {
  const a = crypto.createHash('sha256').update(String(given)).digest();
  const b = crypto.createHash('sha256').update(String(expected)).digest();
  return crypto.timingSafeEqual(a, b);
}

// Exchanges a username and password for a token valid for one hour.
// Object.hasOwn() makes sure a username such as "__proto__" or "toString"
// cannot match something inherited by every JavaScript object. All failures
// give the same answer, so an attacker cannot tell a wrong username from a
// wrong password.
app.post('/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  const user = Object.hasOwn(DEMO_USERS, username) ? DEMO_USERS[username] : null;
  if (!user || !user.password || !passwordMatches(password, user.password)) {
    return res.status(401).json({ error: 'invalid credentials' });
  }
  res.json({ token: signUserToken(username, user.role), expiresIn: '1h' });
});

// ---- Create a new order -----------------------------------------------------
// Only a logged-in customer or admin may place an order (requireRole answers
// 401 or 403 before this code runs otherwise).
app.post('/orders', requireRole('customer', 'admin'), async (req, res) => {
  const { sku, quantity } = req.body;
  // Reject bad input early, before any other service is touched.
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  // These three flags remember how far we got. The catch block at the bottom
  // reads them to decide the right way to undo the work.
  let reserved = false;   // stock is currently held and must be released on failure
  let paid = false;       // payment was approved, so stock must NOT be released
  let orderId = null;

  try {
    // 1. Reserve stock (REST). The reservation moves units from "available" to
    //    "held", so two customers cannot buy the last item. If Inventory says
    //    "unknown item" (404) or "not enough stock" (409), pass that answer
    //    straight to the customer.
    const reserveResponse = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/reserve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${mintServiceToken()}` },
      body: JSON.stringify({ sku, quantity }),
      signal: AbortSignal.timeout(3000),
    });

    if (!reserveResponse.ok) {
      const errorData = await reserveResponse.json().catch(() => ({}));
      return res.status(reserveResponse.status).json({
        error: errorData.error || 'Failed to reserve inventory',
      });
    }
    reserved = true;

    // 2. The amount is calculated here from Inventory's price (see the
    //    decisions at the top of the file).
    const { price } = await reserveResponse.json();
    const amount = price * quantity;

    // 3. Record the order as PENDING first, because Payment needs an order id
    //    to attach the charge to. PENDING means "in progress, outcome unknown".
    const inserted = await pool.query(
      "INSERT INTO orders (amount, sku, quantity, status) VALUES ($1, $2, $3, 'PENDING') RETURNING id",
      [amount, sku, quantity]
    );
    orderId = inserted.rows[0].id;

    // 4. Charge the customer (gRPC over mTLS, with a 3-second limit set in
    //    paymentClient.js).
    const payment = await paymentClient.charge(orderId, amount);

    // 5a. The payment was DECLINED. This is a clear "no" (not an error), so we
    //     undo the reservation (compensation) and record the failure together
    //     with its event.
    if (!payment.approved) {
      await callInventory('release', sku, quantity);
      reserved = false;
      await finalizeOrder(orderId, 'FAILED', null, 'order.failed',
        { orderId, sku, quantity, amount, transactionId: null });

      return res.status(402).json({ error: 'Payment declined', reason: payment.reason });
    }

    // 5b. The payment was APPROVED. From this point the customer has paid, so
    //     the flow can only move forward (see the catch block). `paid` is set
    //     BEFORE the commit so a failure of the commit is handled correctly.
    paid = true;
    await callInventory('commit', sku, quantity);   // held stock becomes a final sale
    reserved = false;

    const confirmedOrder = await finalizeOrder(orderId, 'CONFIRMED', payment.transaction_id, 'order.confirmed',
      { orderId, sku, quantity, amount, transactionId: payment.transaction_id });

    return res.status(201).json({
      message: 'Order placed. Price of the order is $' + amount,
      order: confirmedOrder,
    });
  } catch (err) {
    console.error(err);

    // Money was taken but a later step failed. Releasing the stock here would
    // be WRONG (the customer paid, so the item would be sold twice). Flag the
    // order for a human instead. No event is sent: telling the customer
    // "confirmed" or "failed" would both be untrue at this point.
    if (paid) {
      if (orderId) {
        await pool.query("UPDATE orders SET status = 'PAID_UNRECONCILED' WHERE id = $1", [orderId])
          .catch(console.error);
      }
      return res.status(500).json({ error: 'Payment taken but order could not be finalized', orderId });
    }

    // Nothing was charged: undo what we did. The .catch(console.error) calls
    // make each undo step best-effort, so one failing undo does not stop the
    // others from running.
    if (reserved) await callInventory('release', sku, quantity).catch(console.error);
    if (orderId) {
      await finalizeOrder(orderId, 'FAILED', null, 'order.failed',
        { orderId, sku, quantity, amount: null, transactionId: null })
        .catch(console.error);
    }

    // 504 = a dependency was too slow (gRPC deadline exceeded);
    // 503 = a dependency was unreachable or failed.
    const status = err.code === grpc.status.DEADLINE_EXCEEDED ? 504 : 503;
    return res.status(status).json({ error: 'Service Unavailable' });
  }
});

// ---- Read orders ------------------------------------------------------------
// Admin only: the full list.
// PRODUCTION: add paging (the list would be huge) and an owner column, so
// customers can list their own orders.
app.get('/orders', requireRole('admin'), async (req, res) => {
  const result = await pool.query('SELECT * FROM orders');
  res.json(result.rows);
});

// Any logged-in user may read one order by id.
// KNOWN GAP: orders have no owner yet, so a customer can read ANY order by
// guessing its id. PRODUCTION: store the owner on the order and return 404 for
// orders that belong to someone else.
app.get('/orders/:id', requireRole('customer', 'admin'), async (req, res) => {
  const result = await pool.query('SELECT * FROM orders WHERE id = $1', [req.params.id]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows[0]);
});

// PRODUCTION: read this from process.env.PORT (it is fixed for the demo).
const PORT = 4001;

// Startup order matters: create the outbox table, start the relay, and only
// THEN accept customers. If the database is unreachable the service exits with
// an error instead of running half-broken.
ensureOutboxSchema(pool)
  .then(() => {
    startRelay(pool);
    app.listen(PORT, () => console.log(`Order service on port ${PORT}`));
  })
  .catch((err) => {
    console.error('Failed to start order service:', err.message);
    process.exit(1);
  });
