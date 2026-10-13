require('dotenv').config();
const grpc = require('@grpc/grpc-js');
const paymentClient = require('./paymentClient');   // keep this after dotenv.config()
const { publishEvent } = require('./publisher');    // keep this after dotenv.config()
const { ensureOutboxSchema, startRelay } = require('./outbox');   // keep this after dotenv.config()

const express = require('express');
const { Pool } = require('pg');

const app = express();
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// Calls Inventory's commit/release. Throws on any non-2xx so failures are never silent.
async function callInventory(action, sku, quantity) {
  const response = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/${action}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sku, quantity }),
    signal: AbortSignal.timeout(3000),
  });
  if (!response.ok) throw new Error(`inventory ${action} failed with status ${response.status}`);
  return response.json();
}

async function emitEvent(type, payload) {
  try {
    await publishEvent(type, payload);
  } catch (err) {
    console.error(`event ${type} for order ${payload.orderId} was NOT published:`, err.message);
  }
}

// Create a new order
app.post('/orders', async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  let reserved = false;   // stock is currently held and must be released on failure
  let paid = false;       // payment was approved, so stock must NOT be released
  let orderId = null;

  try {
    // 1. Reserve stock (REST). Pass Inventory's 404/409 through to the client.
    const reserveResponse = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/reserve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
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

    // 2. Amount is derived server-side from Inventory's price
    const { price } = await reserveResponse.json();
    const amount = price * quantity;

    // 3. Record the order as PENDING (Payment needs an order_id)
    const inserted = await pool.query(
      "INSERT INTO orders (amount, sku, quantity, status) VALUES ($1, $2, $3, 'PENDING') RETURNING id",
      [amount, sku, quantity]
    );
    orderId = inserted.rows[0].id;

    // 4. Charge (gRPC over mTLS)
    const payment = await paymentClient.charge(orderId, amount);

    // 5a. Declined: definite "no" -> compensate
    if (!payment.approved) {
      await callInventory('release', sku, quantity);
      reserved = false;
      await pool.query("UPDATE orders SET status = 'FAILED' WHERE id = $1", [orderId]);

      await emitEvent('order.failed', { orderId, sku, quantity, amount, transactionId: null });

      return res.status(402).json({ error: 'Payment declined', reason: payment.reason });
    }

    // 5b. Approved: finalize the sale
    paid = true;
    await callInventory('commit', sku, quantity);
    reserved = false;

    const confirmed = await pool.query(
      "UPDATE orders SET status = 'CONFIRMED', transaction_id = $1 WHERE id = $2 RETURNING *",
      [payment.transaction_id, orderId]
    );

    await emitEvent('order.confirmed', {
    orderId, sku, quantity, amount, transactionId: payment.transaction_id,
    });

    return res.status(201).json({
      message: 'Order placed. Price of the order is $' + amount,
      order: confirmed.rows[0],
    });
  } catch (err) {
    console.error(err);

    // Money was taken but finalization failed: releasing stock here would be wrong.
    // Flag the order for reconciliation instead.
    if (paid) {
      if (orderId) {
        await pool.query("UPDATE orders SET status = 'PAID_UNRECONCILED' WHERE id = $1", [orderId])
          .catch(console.error);
      }
      return res.status(500).json({ error: 'Payment taken but order could not be finalized', orderId });
    }

    // Otherwise undo what we did
    if (reserved) await callInventory('release', sku, quantity).catch(console.error);
    if (orderId) {
      await pool.query("UPDATE orders SET status = 'FAILED' WHERE id = $1", [orderId])
        .catch(console.error);
      await emitEvent('order.failed', { orderId, sku, quantity, amount: null, transactionId: null });
    }

    const status = err.code === grpc.status.DEADLINE_EXCEEDED ? 504 : 503;
    return res.status(status).json({ error: 'Service Unavailable' });
  }
});

// Get all orders
app.get('/orders', async (req, res) => {
  const result = await pool.query('SELECT * FROM orders');
  res.json(result.rows);
});

// Get a specific order by ID
app.get('/orders/:id', async (req, res) => {
  const result = await pool.query('SELECT * FROM orders WHERE id = $1', [req.params.id]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows[0]);
});

const PORT = 4001;

// Create the outbox table, start the relay, then accept requests.
ensureOutboxSchema(pool)
  .then(() => {
    startRelay(pool);
    app.listen(PORT, () => console.log(`Order service on port ${PORT}`));
  })
  .catch((err) => {
    console.error('Failed to start order service:', err.message);
    process.exit(1);
  });