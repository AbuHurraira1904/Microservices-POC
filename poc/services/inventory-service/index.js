// =============================================================================
// INVENTORY SERVICE  (port 4002)
// =============================================================================
// Owns the stock: how many units of each item (identified by its "sku", the
// stock-keeping unit code) can be sold. Other services never touch its
// database; they ask this service, which keeps the rules in one place.
//
// The data model: two numbers per item
//   - quantity: units available to sell right now.
//   - reserved: units held for orders that are still being paid for.
//
// The two-phase reservation (why there are three "action" endpoints)
//   Buying is not one step, because the payment happens in another service in
//   the middle. So stock is handled in two phases:
//     1. reserve  available -> held    (quantity goes down, reserved goes up)
//     2a. commit  held -> gone         (payment succeeded: the sale is final)
//     2b. release held -> available    (payment failed: give the stock back;
//                                       this is a "compensation", an action
//                                       that undoes an earlier step)
//   Why not simply subtract at the start? If the payment then failed, the
//   stock would have to be added back, and in the meantime the display would
//   show too little stock. Holding makes both outcomes a simple move between
//   two numbers.
//
// Who may call what (see auth.js; "role" comes from the caller's token)
//   - reserve / commit / release : only the Order service (role "service").
//     These are internal operations. Customers must not be able to release or
//     commit stock themselves.
//   - create an item             : admins only.
//   - read stock levels          : any logged-in caller.
//
// Decisions made here, and why
//   - "Check and change" is ONE SQL statement (UPDATE ... WHERE quantity >= n).
//     The usual approach of reading the number, checking it in code, and then
//     writing it back has a gap in the middle: two customers can both read
//     "1 left" and both buy it. In a single UPDATE the database locks the row,
//     so a second request waits and then re-checks against the updated number.
//   - The follow-up SELECT after a failed UPDATE only decides which error to
//     report (item missing = 404, not enough stock = 409). It never decides
//     whether the stock may move; that was already settled atomically.
//   - Each update also has a guard (WHERE reserved >= n), so commit or release
//     can never push the held count below zero.
//
// Error codes used: 400 bad input, 401/403 not allowed (auth.js), 404 unknown
// item, 409 conflict with the current state (not enough stock), 503 the
// database failed.
//
// PRODUCTION CHANGES (what a full system would do differently)
//   - Tie each reservation to an order id (a "reservations" table). Today
//     reserve/commit/release only move numbers, so repeating a call moves the
//     numbers again, and if Order times out it cannot know whether a hold
//     exists. With reservation records, every call can be safely repeated and
//     held stock can expire automatically if an order is abandoned.
//   - Create the tables with migration files; today they are made by hand.
//   - Validate input types properly (a schema library) and return a list for
//     "no items" instead of 404.
//   - Add paging to the list endpoint, and a stock-history table recording
//     every change (who, when, why).
//   - Read the port from the PORT setting, and add a health-check endpoint.
//   - Keep prices as whole cents in a separate pricing service or table.
// =============================================================================

// Load the settings from the .env file first; the modules below read them
// when they load.
require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { requireRole } = require('./auth');   // keep this after dotenv.config()

// Roles that may read stock levels (any valid token works).
const READERS = ['customer', 'admin', 'service'];

const app = express();
// Lets Express read JSON request bodies.
app.use(express.json());

// A pool keeps a few database connections open and reuses them.
const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

// ---- Phase 1: reserve (available -> held) -----------------------------------
app.post('/inventory/reserve', requireRole('service'), async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  try {
    // Atomic: the check and the move happen in one statement. It only changes
    // the row if enough units are available, and the answer ("did it work?")
    // is simply whether a row came back.
    const result = await pool.query(
      `UPDATE inventory
         SET quantity = quantity - $1,
             reserved = reserved + $1
       WHERE sku = $2 AND quantity >= $1
       RETURNING *`,
      [quantity, sku]
    );

    // Success. The row includes the price, which Order uses to compute the
    // amount, so the customer cannot choose their own price.
    if (result.rows.length > 0) return res.status(200).json(result.rows[0]);

    // Zero rows: diagnose only, after the atomic decision was made
    const check = await pool.query('SELECT 1 FROM inventory WHERE sku = $1', [sku]);
    if (check.rows.length === 0) return res.status(404).json({ error: 'SKU not found' });
    return res.status(409).json({ error: 'Insufficient stock' });
  } catch (err) {
    console.error(err);
    return res.status(503).json({ error: 'Service Unavailable' });
  }
});

// ---- Phase 2a: commit (held -> gone). Called after the payment succeeded. ----
app.post('/inventory/commit', requireRole('service'), async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  try {
    // Finalize: reserved units leave the system; guard prevents committing more than is held
    const result = await pool.query(
      `UPDATE inventory
         SET reserved = reserved - $1
       WHERE sku = $2 AND reserved >= $1
       RETURNING *`,
      [quantity, sku]
    );

    if (result.rows.length > 0) return res.status(200).json(result.rows[0]);

    const check = await pool.query('SELECT 1 FROM inventory WHERE sku = $1', [sku]);
    if (check.rows.length === 0) return res.status(404).json({ error: 'SKU not found' });
    return res.status(409).json({ error: 'Not enough reserved stock to commit' });
  } catch (err) {
    console.error(err);
    return res.status(503).json({ error: 'Service Unavailable' });
  }
});

// ---- Reading stock levels ---------------------------------------------------
// Any logged-in caller may read. Without a token the answer is 401, so the
// catalogue is not public. PRODUCTION: a public product catalogue would be a
// separate, cached, read-only endpoint; this one would stay internal.
app.get('/inventory', requireRole(...READERS), async (req, res) => {
  const result = await pool.query('SELECT * FROM inventory');
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows);
});

app.get('/inventory/:sku', requireRole(...READERS), async (req, res) => {
  const result = await pool.query('SELECT * FROM inventory WHERE sku = $1', [req.params.sku]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows[0]);
});

// ---- Creating a new item (admin only) ---------------------------------------
// Before the JWT work, anyone could call this. Now only the admin role can.
app.post('/inventory', requireRole('admin'), async (req, res) => {
  const { sku, quantity, price } = req.body;

  // Zero is allowed for quantity and price (an item may be out of stock or
  // free), which is why this checks for null/negative instead of "falsy".
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (quantity == null || quantity < 0) return res.status(400).json({ error: 'quantity must be a non-negative number' });
  if (price == null || price < 0) return res.status(400).json({ error: 'price must be a non-negative number' });

  try {
    const result = await pool.query(
      'INSERT INTO inventory (sku, quantity, price) VALUES ($1, $2, $3) RETURNING *',
      [sku, quantity, price]
    );
    res.status(201).json(result.rows[0]);
  } catch (err) {
    // The database itself enforces "one row per sku"; we translate its
    // complaint into a clear 409.
    if (err.code === '23505') { // unique_violation, since sku is UNIQUE
      return res.status(409).json({ error: 'SKU already exists' });
    }
    console.error(err);
    res.status(500).json({ error: 'Failed to insert item' });
  }
});

// ---- Phase 2b: release (held -> available). The compensation step. ----------
// Called when the payment was declined or something failed before payment was
// taken. It is NEVER called after a successful payment (see index.js in the
// Order service): the customer has paid, so the stock stays sold.
app.post('/inventory/release', requireRole('service'), async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  try {
    // Compensation: return held units to available stock.
    // The guard prevents releasing more than is currently reserved.
    const result = await pool.query(
      `UPDATE inventory
         SET quantity = quantity + $1,
             reserved = reserved - $1
       WHERE sku = $2 AND reserved >= $1
       RETURNING *`,
      [quantity, sku]
    );

    if (result.rows.length > 0) return res.status(200).json(result.rows[0]);

    const check = await pool.query('SELECT 1 FROM inventory WHERE sku = $1', [sku]);
    if (check.rows.length === 0) return res.status(404).json({ error: 'SKU not found' });
    return res.status(409).json({ error: 'Not enough reserved stock to release' });
  } catch (err) {
    console.error(err);
    return res.status(503).json({ error: 'Service Unavailable' });
  }
});

// PRODUCTION: read this from process.env.PORT (it is fixed for the demo).
const PORT = 4002;
app.listen(PORT, () => console.log(`Inventory service on port ${PORT}`));
