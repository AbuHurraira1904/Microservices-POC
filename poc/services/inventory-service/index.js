require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const { requireRole } = require('./auth');   // keep this after dotenv.config()

// Who may call what:
//   service  = the Order service (reserve / commit / release)
//   admin    = a person managing stock (create items)
//   any valid token (customer, admin, service) may read stock levels
const READERS = ['customer', 'admin', 'service'];

const app = express();
app.use(express.json());

const pool = new Pool({
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
});

app.post('/inventory/reserve', requireRole('service'), async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive number' });

  try {
    // Atomic: the check and the move happen in one statement
    const result = await pool.query(
      `UPDATE inventory
         SET quantity = quantity - $1,
             reserved = reserved + $1
       WHERE sku = $2 AND quantity >= $1
       RETURNING *`,
      [quantity, sku]
    );

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

app.post('/inventory', requireRole('admin'), async (req, res) => {
  const { sku, quantity, price } = req.body;

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
    if (err.code === '23505') { // unique_violation, since sku is UNIQUE
      return res.status(409).json({ error: 'SKU already exists' });
    }
    console.error(err);
    res.status(500).json({ error: 'Failed to insert item' });
  }
});

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

const PORT = 4002;
app.listen(PORT, () => console.log(`Inventory service on port ${PORT}`));