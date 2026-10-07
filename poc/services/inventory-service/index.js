require('dotenv').config();
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

app.post('/inventory/reserve', async (req, res) => {
  const { sku, quantity } = req.body;

  const result = await pool.query(
    'UPDATE inventory SET quantity = quantity - $1 WHERE sku = $2 AND quantity >= $1 RETURNING *',
    [quantity, sku]
  );

  if (result.rows.length > 0) {
    return res.status(200).json(result.rows[0]); // success
  }

  //Zero rows... Now to check why?
  const check = await pool.query('SELECT * FROM inventory WHERE sku = $1', [sku]);

  if (check.rows.length === 0) {
    return res.status(404).json({ error: 'SKU not found' });
  }

  return res.status(409).json({ error: 'Insufficient stock' });
});

app.get('/inventory', async (req, res) => {
  const result = await pool.query('SELECT * FROM inventory');
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows);
});

app.get('/inventory/:sku', async (req, res) => {
  const result = await pool.query('SELECT * FROM inventory WHERE sku = $1', [req.params.sku]);
  if (result.rows.length === 0) return res.status(404).json({ error: 'not found' });
  res.json(result.rows[0]);
});

app.post('/inventory', async (req, res) => {
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

const PORT = 4002;
app.listen(PORT, () => console.log(`Inventory service on port ${PORT}`));