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

// // PROBLEM: RACE CONDITION
// // Create a new order
// app.post('/orders', async (req, res) => {
//   const { amount, sku, quantity } = req.body;
//   if (!amount) return res.status(400).json({ error: 'amount is required' });
//   if (!sku) return res.status(400).json({ error: 'sku is required' });
//   if (!quantity) return res.status(400).json({ error: 'quantity is required' });

//   try {
//     //call inventory service to get the inventory for the sku and check if the quantity is available
//     const inventoryResponse = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/${sku}`)

//     if (inventoryResponse.status === 404) {
//       return res.status(404).json({ error: 'SKU not found in inventory' });
//     }

//     if (!inventoryResponse.ok) {
//       return res.status(inventoryResponse.status).json({ error: 'Inventory service error' });
//     }

//     const inventoryData = await inventoryResponse.json();
//     if (inventoryData.quantity < quantity) {
//       return res.status(400).json({ error: 'Insufficient inventory' });
//     }
    
//     const result = await pool.query(
//     'INSERT INTO orders (amount, sku, quantity) VALUES ($1, $2, $3) RETURNING *',
//     [amount, sku, quantity]
//     );

//   res.status(201).json(result.rows[0]);
//   }
//   catch (err) {
//     console.error(err);
//     res.status(503).json({ error: 'Service Unavailable' });
//     return;
//   }
// });

// Create a new order
app.post('/orders', async (req, res) => {
  const { sku, quantity } = req.body;
  if (!sku) return res.status(400).json({ error: 'sku is required' });
  if (!quantity) return res.status(400).json({ error: 'quantity is required' });

  try {
    const inventoryResponse = await fetch(`${process.env.INVENTORY_SERVICE_URL}/inventory/reserve`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sku, quantity }),
    });

    if (!inventoryResponse.ok) {
      const errorData = await inventoryResponse.json();
      return res.status(inventoryResponse.status).json({
        error: errorData.error || 'Failed to update inventory'
      });
    }

    const inventoryResponseData = await inventoryResponse.json();
    amount = inventoryResponseData.price * quantity;
    
    const orderResponseData = await pool.query(
    'INSERT INTO orders (amount, sku, quantity) VALUES ($1, $2, $3) RETURNING *',
    [amount, sku, quantity]
    );

    res.status(201).json({
      message: 'Order placed and stock updated. Price of the order is $' + amount,
      order: orderResponseData.rows[0],
    });
  }
  catch (err) {
    console.error(err);
    res.status(503).json({ error: 'Service Unavailable' });
    return;
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
app.listen(PORT, () => console.log(`Order service on port ${PORT}`));