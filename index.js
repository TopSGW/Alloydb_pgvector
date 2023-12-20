const express = require('express')
const postgresql = require('./postgres.js')

postgresql()

const app = express();

app.post('/get_matterId', async (req, res) => {
  const rows = await process.postgresql.query('SELECT id FROM matters');
  res.status(200).send(JSON.stringify(rows));
});

app.listen(3000, () => {
  console.log('Server running on 3000');
});
