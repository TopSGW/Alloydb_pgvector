const express = require('express')
const postgreConnector = require('./postgres.js')

postgreConnector()

const app = express();

app.get('/get_matterId', async (req, res) => {
  const rows = await process.postgresql.query('SELECT id FROM matters');
  res.status(200).send(JSON.stringify(rows));
});

app.listen(3000, () => {
  console.log('Server running on 3000');
});
