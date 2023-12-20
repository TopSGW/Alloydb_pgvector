const rows = await process.postgresql.query('SELECT id FROM matters');
console.log(rows);