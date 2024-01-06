const { handleInsertEmails, handleUpdateEmails } = require("./postgres");

const express = require("express");

const app = express();

app.post("/handleInsertEmails", async (req, res) => {
  res.status(200).send(await handleInsertEmails(req.body));
});

app.post("/handleUpdateEmails", async (req, res) => {
  res.status(200).send(await handleUpdateEmails(req.body));
});

app.listen(3000, () => {
  console.log("Server running on 3000");
});
