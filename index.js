const { handleInsertEmails, handleUpdateEmails } = require("./postgres");
require("dotenv").config();
const express = require("express");

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.post("/handleInsertEmails", async (req, res) => {
  console.log(req.body);
  res.status(200).send(await handleInsertEmails(req.body));
});

app.post("/handleUpdateEmails", async (req, res) => {
  res.status(200).send(await handleUpdateEmails(req.body));
});

app.listen(3000, () => {
  console.log("Server running on 3000");
});
