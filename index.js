require("dotenv").config();
const {
  handleInsertEmails,
  handleUpdateEmails,
  handleUpdateMatters,
  handleInsertMatters,
  handleInsertContacts,
  handleUpdateContacts,
  handleDeleteEmails,
  handleDeleteMatters,
  handleBatchEmail,
} = require("./postgres.js");
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

app.post("/handleDeleteEmails", async (req, res) => {
  res.status(200).send(await handleDeleteEmails(req.body));
});

app.post("/handleInsertMatters", async (req, res) => {
  res.status(200).send(await handleInsertMatters(req.body));
});

app.post("/handleUpdateMatters", async (req, res) => {
  res.status(200).send(await handleUpdateMatters(req.body));
});

app.post("/handleDeleteMatters", async (req, res) => {
  res.status(200).send(await handleDeleteMatters(req.body));
});

app.post("/handleInsertContacts", async (req, res) => {
  res.status(200).send(await handleInsertContacts(req.body));
});

app.post("/handleUpdateContacts", async (req, res) => {
  res.status(200).send(await handleUpdateContacts(req.body));
});

app.post("/handleBatchEmail", async (_, res) => {
  res.status(200).send(await handleBatchEmail());
});
app.listen(3000, () => {
  console.log("Server running on 3000");
});
