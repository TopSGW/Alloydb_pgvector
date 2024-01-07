const { Pool } = require("pg");

const getAlloyDBClient = () => {
  const pool = new Pool({
    user: process.env.ALLOY_DB_USER,
    host: process.env.ALLOY_DB_HOST,
    database: process.env.ALLOY_DB_DBNAME,
    password: process.env.ALLOY_DB_PASSWORD,
    port: process.env.ALLOY_DB_PORT || 5432,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
    keepAlive: true,
  });
  console.log("check AlloyDB host");
  console.log(process.env.ALLOY_DB_HOST);
  const connection = {
    pool,
    query: async (text, params) => {
      const client = await pool.connect();

      try {
        const res = await client.query(text, params);
        return res;
      } catch (err) {
        throw err;
      } finally {
        client.release();
      }
    },
  };

  return connection;
};

module.exports.handleInsertEmails = async (body) => {
  try {
    console.log(body);
    const data = body.event.data.new;
    console.log(`uuid: ${data.uuid}`);
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `UPDATE emails
      SET email_vector = embedding('textembedding-gecko@003',
                                   CONCAT(E'Email info \n\n', E'sender: ', sender, E'\n', E'email_id: ', email_id, E'\n',
                                          'recipient: ', recipient, E'\n', E'Subject: ', subject, E'\n', E'Body: ', body,
                                          E'\n', E'reasonning: ', reasoning, E'\n', E'date ', date))
      WHERE uuid = $1;`,
      [data.uuid]
    );
    await alloyDBClient.query(
      `UPDATE emails
      SET email_contact_vector = embedding('textembedding-gecko@003',
                                           CONCAT(E'Email info\n\n', 'Sender: ', sender, E'\n', 'email_id: ', email_id, E'\n',
                                                  'Recipient: ', recipient, E'\n'))
      WHERE uuid = $1;`,
      [data.uuid]
    );
    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// Emails

module.exports.handleUpdateEmails = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `UPDATE emails
      SET email_vector = embedding('textembedding-gecko@003',
                                   CONCAT(E'Email info \n\n', E'sender: ', sender, E'\n', E'email_id: ', email_id, E'\n',
                                          'recipient: ', recipient, E'\n', E'Subject: ', subject, E'\n', E'Body: ', body,
                                          E'\n', E'reasonning: ', reasoning, E'\n', E'date ', date))
      WHERE uuid = $1;`,
      [data.uuid]
    );
    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
