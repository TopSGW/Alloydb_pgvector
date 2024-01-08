const { Pool } = require("pg");

const getAlloyDBClient = () => {
  const pool = new Pool({
    user: process.env.ALLOY_DB_USER,
    host: process.env.ALLOY_DB_HOST,
    database: process.env.ALLOY_DB_DBNAME,
    password: process.env.ALLOY_DB_PASSWORD,
    port: process.env.ALLOY_DB_PORT || 5432,
  });
  pool.on("error", (err, client) => {
    console.error("Unexpected error on idle client", err);
  });

  const connection = {
    pool,
    endPool: async () => {
      await pool.end();
    },
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

module.exports.getMatchingMatter = async (body) => {
  try {
    const { emailId } = body;
    const alloyDBClient = getAlloyDBClient();
    const orgId = await alloyDBClient.query(
      `
        SELECT organization_id
        FROM users
        WHERE uuid = (SELECT emails.user_id
                      FROM emails
                      WHERE emails.uuid = $1 AND emails.email_category = 'Legal');
      `,
      [emailId]
    );

    if (!orgId.rowCount) {
      return { msg: "Invalid Email!" };
    }

    const matterId = await alloyDBClient.query(
      `
        SELECT id
        FROM matters
        WHERE organization_id = $2
          AND id IN
              (SELECT matter_id
              FROM contacts
              WHERE contact_vector <->
                    (SELECT email_contact_vector
                      FROM emails
                      WHERE emails.uuid = $1) < 1)
        ORDER BY matter_vector <->
                (SELECT email_vector
                  FROM emails
                  WHERE emails.uuid = $1)
        LIMIT 1;
      `,
      [emailId, orgId.rows[0].organization_id]
    );

    await alloyDBClient.endPool();

    if (!matterId.rowCount) {
      return { msg: "There is no associated matter" };
    }
    return matterId.rows;
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// Matters

module.exports.handleUpdateMatters = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `
        UPDATE matters
        SET matter_vector = embedding('textembedding-gecko@003', 
                              CONCAT(
                                E'Matter info\n\n', 'Matter name: ', matter_name, 
                                E'\n', E'Matter description: ', description, 
                                E'\n', E'Identifying items in email: ', identifying_items_in_email,
                                E'\n', E'Key issues in matter: ', key_issues_in_matter, 
                                E'\n', E'Associated_emails: ', associated_emails, 
                                E'\n', E'location : ', location, 
                                E'\n'
                              )
                            )
        WHERE id = $1;
      `,
      [data.id]
    );
    await alloyDBClient.endPool();

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertMatters = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `
        UPDATE matters
        SET matter_vector=embedding('textembedding-gecko@003',
                            CONCAT(
                              E'Matter info\n\n', 'Matter name: ', matter_name, 
                              E'\n', E'Matter description: ', description, 
                              E'\n', E'Identifying items in email: ', identifying_items_in_email,
                              E'\n', E'Key issues in matter: ', key_issues_in_matter, 
                              E'\n', E'Associated_emails: ', associated_emails, 
                              E'\n', E'location : ', location,
                              E'\n'
                            )
                          )
        WHERE id = $1;
      `,
      [data.id]
    );
    await alloyDBClient.endPool();

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
      `
        UPDATE emails
        SET email_vector = embedding('textembedding-gecko@003',
                            CONCAT(E'Email info \n\n', 'sender: ', sender, 
                              E'\n', 'email_id: ', email_id, 
                              E'\n', 'recipient: ', recipient, 
                              E'\n', 'Subject: ', subject, 
                              E'\n', 'Body: ', body,
                              E'\n', 'reasonning: ', reasoning, 
                              E'\n', E'date ', date
                            )
                          )
        WHERE uuid = $1;
      `,
      [data.uuid]
    );
    await alloyDBClient.endPool();

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertEmails = async (body) => {
  // try {
  const data = body.event.data.new;
  console.log(`uuid: ${data.uuid}`);
  const alloyDBClient = getAlloyDBClient();
  console.log("alloyDBClient", alloyDBClient);
  await alloyDBClient.query(
    `
        UPDATE emails
        SET email_vector = embedding('textembedding-gecko@003',
                            CONCAT(E'Email info \n\n', 'sender: ', sender, 
                              E'\n', 'email_id: ', email_id, 
                              E'\n', 'recipient: ', recipient, 
                              E'\n', 'Subject: ', subject, 
                              E'\n', 'Body: ', body,
                              E'\n', 'reasonning: ', reasoning, 
                              E'\n', 'date ', date
                            )
                          )
        WHERE uuid = $1;
      `,
    [data.uuid]
  );
  console.log("data.uuid", data.uuid);
  await alloyDBClient.query(
    `
        UPDATE emails
        SET email_contact_vector = embedding('textembedding-gecko@003',
                                    CONCAT(E'Email info\n\n', 'Sender: ', sender, 
                                        E'\n', 'email_id: ', email_id, 
                                        E'\n', 'Recipient: ', recipient, 
                                        E'\n'
                                    )
                                  )
        WHERE uuid = $1;
      `,
    [data.uuid]
  );

  console.log("data.uuid1", data.uuid);
  await alloyDBClient.endPool();

  return { status: "ok", op: "insert" };
  // } catch (error) {
  //   console.log(error);
  //   throw new Error(error);
  // }
};

//Contacts

module.exports.handleUpdateContacts = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `
        UPDATE contacts
        SET contact_vector=embedding('textembedding-gecko@003',
                                    CONCAT(E'Contact info\n\n', 'Name: ', name, 
                                      E'\n', 'Email: ', email, 
                                      E'\n', 'Primary email address: ', primary_email_address, 
                                      E'\n', 'Secondary email address: ', secondary_email_address
                                    )
                                  )
        WHERE id = $1;
      `,
      [data.id]
    );
    await alloyDBClient.endPool();

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertContacts = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = getAlloyDBClient();
    await alloyDBClient.query(
      `
        UPDATE contacts
        SET contact_vector=embedding('textembedding-gecko@003',
                                      CONCAT(E'Contact info\n\n', 'Name: ', name, 
                                        E'\n', 'Email: ', email, 
                                        E'\n', 'Primary email address: ', primary_email_address, 
                                        E'\n', 'Secondary email address: ', secondary_email_address
                                      )
                                    )
        WHERE id = $1;
      `,
      [data.id]
    );
    await alloyDBClient.endPool();

    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
