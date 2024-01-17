const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { Pool } = require("pg");

let alloyDBClient;
(async () => {
  const getSSLCertificate = async () => {
    const client = new SecretManagerServiceClient();

    const [version] = await client.accessSecretVersion({
      name: `projects/${process.env.PROJECT_ID}/secrets/${process.env.ALLOY_DB_ROOT_CERT_SECRET_NAME}/versions/latest`,
    });

    const payload = version.payload.data.toString("utf8");
    return payload;
  };

  const sslCertificate = await getSSLCertificate();

  const pool = new Pool({
    user: process.env.ALLOY_DB_USER,
    host: process.env.ALLOY_DB_HOST,
    database: process.env.ALLOY_DB_DBNAME,
    password: process.env.ALLOY_DB_PASSWORD,
    port: process.env.ALLOY_DB_PORT || 5432,
    ssl: {
      ca: sslCertificate,
      rejectUnauthorized: true,
      checkServerIdentity: () => null,
    },
  });

  alloyDBClient = {
    pool,
    query: async (text, params) => {
      const client = await pool.connect();

      try {
        const res = await client.query(text, params);
        return res;
      } finally {
        client.release();
      }
    },
  };
  await alloyDBClient.query(`SELECT 1;`);
})();

async function handleMatchingEmail(matterId) {
  let visit_emails = [],
    flag = true;
  while (flag) {
    const vector_infos = await alloyDBClient.query(
      `
        SELECT 1 
        FROM matterVectors 
        WHERE matter_id = $1;
      `,
      [matterId]
    );
    if (!vector_infos.rowCount) {
      await alloyDBClient.query(
        `
          INSERT INTO matterVectors(matter_id, matter_vector)
          VALUES ($1, (SELECT matter_vector from matters where id = $1));
        `,
        [matterId]
      );
    }
    const email_list = await alloyDBClient.query(
      `
        select uuid as email_id
        from emails
        where emails.email_category = 'Legal'
          and user_id in
              (select uuid from users where organization_id = (select organization_id from matters where id = $1))
          and cosine_distance(email_vector, (select matter_vector from matters where id=$1)) <= 0.1
        order by date;
      `,
      [matterId]
    );
    const emails = await alloyDBClient.query(
      "SELECT email_id from test_time_entries;"
    );
    let vis = [];
    if (!email_list.rowCount) flag = false;
    for (let val of email_list.rows) {
      if (visit_emails[val.email_id] == 1) {
        flag = false;
        break;
      }
      let scores = await alloyDBClient.query(
        `
          SELECT (1 - cosine_distance(email_vector, (select matter_vector from matterVectors where matter_id = $1))) * 100 as score
          FROM emails
          WHERE uuid=$2;
        `,
        [matterId, val.email_id]
      );
      await alloyDBClient.query(
        `
          INSERT INTO test_time_entries(matter_id, email_id, score)
          VALUES ($1, $2, $3)
          ON CONFLICT(matter_id, email_id)
          DO UPDATE SET email_id = EXCLUDED.email_id, score = EXCLUDED.score;
        `,
        [matterId, val.email_id, scores.rows[0].score]
      );
      vis[val.email_id] = 1;
      visit_emails[val.email_id] = 1;
      await alloyDBClient.query(
        `
          UPDATE matterVectors SET matter_vector = CONCAT(matter_vector, (select email_vector from emails where uuid=$1));
        `,
        [val.email_id]
      );
    }
    for (let dval of emails.rows) {
      if (vis[dval.email_id] != 1 && flag) {
        await alloyDBClient.query(
          `
            DELETE from test_time_entries where matter_id = $1 and email_id = $2;
          `,
          [matterId, dval.email_id]
        );
      }
    }
  }
  const bestscore = await alloyDBClient.query(
    `
      SELECT MAX(score) as best_score, email_id from test_time_entries WHERE matter_id = $1;
    `,
    [matterId]
  );
  console.log("bestscore: ", bestscore);
  await alloyDBClient.query(
    `
      INSERT INTO confidence_score(matter_id, email_id, score) VALUES ($1,$2,$3)
      ON CONFLICT (email_id)
      DO UPDATE SET matter_id=$1, score=$3;    
    `,
    [matterId, bestscore.rows[0].email_id, bestscore.rows[0].best_score]
  );
}

module.exports.handleBatchEmail = async () => {
  const matterlist = await alloyDBClient.query("SELECT id FROM matters;");
  for (let val of matterlist.rows) {
    await handleMatchingEmail(val.id);
  }
};
// This function returns the ID from the matters table that best matches the provided email ID.
module.exports.getMatchingMatter = async (body) => {
  try {
    const { emailId } = body;
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
// This function is invoked when the matters table is updated, and it regenerates the embeddings for matters.
module.exports.handleUpdateMatters = async (body) => {
  try {
    const data = body.event.data.new;
    // matter_name, description, identifying_items_in_email, key_issues_in_matter, associated_emails, location
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
    await handleMatchingEmail(data.id);
    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// This function is invoked when a new element is inserted into the matters table, it generates embedding for matters.
module.exports.handleInsertMatters = async (body) => {
  try {
    const data = body.event.data.new;
    // matter_name, description, identifying_items_in_email, key_issues_in_matter, associated_emails, location
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
    await handleMatchingEmail(data.id);
    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// This function invoked when a element is deleted in the matters table.
module.exports.handleDeleteMatters = async (body) => {
  try {
    const data = body.event.data.old;
    await alloyDBClient.query(
      `
        DELETE from test_time_entries where matter_id = $1;
      `,
      [data.id]
    );
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// Emails
// This function is invoked when emails table is updated, and it regenerates the embedding for emails.
module.exports.handleUpdateEmails = async (body) => {
  try {
    const data = body.event.data.new;
    // sender, email_id, recipient, subject, body, reasoning, date
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
    // sender, email_id, recipient
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

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
// This function is invoked when a new element is inserted into emails table, it generates embedding for emails.
module.exports.handleInsertEmails = async (body) => {
  try {
    const data = body.event.data.new;
    // sender, email_id, recipient, subject, body, reasoning, date
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
    // sender, email_id, Recipient
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
    if (data.email_category == "Legal") {
      await handleBatchEmail();
    }
    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// Contacts
// This function is invoked when contacts table is update, it regenerates the embedding for contacts.
module.exports.handleUpdateContacts = async (body) => {
  try {
    const data = body.event.data.new;
    // name, email, primary_email_address, secondary_email_address
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

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// This fucntion is invoked when a new data is inserted into contacts table, it generates embedding for contacts.
module.exports.handleInsertContacts = async (body) => {
  try {
    const data = body.event.data.new;
    // name, email, primary_email_address, secondary_email_address
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

    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
