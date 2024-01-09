const { SecretManagerServiceClient } = require("@google-cloud/secret-manager");
const { Pool } = require("pg");

let sslCertificate;

const getSSLCertificate = async () => {
  const client = new SecretManagerServiceClient();

  const [version] = await client.accessSecretVersion({
    name: `projects/${process.env.projectId}/secrets/${process.env.secretId}/versions/latest`,
  });

  const payload = version.payload.data.toString("utf8");
  return payload;
};

await getSSLCertificate().then((result) => (sslCertificate = result));

const pool = new Pool({
  user: process.env.ALLOY_DB_USER,
  host: process.env.ALLOY_DB_HOST,
  database: process.env.ALLOY_DB_DBNAME,
  password: process.env.ALLOY_DB_PASSWORD,
  port: process.env.ALLOY_DB_PORT || 5432,
  ssl: {
    ca: sslCertificate,
    rejectUnauthorized: true,
    checkServerIdentity: () => {
      return null;
    },
  },
});

const getAlloyDBClient = async () => {
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

module.exports.getMatchingMatter = async (body) => {
  try {
    const { emailId } = body;
    const alloyDBClient = await getAlloyDBClient();
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
      return { msg: "Invalid Email!", rlt: null };
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
      return { msg: "There is no associated matter", rlt: null };
    }
    return { msg: "ok", rlt: matterId.rows };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
// Matters
module.exports.handleUpdateMatters = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = await getAlloyDBClient();
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
    const c_matters = await alloyDBClient.query(
      `
        SELECT uuid as email_id,
        (1 - (email_vector <=> (SELECT matter_vector
                              FROM matters
                              WHERE id = $1
                              LIMIT 1))) * 100 as score
        FROM emails
        WHERE email_contact_vector <-> (SELECT contact_vector
                               FROM contacts
                               WHERE matter_id = $1
                                 AND organization_id = (SELECT organization_id
                                                        FROM matters
                                                        WHERE id = $1
                                                        LIMIT 1)
                               LIMIT 1) < 1 AND emails.email_category='Legal'
        ORDER BY score DESC;      
      `,
      [data.id]
    );
    if (c_matters.rowCount) {
      for (let val of c_matters.rows) {
        let k_val = await alloyDBClient.query(
          `SELECT score from confidence_score where email_id=$1;`,
          [val.email_id]
        );
        if (k_val.rows.length > 0 && k_val.rows[0].score < val.score) {
          await alloyDBClient.query(
            `UPDATE confidence_score set matter_id=$1, score=$2 where email_id=$3`,
            [data.id, val.score, val.email_id]
          );
        }
      }
    }

    const d_contacts = await alloyDBClient.query(
      `
        SELECT contact_vector from contacts where matter_id=$1;
      `,
      [data.id]
    );
    for (let val of d_contacts.rows) {
      const choose_email = await alloyDBClient.query(
        `SELECT $1 <-> email_contact_vector
        from emails
        where user_id = (select uuid from users where organization_id=(select organization_id from contacts where matter_id=$2))
        ORDER BY score
        LIMIT 1;
        `,
        [val.contact_vector, data.id]
      );
    }

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
    const alloyDBClient = await getAlloyDBClient();
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
    const c_matters = await alloyDBClient.query(
      `
        SELECT uuid as email_id,
        (1 - (email_vector <=> (SELECT matter_vector
                              FROM matters
                              WHERE id = $1
                              LIMIT 1))) * 100 as score
        FROM emails
        WHERE email_contact_vector <-> (SELECT contact_vector
                               FROM contacts
                               WHERE matter_id = $1
                                 AND organization_id = (SELECT organization_id
                                                        FROM matters
                                                        WHERE id = $1
                                                        LIMIT 1)
                               LIMIT 1) < 1 AND emails.email_category='Legal'
        ORDER BY score DESC;      
      `,
      [data.id]
    );
    if (c_matters.rowCount) {
      for (let val of c_matters) {
        let k_val = await alloyDBClient.query(
          `SELECT score from confidence_score where email_id=$1;`,
          [val.email_id]
        );
        if (k_val.rows.length > 0 && k_val.rows[0].score < val.score) {
          await alloyDBClient.query(
            `UPDATE confidence_score set matter_id=$1, score=$2 where email_id=$3`,
            [data.id, val.score, val.email_id]
          );
        }
      }
    }
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
    const alloyDBClient = await getAlloyDBClient();
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
    const matchingMatterId = await alloyDBClient.query(
      `
        SELECT id
        FROM matters
        WHERE organization_id = (SELECT organization_id
                                FROM users
                                WHERE uuid = (SELECT emails.user_id
                                              FROM emails
                                              WHERE emails.uuid = $1 
                                                and emails.email_category = 'Legal'))
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
      [data.uuid]
    );
    if (matchingMatterId.rowCount) {
      const score = await alloyDBClient.query(
        `
          SELECT (1 - (email_vector <=> matter_vector)) * 100 as score
          FROM matters,
              emails
          WHERE emails.uuid = $1
            and matters.id = $2;
        `,
        [data.uuid, matchingMatterId.rows[0].id]
      );
      await alloyDBClient.query(
        `
          UPDATE confidence_score
          set matter_id=$1,
              score=$2
          WHERE email_id = $3;
        `,
        [matchingMatterId.rows[0].id, score.rows[0].score, data.uuid]
      );
    }
    await alloyDBClient.endPool();

    return { status: "ok", op: "update" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertEmails = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = await getAlloyDBClient();
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
    const matchingMatterId = await alloyDBClient.query(
      `
        SELECT id
        FROM matters
        WHERE organization_id = (SELECT organization_id
                                FROM users
                                WHERE uuid = (SELECT emails.user_id
                                              FROM emails
                                              WHERE emails.uuid = $1 
                                                and emails.email_category = 'Legal'))
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
      [data.uuid]
    );
    if (matchingMatterId.rowCount) {
      const score = await alloyDBClient.query(
        `
          SELECT (1 - (email_vector <=> matter_vector)) * 100 as score
          FROM matters,
              emails
          WHERE emails.uuid = $1
            and matters.id = $2;
        `,
        [data.uuid, matchingMatterId.rows[0].id]
      );
      await alloyDBClient.query(
        `
          INSERT INTO confidence_score(email_id, matter_id, score)
          values($1, $2, $3);        
        `,
        [data.uuid, matchingMatterId.rows[0].id, score.rows[0].score]
      );
    }
    await alloyDBClient.endPool();

    return { status: "ok", op: "insert" };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

//Contacts

module.exports.handleUpdateContacts = async (body) => {
  try {
    const data = body.event.data.new;
    const alloyDBClient = await getAlloyDBClient();
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
    const alloyDBClient = await getAlloyDBClient();
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
