const { SecretManagerServiceClient } = require('@google-cloud/secret-manager');
const { Pool } = require('pg');
const fs = require('fs');

let alloyDBClient;
(async () => {
  const getSSLCertificate = async () => {
    const client = new SecretManagerServiceClient();

    const [version] = await client.accessSecretVersion({
      name: `projects/${process.env.projectId}/secrets/${process.env.secretId}/versions/latest`,
    });

    const payload = version.payload.data.toString('utf8');
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
      checkServerIdentity: () => {
        return null;
      },
    },
  });

  pool.on('error', (err, client) => {
    console.error('Unexpected error on idle client', err);
  });

  alloyDBClient = {
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
  await alloyDBClient.query(`SELECT 1;`);
})();

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
      return { msg: 'Invalid Email!', rlt: null };
    }
    //    const userName = `${orgId.rows[0].firstname} ${orgId.rows[0].last_name}`;
    const matterId = await alloyDBClient.query(
      `
        SELECT id
        FROM matters
        WHERE organization_id = $2
          AND id IN
              (SELECT matter_id
              FROM contacts
              WHERE contact_vector <=>
                    (SELECT email_contact_vector
                      FROM emails
                      WHERE emails.uuid = $1) < 0.3)
        ORDER BY matter_vector <->
                (SELECT email_vector
                  FROM emails
                  WHERE emails.uuid = $1)
        LIMIT 1;
      `,
      [emailId, orgId.rows[0].organization_id, userName]
    );

    if (!matterId.rowCount) {
      return { msg: 'There is no associated matter', rlt: null };
    }
    return { msg: 'ok', rlt: matterId.rows };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

async function handleMatchingEmail(matterId) {
  let visit_emails = [];
  let flag = true;
  while (flag) {
    // const vector_infos = await alloyDBClient.query(
    //   `
    //     SELECT 1
    //     FROM matterVectors
    //     WHERE matter_id = $1;
    //   `,
    //   [matterId]
    // );
    // if (!vector_infos.rowCount) {
    //   await alloyDBClient.query(
    //     `
    //       INSERT INTO matterVectors(matter_id, matter_vector)
    //       VALUES ($1, (SELECT matter_vector from matters where id = $1));
    //     `,
    //     [matterId]
    //   );
    // }
    const email_list = await alloyDBClient.query(
      `
        SELECT uuid as email_id
        FROM emails
        WHERE emails.email_category = 'Legal'
          and user_id in
              (select uuid from users where organization_id = (select organization_id from matters where id = $1))
          and cosine_distance(email_vector, (select matter_vector from matters where id=$1)) <= 0.1
        order by date;
      `,
      [matterId]
    );
    const emails = await alloyDBClient.query(
      'SELECT email_id from test_time_entries;'
    );
    let vis = [];
    if (!email_list.rowCount) {
      await alloyDBClient.query(
        `
          UPDATE matterVectors
          SET matter_vector=(SELECT matter_vector FROM matters WHERE id = $1)
          where matter_id = $1;
        `,
        [matterId]
      );
      flag = false;
    }

    email_list.rows.map((email) => {});
    for (let val of email_list.rows) {
      // if (visit_emails[val.email_id] == 1) {
      //   flag = false;
      //   await alloyDBClient.query(
      //     `
      //       UPDATE matterVectors
      //       SET matter_vector=(SELECT matter_vector FROM matters WHERE id = $1)
      //       where matter_id = $1;
      //     `,
      //     [matterId]
      //   );
      //   break;
      // }
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
  const scorelist = await alloyDBClient.query(
    `
      SELECT score, email_id from test_time_entries WHERE matter_id = $1;
    `,
    [matterId]
  );
  for (let val of scorelist.rows) {
    let confidence_score = await alloyDBClient.query(
      `
        SELECT score FROM confidence_score WHERE email_id = $1;
      `,
      [val.email_id]
    );
    if (!confidence_score.rowCount) {
      await alloyDBClient.query(
        `
          INSERT INTO confidence_score(email_id, matter_id, score)
          VALUES ($1,$2,$3) 
        `,
        [val.email_id, matterId, val.score]
      );
    } else if (confidence_score.rows[0].score < val.score) {
      await alloyDBClient.query(
        `
          UPDATE confidence_score SET matter_id = $1, score = $2 WHERE email_id = $3
        `,
        [matterId, val.score, val.email_id]
      );
    }
  }
}

async function test() {
  const scoreLimit = 90;
  const result = await alloyDBClient.query(
    `
    SELECT *
    FROM matters
    LEFT JOIN confidence_score on matters.id = confidence_score.matter_id
    LEFT JOIN emails on emails.id = confidence_score.id
    WHERE score > $1
    ORDER BY matters.id, emails.date
  `,
    [scoreLimit]
  );
  const batchGroup = result.rows.reduce((batchGroup, row) => {
    if (batchGroup[row.matter_id]) {
      batchGroup[row.matter_id] = [...batchGroup[row.matter_id], row.email_id];
    } else {
      batchGroup[row.matter_id] = [row.email_id];
    }
    return batchGroup;
  }, {});

  Prmise.all(
    Object.keys(batchGroup).map((keu) => {
      processBatch(key, batchGroup[key]);
    })
  ).then(() => {
    //Update confidence_score table
  });
}
async function processBatch(matter_id, emails) {
  emails.map(async (email) => {
    await alloyDBClient.query(
      'Update matter set vector = xxxx(vector, $2, $3, $4) where id = $1',
      [matter_id, email.body, email.subject]
    );
  });
}

module.exports.handleBatchEmail = async () => {
  const matterlist = await alloyDBClient.query('SELECT id FROM matters;');
  for (let val of matterlist.rows) {
    await handleMatchingEmail(val.id);
  }
};

const getMatchingScore = async (body) => {
  const { matterId } = body;
  const matchingEmails = await alloyDBClient.query(
    `
      SELECT uuid AS email_id
      FROM emails
      WHERE user_id =
            (SELECT uuid FROM users WHERE organization_id = (SELECT organization_id FROM matters WHERE id = $1))
      AND email_category = 'Legal'        
    `,
    [matterId]
  );
  if (matchingEmails.rowCount) {
    for (let val of matchingEmails.rows) {
      const matchingMatterId = await this.getMatchingMatter({
        emailId: val.email_id,
      });
      if (matchingMatterId.msg == 'ok') {
        await alloyDBClient.query(
          `
              UPDATE confidence_score
              SET matter_id = $1,
                  score=(SELECT (1 - (email_vector <=> matter_vector)) * 100 as score
                        FROM emails,
                              matters
                        WHERE emails.uuid = $2
                          AND matters.id = $1
                        LIMIT 1)
              WHERE email_id = $2              
            `,
          [matchingMatterId.rlt[0].id, val.email_id]
        );
      } else {
        await alloyDBClient.query(
          `
            DELETE
            FROM confidence_score
            WHERE email_id = $1;          
          `,
          [val.email_id]
        );
      }
    }
  }
};

// Matters
module.exports.handleUpdateMatters = async (body) => {
  try {
    const data = body.event.data.new;
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
    await getMatchingScore({ matterId: data.id });
    return { status: 'ok', op: 'update' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertMatters = async (body) => {
  try {
    const data = body.event.data.new;
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
    await getMatchingScore({ matterId: data.id });
    return { status: 'ok', op: 'insert' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleDeleteMatters = async (body) => {
  try {
    const data = body.event.data.old;
    await getMatchingScore({ matterId: data.id });
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

// Emails

module.exports.handleUpdateEmails = async (body) => {
  try {
    const data = body.event.data.new;
    await alloyDBClient.query(
      `
        UPDATE emails
        SET email_vector = embedding('textembedding-gecko@003',
                            CONCAT(E'Email info \n\n', 'sender: ', sender, 
                              E'\n', 'email_id: ', email_id, 
                              E'\n', 'cc: ', cc, 
                              E'\n', 'bcc: ', bcc, 
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

    await alloyDBClient.query(
      `
          UPDATE emails
          SET email_contact_vector = embedding('textembedding-gecko@003',
                                      CONCAT(E'Email info\n\n', 'Sender: ', sender, 
                                          E'\n', 'email_id: ', email_id, 
                                          E'\n', 'cc: ', cc, 
                                          E'\n', 'bcc: ', bcc, 
                                          E'\n', 'Recipient: ', recipient, 
                                          E'\n'
                                      )
                                    )
          WHERE uuid = $1;
        `,
      [data.uuid]
    );
    const matchingMatterId = await this.getMatchingMatter({
      emailId: data.uuid,
    });

    if (matchingMatterId.msg == 'ok') {
      // calculate score
      const score = await alloyDBClient.query(
        `
          SELECT (1 - (email_vector <=> matter_vector)) * 100 as score
          FROM matters,
              emails
          WHERE emails.uuid = $1
            and matters.id = $2;
        `,
        [data.uuid, matchingMatterId.rlt[0].id]
      );
      await alloyDBClient.query(
        `
          INSERT INTO confidence_score (email_id, matter_id, score)
          VALUES ($1, $2, $3)
          ON CONFLICT (email_id)
              DO UPDATE
              SET matter_id = $2,
                  score = $3;        
        `,
        [data.uuid, matchingMatterId.rlt[0].id, score.rows[0].score]
      );
    } else {
      await alloyDBClient.query(
        `
          DELETE FROM confidence_score WHERE email_id=$1;
        `,
        [data.uuid]
      );
    }
    return { status: 'ok', op: 'update' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertEmails = async (body) => {
  try {
    const data = body.event.data.new;
    await alloyDBClient.query(
      `
        UPDATE emails
        SET email_vector = embedding('textembedding-gecko@003',
                            CONCAT(E'Email info \n\n', 'sender: ', sender, 
                              E'\n', 'email_id: ', email_id, 
                              E'\n', 'cc: ', cc, 
                              E'\n', 'bcc: ', bcc, 
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

    await alloyDBClient.query(
      `
        UPDATE emails
          SET email_contact_vector = embedding('textembedding-gecko@003',
                                      CONCAT(E'Email info\n\n', 'Sender: ', sender, 
                                          E'\n', 'email_id: ', email_id, 
                                          E'\n', 'cc: ', cc, 
                                          E'\n', 'bcc: ', bcc, 
                                          E'\n', 'Recipient: ', recipient, 
                                          E'\n'
                                      )
                                    )
          WHERE uuid = $1;
        `,
      [data.uuid]
    );

    if (matchingMatterId.msg == 'ok') {
      const score = await alloyDBClient.query(
        `
          SELECT (1 - (email_vector <=> matter_vector)) * 100 as score
          FROM matters,
              emails
          WHERE emails.uuid = $1
            and matters.id = $2;
        `,
        [data.uuid, matchingMatterId.rlt[0].id]
      );
      await alloyDBClient.query(
        `
          INSERT INTO confidence_score(email_id, matter_id, score)
          values($1, $2, $3);        
        `,
        [data.uuid, matchingMatterId.rlt[0].id, score.rows[0].score]
      );
    }
    return { status: 'ok', op: 'insert' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleDeleteEmails = async (body) => {
  try {
    const data = body.event.data.old;
    alloyDBClient.query(
      `
        DELETE FROM confidence_score WHERE email_id=$1;
      `,
      [data.uuid]
    );
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

//Contacts

module.exports.handleUpdateContacts = async (body) => {
  try {
    const data = body.event.data.new;
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
    await getMatchingScore(data.matter_id);
    return { status: 'ok', op: 'update' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};

module.exports.handleInsertContacts = async (body) => {
  try {
    const data = body.event.data.new;
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

    return { status: 'ok', op: 'insert' };
  } catch (error) {
    console.log(error);
    throw new Error(error);
  }
};
