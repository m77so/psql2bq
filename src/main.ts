import { config } from 'node-config-ts'
import { Client } from 'pg'

const client = new Client({
    user: config.db.user,
    host: config.db.host,
    database: config.db.database,
    password: config.db.password,
    port: config.db.port,
});


client.connect();

client.query('SELECT 1', (err, res) => {
    if (err) {
      console.error(err);
      return;
    }
    console.log(res.rows);
    client.end();
  });