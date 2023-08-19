import { config } from 'node-config-ts'
import { Client } from 'pg'
import { BQWriter } from './bigquery';




async function main() {
    const bqw = new BQWriter(
        config.BigQuery.projectId,
        config.BigQuery.datasetId,
        config.BigQuery.tableId
    )
    await bqw.init()

    async function sendChanges() {
        const client = new Client({
            user: config.db.user,
            host: config.db.host,
            database: config.db.database,
            password: config.db.password,
            port: config.db.port,
        });
    
    
        try {
            await client.connect();
            const res = await client.query(` select * from pg_logical_slot_get_changes('${config.replication_slot}', null, null);`)
            await bqw.appendRows(res.rows)
            console.log(`${(new Date().toISOString())} ${res.rows.length} records appended`)
        } catch (err) {
            console.error(err);
        } finally {
            await client.end();
        }

        setTimeout(async () => {
            try {
              await sendChanges();
            } catch (error) {
              console.error(error);
            }
          }, 4000);
    }

    await sendChanges()
}

main()
