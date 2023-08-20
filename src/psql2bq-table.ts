import { Client } from 'pg'
import { BigQuery } from '@google-cloud/bigquery'
import { config } from 'node-config-ts'
import { getAllTableInPsqlSchema } from './postgres'



async function createBqTableFromPsql(
    schemaName: string,
    tableName: string,
    destination: {
        projectId: string,
        datasetId: string,
        tableId: string
    }
) {
    const client = new Client({
        user: config.db.user,
        host: config.db.host,
        database: config.db.database,
        password: config.db.password,
        port: config.db.port,
    })

    try {
        await client.connect();
        const res = await client.query(` 
        SELECT column_name, data_type,  udt_name::regtype
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = $1 and table_name = $2
        `, [schemaName, tableName])
        const bqSchema = res.rows.map(row => {
            let type: string | null 
            let rdtype = row.data_type
            let isArray = false
            if (row.data_type === 'ARRAY') {
                rdtype = row.udt_name.slice(0, -2)
                isArray = true
            }
            switch(rdtype) { 
                case 'character varying':
                case 'text':
                case 'character':
                    type = "STRING"
                    break
                case 'timestamp with time zone':
                    type = "TIMESTAMP"
                    break
                case 'boolean':
                    type = "BOOLEAN"
                    break
                case 'integer':
                case 'smallint':
                case 'bigint':
                    type = "INT64"
                    break
                case 'jsonb':
                    type = "JSON"
                    break
                case 'USER-DEFINED':
                    type = "STRING"
                    //console.log(row.column_name + " is user defined (" + row.udt_name + ")")
                    break
                default:
                    console.error(row.column_name + " " + rdtype + " is not deifned (" + row.udt_name + ")")
                    type = "STRING"

            }
            return {
                name: row.column_name,
                type: type,
                mode: isArray ? "REPEATED" : "NULLABLE"
            }
        })
        const bigquery = new BigQuery({ projectId: destination.projectId,  keyFilename: config.GCP_KEY })

        const dataset = bigquery.dataset(destination.datasetId)
        const table = dataset.table(destination.tableId)
        await table.delete()
        await table.create({schema: bqSchema, createDisposition: 'CREATE_IF_NEEDED', writeDisposition: 'WRITE_TRUNCATE' })
  
    } catch (err) {
        console.error(err);
    } finally {
        await client.end();
    }
}
(async ()=>{
    const srcDataset = process.argv[2]
    const destDataset = process.argv[3]

    const tables = await getAllTableInPsqlSchema(srcDataset)

    for (let table of tables) {
        console.log(table)
        await createBqTableFromPsql(srcDataset, table, {
            projectId: config.BigQuery.projectId,
            datasetId: destDataset,
            tableId: table
        })
    }
})()


