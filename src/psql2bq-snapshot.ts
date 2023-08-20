import { Client, Pool } from 'pg'
import { BigQuery } from '@google-cloud/bigquery'
import { config } from 'node-config-ts'
import { getAllTableInPsqlSchema } from './postgres'
import { BQWriter } from './bigquery'
import QueryStream from 'pg-query-stream'

async function copyPsql2Bq(
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
    const bqw = new BQWriter(
        destination.projectId,
        destination.datasetId,
        destination.tableId
    )
    try {
        await client.connect();
        console.log([schemaName, tableName])
        const pool = new Pool()
        pool.connect((err, c, done) => {
            const query = new QueryStream(`SELECT * FROM "${schemaName}"."${tableName}"`)
            const stream = c.query(query)
            stream.on('end', done)
            stream.on('data', (row)=>{

            })
        })
        const res = await client.query(new QueryStream(query))

        await bqw.init()
        await bqw.appendRows(
            bqw.fixPgRows(res.rows)
        )
    } catch (err) {
        console.error(err);
    } finally {
        await client.end();
        await bqw.close()
    }
}



(async ()=>{
    const srcDataset = process.argv[2]
    const destDataset = process.argv[3]

    // const tables = await getAllTableInPsqlSchema(srcDataset)

    // for (let table of tables) {
    //     console.log(table)
    //     await copyPsql2Bq(srcDataset, table, {
    //         projectId: config.BigQuery.projectId,
    //         datasetId: destDataset,
    //         tableId: table
    //     })
    // }
    await copyPsql2Bq(srcDataset, table, {
        projectId: config.BigQuery.projectId,
        datasetId: destDataset,
        tableId: table
    })
})()
