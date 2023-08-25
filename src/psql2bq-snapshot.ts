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
    await client.connect()
    const bqw = new BQWriter(
        destination.projectId,
        destination.datasetId,
        destination.tableId
    )
    try {
        console.log([schemaName, tableName])

        await bqw.init()


        const query = new QueryStream(`SELECT * FROM "${schemaName}"."${tableName}"`)

        return new Promise((resolve, reject) => {
            const stream = client.query(query)
            stream.on('end', async () => {
                await bqw.appendRows([])
                //await bqw.close()
                resolve(1)

            })
            stream.on('data', async (row) => {
                bqw.appendRow(bqw.fixPgRow(row))
                if (bqw.pending_rows.length > 1023 && bqw.pending_rows.length % 128 === 0) {
                    await bqw.appendRows([])
                }
            })
        })
    } catch (err) {
        console.log(12345)
        console.error(err);
    } finally {

    }
    console.log(12341)
}



(async () => {
    const srcDataset = process.argv[2]
    const destDataset = process.argv[3]

    const tables = await getAllTableInPsqlSchema(srcDataset)

    for (let table of tables) {
        console.log(table)
        const res = await copyPsql2Bq(srcDataset, table, {
            projectId: config.BigQuery.projectId,
            datasetId: destDataset,
            tableId: table
        })
        console.log(res)

    }
    // await copyPsql2Bq(srcDataset, table, {
    //     projectId: config.BigQuery.projectId,
    //     datasetId: destDataset,
    //     tableId: table
    // })
})()
