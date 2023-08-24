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
        console.log([schemaName, tableName])

        await bqw.init()

        const query = new QueryStream(`SELECT * FROM "${schemaName}"."${tableName}"`)
        console.log('z')
        return new Promise((resolve, reject)=>{
        console.log('z1')

            const stream = client.query(query)
            console.log('r32')
            stream.on('end', async ()=>{
                console.log('xx')
                await bqw.appendRows([])
                console.log('bye')
                await bqw.close()
                resolve(bqw.offsetValue)
            })
            stream.on('error',(err)=>{
                console.log(err)
                reject(err)
            })
            stream.on('data', async (row)=>{
                console.log('t')

                bqw.appendRow(bqw.fixPgRow(row))
                if (bqw.pending_rows.length > 1023 && bqw.pending_rows.length % 128 === 0){
                    await bqw.appendRows([])
                }
            })
        })

    } catch (err) {
        console.error(err);
    } finally {
        
    }
}



(async ()=>{
    const srcDataset = process.argv[2]
    const destDataset = process.argv[3]

    const tables = await getAllTableInPsqlSchema(srcDataset)

    for (let table of tables) {
        console.log(table)
        await copyPsql2Bq(srcDataset, table, {
            projectId: config.BigQuery.projectId,
            datasetId: destDataset,
            tableId: table
        })
        //break
    }
    // await copyPsql2Bq(srcDataset, table, {
    //     projectId: config.BigQuery.projectId,
    //     datasetId: destDataset,
    //     tableId: table
    // })
})()
