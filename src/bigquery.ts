import {  managedwriter, adapt } from '@google-cloud/bigquery-storage'
const { WriterClient, JSONWriter } = managedwriter
import { config } from 'node-config-ts'

import { BigQuery } from '@google-cloud/bigquery'
import { PendingWrite } from '@google-cloud/bigquery-storage/build/src/managedwriter/pending_write'
import { StreamConnection } from '@google-cloud/bigquery-storage/build/src/managedwriter/stream_connection'

// https://github.com/googleapis/nodejs-bigquery-storage/blob/18670627cbebf57c139036a7a949ace599606eb0/src/managedwriter/json_writer.ts#L26C1-L30C35
type JSONPrimitive = string | number | boolean | null
type JSONValue = JSONPrimitive | JSONObject | JSONArray
type JSONObject = { [member: string]: JSONValue }
type PgJSONPrimitive = string | number | boolean | Date | null
type PgJSONValue = PgJSONPrimitive | PgJSONObject | PgJSONArray
type PgJSONObject = { [member: string]: PgJSONValue}
type PgJSONArray = Array<PgJSONValue>
type JSONArray = Array<JSONValue>
type StreamType = typeof managedwriter.DefaultStream | typeof managedwriter.CommittedStream
type TableMetadataObject = { name: string, type: string, mode?: string, description?: string}


export class BQWriter {
    offsetValue: number
    projectId: string
    datasetId: string
    tableId: string
    writer : managedwriter.JSONWriter | null
    writeClient: managedwriter.WriterClient | null
    streamType : StreamType
    isCdc : boolean
    schema : TableMetadataObject[]
    pending_rows: JSONObject[]
    constructor(
        projectId: string,
        datasetId: string,
        tableId: string,
        streamType: StreamType = managedwriter.CommittedStream,
        isCdc: boolean = false
    ) {
        this.offsetValue = 0
        this.projectId = projectId
        this.datasetId = datasetId
        this.tableId = tableId
        this.writeClient = null
        this.writer = null
        this.streamType = streamType
        this.isCdc = isCdc
        if (isCdc) {
            this.streamType = managedwriter.DefaultStream
        }
        this.schema = []
        this.pending_rows = []
    }

    async init() {
        const destinationTable = `projects/${this.projectId}/datasets/${this.datasetId}/tables/${this.tableId}`;
        console.log(destinationTable)
        const writeClient = new WriterClient({ projectId: this.projectId,  keyFilename: config.GCP_KEY });
        this.writeClient = writeClient
        const bigquery = new BigQuery({ projectId: this.projectId,  keyFilename: config.GCP_KEY })
        try {
            const dataset = bigquery.dataset(this.datasetId)
            const table = await dataset.table(this.tableId)
            const [metadata] = await table.getMetadata()
            const { schema } = metadata

            if (this.isCdc) {
                schema.fields.push(
                    { name: '_CHANGE_TYPE', type: 'STRING', mode: 'NULLABLE' }
                )
            }
            this.schema = schema.fields
            const storageSchema = adapt.convertBigQuerySchemaToStorageTableSchema(schema)
            const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(
                storageSchema, 'root'
            )
            const connection: StreamConnection = await ( async ()=>{
                switch (this.streamType) {
                    case managedwriter.DefaultStream:
                        return await writeClient.createStreamConnection({
                            streamId: managedwriter.DefaultStream,
                            destinationTable
                        })
                    case managedwriter.CommittedStream:
                        const streamId = await writeClient.createWriteStream({
                            streamType: managedwriter.CommittedStream, destinationTable
                        })
                        return await writeClient.createStreamConnection({
                            streamId,
                            destinationTable
                        })
                }
            })()

            this.writer = new JSONWriter({
                connection,
                protoDescriptor
            })
    
        } catch (err) {
            console.error(err);
            writeClient.close()
            throw err
        }
    }

// pgのresponseを取り込める形式に変換する
    fixPgRow(row: PgJSONObject): JSONObject {
        const fixTimestamp = (value: PgJSONValue): JSONValue => {
            if (value instanceof Date) {
                return (value as Date).getTime() * 1000
            } else if ( typeof value === "number") {
                return value as number
            } else {
                return null
            }
        }
        const jsonFixedRow:  JSONObject = (row=>{
            let res: JSONObject = {}
            this.schema.forEach(s => {
                const k = s.name
                if (s.type === 'JSON') {
                    res[k] = (s.mode === "REPEATED" && Array.isArray(row[k]))  ? 
                    (row[k] as PgJSONObject[]).map(e => JSON.stringify(e)) : JSON.stringify(row[k])
                }
                else if (s.type === 'TIMESTAMP') {
                    if (s.mode === "REPEATED" && Array.isArray(row[k])) {
                        res[k] = (row[k] as any[]).map(e => fixTimestamp(e))
                    } else {
                        res[k] = fixTimestamp(row[k])
                    }
                }
                else {
                    res[k] = row[k] as JSONValue
                }
            })

            return res
        })(row)
        return jsonFixedRow
    }

    appendRow(row: JSONObject) {
        this.pending_rows.push(row)
    }

    async appendRows(rows: JSONObject[]) {
        let appendrows = this.pending_rows.splice(0, this.pending_rows.length)
        if (rows.length === 0 && appendrows.length === 0) {
            return
        } else if (rows.length > 0 && appendrows.length === 0) {
            appendrows = rows
        } else if (rows.length > 0 && appendrows.length > 0) {
            appendrows = appendrows.concat(rows)
        } 
        console.log("flush2", this.tableId, appendrows.length, this.offsetValue)

        const pendingWrites: PendingWrite[] = []
        if (this.writer === null) throw Error('writer is null')
       
        let pw = this.writer.appendRows(appendrows, this.offsetValue)
        pendingWrites.push(pw)
        const results = await Promise.all(
            pendingWrites.map(pw => pw.getResult())
        )
        this.offsetValue += appendrows.length
    }

    close() {
        this.writeClient?.close()
    }

}
