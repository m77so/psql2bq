import {  managedwriter, adapt } from '@google-cloud/bigquery-storage'
const { WriterClient, JSONWriter } = managedwriter
import { config } from 'node-config-ts'

import { BigQuery } from '@google-cloud/bigquery'
import { PendingWrite } from '@google-cloud/bigquery-storage/build/src/managedwriter/pending_write'

// https://github.com/googleapis/nodejs-bigquery-storage/blob/18670627cbebf57c139036a7a949ace599606eb0/src/managedwriter/json_writer.ts#L26C1-L30C35
type JSONPrimitive = string | number | boolean | null
type JSONValue = JSONPrimitive | JSONObject | JSONArray
type JSONObject = { [member: string]: JSONValue }
type JSONArray = Array<JSONValue>

export class BQWriter<T extends JSONObject> {
    offsetValue: number
    projectId: string
    datasetId: string
    tableId: string
    writer : managedwriter.JSONWriter | null
    writeClient: managedwriter.WriterClient | null
    constructor(
        projectId: string,
        datasetId: string,
        tableId: string
    ) {
        this.offsetValue = 0
        this.projectId = projectId
        this.datasetId = datasetId
        this.tableId = tableId
        this.writeClient = null
        this.writer = null
    }

    async init() {
        const destinationTable = `projects/${this.projectId}/datasets/${this.datasetId}/tables/${this.tableId}`;
        const streamType = managedwriter.CommittedStream ;
        const writeClient = new WriterClient({ projectId: this.projectId,  keyFilename: config.GCP_KEY });
        this.writeClient = writeClient
        const bigquery = new BigQuery({ projectId: this.projectId,  keyFilename: config.GCP_KEY })
        try {
            const dataset = bigquery.dataset(this.datasetId)
            const table = await dataset.table(this.tableId)
            const [metadata] = await table.getMetadata()
            const { schema } = metadata
            const storageSchema = adapt.convertBigQuerySchemaToStorageTableSchema(schema)
            const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(
                storageSchema, 'root'
            )
            const streamId = await writeClient.createWriteStream({
                streamType, destinationTable
            })
            const connection = await writeClient.createStreamConnection({
                streamId
            })
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

    async appendRows(rows: T[]) {
        const pendingWrites: PendingWrite[] = []
        if (this.writer === null) throw Error('writer is null')
        let pw = this.writer.appendRows(rows, this.offsetValue)
        pendingWrites.push(pw)
        const results = await Promise.all(
            pendingWrites.map(pw => pw.getResult())
        )
        this.offsetValue += rows.length
    }

    close() {
        this.writeClient?.close()
    }

}