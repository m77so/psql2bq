import { Client } from 'pg'
import { config } from 'node-config-ts'



export async function getAllTableInPsqlSchema(schemaName: string) {
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
        SELECT table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = $1 
        `, [schemaName])
        const tables = res.rows.map(row=>row.table_name)
        return tables
    } catch (err) {
        console.error(err);
    } finally {
        await client.end();
    }
    return []
    
}