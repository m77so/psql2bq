/* tslint:disable */
/* eslint-disable */
interface Config {
  db: Db;
  BigQuery: BigQuery;
  replication_slot: string;
  GCP_KEY: string;
}
interface BigQuery {
  projectId: string;
  datasetId: string;
  tableId: string;
}
interface Db {
  user: string;
  host: string;
  database: string;
  password: string;
  port: number;
}