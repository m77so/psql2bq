/* tslint:disable */
/* eslint-disable */
interface Config {
  db: Db;
}
interface Db {
  user: string;
  host: string;
  database: string;
  password: string;
  port: number;
}