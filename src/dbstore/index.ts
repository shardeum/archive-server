import { Config } from '../Config'
import * as db from './sqlite3storage'

export const initializeDB = async (config: Config) => {
  await db.init(config)
  await db.runCreate(
    'CREATE TABLE if not exists `transactions` (`txId` TEXT NOT NULL UNIQUE PRIMARY KEY, `accountId` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycleNumber` NUMBER NOT NULL, `data` JSON NOT NULL, `result` JSON NOT NULL, `originTxData` JSON, `sign` JSON NOT NULL)'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_idx` ON `transactions` (`cycleNumber` DESC, `timestamp` DESC, `accountId`)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `cycles` (`cycleMarker` TEXT NOT NULL UNIQUE PRIMARY KEY, `counter` NUMBER NOT NULL, `cycleRecord` JSON NOT NULL)'
  )
  await db.runCreate('CREATE INDEX if not exists `cycles_idx` ON `cycles` (`counter` DESC)')
  await db.runCreate(
    'CREATE TABLE if not exists `accounts` (`accountId` TEXT NOT NULL UNIQUE PRIMARY KEY, `data` JSON NOT NULL, `timestamp` BIGINT NOT NULL, `hash` TEXT NOT NULL, `cycleNumber` NUMBER NOT NULL, `isGlobal` BOOLEAN)'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `accounts_idx` ON `accounts` (`cycleNumber` DESC, `timestamp` DESC)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `receipts` (`receiptId` TEXT NOT NULL UNIQUE PRIMARY KEY, `tx` JSON NOT NULL, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `result` JSON NOT NULL, `beforeStateAccounts` JSON, `accounts` JSON NOT NULL, `receipt` JSON, `sign` JSON NOT NULL)'
  )
  await db.runCreate('CREATE INDEX if not exists `receipts_idx` ON `receipts` (`cycle` ASC, `timestamp` ASC)')
  await db.runCreate(
    'CREATE TABLE if not exists `originalTxsData` (`txId` TEXT NOT NULL UNIQUE PRIMARY KEY, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL, `originalTxData` JSON NOT NULL, `sign` JSON NOT NULL)'
  )
  // await db.runCreate('Drop INDEX if exists `originalTxData_idx`');
  await db.runCreate(
    'CREATE INDEX if not exists `originalTxsData_idx` ON `originalTxsData` (`cycle` ASC, `timestamp` ASC)'
  )
}
