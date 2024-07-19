import { Config } from '../Config'
import * as db from './sqlite3storage'

export const initializeDB = async (config: Config): Promise<void> => {
  await db.init(config)
  await db.runCreate(
    'CREATE TABLE if not exists `transactions` (`txId` TEXT NOT NULL UNIQUE PRIMARY KEY, `appReceiptId` TEXT, `timestamp` BIGINT NOT NULL, `cycleNumber` NUMBER NOT NULL, `data` JSON NOT NULL, `originalTxData` JSON NOT NULL)'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_cycleNumber` ON `transactions` (`cycleNumber` DESC)'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_timestamp` ON `transactions` (`timestamp` DESC)'
  )
  await db.runCreate(
    'CREATE INDEX if not exists `transactions_appReceiptId_idx` ON `transactions` (`appReceiptId`)'
  )
  await db.runCreate(
    'CREATE TABLE if not exists `cycles` (`cycleMarker` TEXT NOT NULL UNIQUE PRIMARY KEY, `counter` NUMBER NOT NULL, `cycleRecord` JSON NOT NULL)'
  )
  await db.runCreate('CREATE INDEX if not exists `cycles_idx` ON `cycles` (`counter` DESC)')
  await db.runCreate(
    'CREATE TABLE if not exists `accounts` (`accountId` TEXT NOT NULL UNIQUE PRIMARY KEY, `data` JSON NOT NULL, `timestamp` BIGINT NOT NULL, `hash` TEXT NOT NULL, `cycleNumber` NUMBER NOT NULL, `isGlobal` BOOLEAN NOT NULL)'
  )
  await db.runCreate('CREATE INDEX if not exists `accounts_cycleNumber` ON `accounts` (`cycleNumber` DESC)')
  await db.runCreate('CREATE INDEX if not exists `accounts_timestamp` ON `accounts` (`timestamp` DESC)')
  await db.runCreate(
    'CREATE TABLE if not exists `receipts` (`receiptId` TEXT NOT NULL UNIQUE PRIMARY KEY, `tx` JSON NOT NULL, `cycle` NUMBER NOT NULL, `timestamp` BIGINT NOT NULL, `beforeStateAccounts` JSON, `accounts` JSON NOT NULL, `appliedReceipt` JSON NOT NULL, `appReceiptData` JSON, `executionShardKey` TEXT NOT NULL, `globalModification` BOOLEAN NOT NULL)'
  )
  await db.runCreate('CREATE INDEX if not exists `receipts_cycle` ON `receipts` (`cycle` ASC)')
  await db.runCreate('CREATE INDEX if not exists `receipts_timestamp` ON `receipts` (`timestamp` ASC)')

  await db.runCreate(
    'CREATE TABLE if not exists `originalTxsData` (`txId` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL, `originalTxData` JSON NOT NULL, PRIMARY KEY (`txId`, `timestamp`))'
  )
  await db.runCreate('CREATE INDEX if not exists `originalTxsData_cycle` ON `originalTxsData` (`cycle` ASC)')
  await db.runCreate(
    'CREATE INDEX if not exists `originalTxsData_timestamp` ON `originalTxsData` (`timestamp` ASC)'
  )
  await db.runCreate('CREATE INDEX if not exists `originalTxsData_txId_idx` ON `originalTxsData` (`txId`)')
}

export const closeDatabase = async (): Promise<void> => {
  await db.close()
}
