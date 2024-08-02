import { Config } from '../Config'
import {
  init,
  runCreate,
  close,
  accountDatabase,
  transactionDatabase,
  cycleDatabase,
  receiptDatabase,
  originalTxDataDatabase,
} from './sqlite3storage'

export const initializeDB = async (config: Config): Promise<void> => {
  await init(config)
  await runCreate(
    transactionDatabase,
    'CREATE TABLE if not exists `transactions` (`txId` TEXT NOT NULL UNIQUE PRIMARY KEY, `appReceiptId` TEXT, `timestamp` BIGINT NOT NULL, `cycleNumber` NUMBER NOT NULL, `data` JSON NOT NULL, `originalTxData` JSON NOT NULL)'
  )
  await runCreate(
    transactionDatabase,
    'CREATE INDEX if not exists `transactions_timestamp` ON `transactions` (`timestamp` ASC)'
  )
  await runCreate(
    transactionDatabase,
    'CREATE INDEX if not exists `transactions_cycleNumber_timestamp` ON `transactions` (`cycleNumber` ASC, `timestamp` ASC)'
  )
  await runCreate(
    transactionDatabase,
    'CREATE INDEX if not exists `transactions_appReceiptId_idx` ON `transactions` (`appReceiptId`)'
  )
  await runCreate(
    cycleDatabase,
    'CREATE TABLE if not exists `cycles` (`cycleMarker` TEXT NOT NULL UNIQUE PRIMARY KEY, `counter` NUMBER NOT NULL, `cycleRecord` JSON NOT NULL)'
  )
  await runCreate(cycleDatabase, 'CREATE INDEX if not exists `cycles_idx` ON `cycles` (`counter` ASC)')
  await runCreate(
    accountDatabase,
    'CREATE TABLE if not exists `accounts` (`accountId` TEXT NOT NULL UNIQUE PRIMARY KEY, `data` JSON NOT NULL, `timestamp` BIGINT NOT NULL, `hash` TEXT NOT NULL, `cycleNumber` NUMBER NOT NULL, `isGlobal` BOOLEAN NOT NULL)'
  )
  await runCreate(
    accountDatabase,
    'CREATE INDEX if not exists `accounts_cycleNumber` ON `accounts` (`cycleNumber` ASC)'
  )
  await runCreate(
    accountDatabase,
    'CREATE INDEX if not exists `accounts_timestamp` ON `accounts` (`timestamp` ASC)'
  )
  await runCreate(
    accountDatabase,
    'CREATE INDEX if not exists `accounts_cycleNumber_timestamp` ON `accounts` (`cycleNumber` ASC, `timestamp` ASC)'
  )
  await runCreate(
    receiptDatabase,
    'CREATE TABLE if not exists `receipts` (`receiptId` TEXT NOT NULL UNIQUE PRIMARY KEY, `tx` JSON NOT NULL, `cycle` NUMBER NOT NULL, `applyTimestamp` BIGINT NOT NULL, `timestamp` BIGINT NOT NULL, `signedReceipt` JSON NOT NULL, `afterStates` JSON, `beforeStates` JSON, `appReceiptData` JSON, `executionShardKey` TEXT NOT NULL, `globalModification` BOOLEAN NOT NULL)'
  )
  await runCreate(receiptDatabase, 'CREATE INDEX if not exists `receipts_cycle` ON `receipts` (`cycle` ASC)')
  await runCreate(
    receiptDatabase,
    'CREATE INDEX if not exists `receipts_timestamp` ON `receipts` (`timestamp` ASC)'
  )
  await runCreate(
    receiptDatabase,
    'CREATE INDEX if not exists `receipts_cycle_timestamp` ON `receipts` (`cycle` ASC, `timestamp` ASC)'
  )
  await runCreate(
    originalTxDataDatabase,
    'CREATE TABLE if not exists `originalTxsData` (`txId` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `cycle` NUMBER NOT NULL, `originalTxData` JSON NOT NULL, PRIMARY KEY (`txId`, `timestamp`))'
  )
  await runCreate(
    originalTxDataDatabase,
    'CREATE INDEX if not exists `originalTxsData_cycle` ON `originalTxsData` (`cycle` ASC)'
  )
  await runCreate(
    originalTxDataDatabase,
    'CREATE INDEX if not exists `originalTxsData_timestamp` ON `originalTxsData` (`timestamp` ASC)'
  )
  await runCreate(
    originalTxDataDatabase,
    'CREATE INDEX if not exists `originalTxsData_cycle_timestamp` ON `originalTxsData` (`cycle` ASC, `timestamp` ASC)'
  )
  await runCreate(
    originalTxDataDatabase,
    'CREATE INDEX if not exists `originalTxsData_txId` ON `originalTxsData` (`txId`)'
  )
}

export const closeDatabase = async (): Promise<void> => {
  await close(accountDatabase, 'Account')
  await close(transactionDatabase, 'Transaction')
  await close(cycleDatabase, 'Cycle')
  await close(receiptDatabase, 'Receipt')
  await close(originalTxDataDatabase, 'OriginalTxData')
}
