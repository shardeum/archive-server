import { Database } from 'sqlite3'
import * as db from '../dbstore/sqlite3storage'

import { Config, config } from '../Config'
import { createDirectories } from '../Utils'
import { initializeDB } from '../txDigester/index'
import { TransactionDigest } from '../txDigester/txDigests'
import { createDB, runCreate, close } from '../dbstore/sqlite3storage'

export let ipfsRecordsDB: Database

export interface IPFSRecord extends Omit<TransactionDigest, 'txCount'> {
  cid: string
  spaceDID: string
  timestamp: number
}

export const initTxDigestDB = async (config: Config): Promise<void> => initializeDB(config)

export const initializeIPFSRecordsDB = async (config: Config): Promise<void> => {
  createDirectories(config.ARCHIVER_DB)
  ipfsRecordsDB = await createDB(
    `${config.ARCHIVER_DB}/${config.ARCHIVER_DATA.ipfsRecordsDB}`,
    'IPFSRecordsDB'
  )
  await runCreate(
    ipfsRecordsDB,
    'CREATE TABLE if not exists `ipfsRecords` (`CID` TEXT NOT NULL, `cycleStart` NUMBER NOT NULL UNIQUE, `cycleEnd` NUMBER NOT NULL UNIQUE, `hash` TEXT NOT NULL, `timestamp` BIGINT NOT NULL, `spaceDID` TEXT NOT NULL, PRIMARY KEY (`cycleEnd`))'
  )
}

export async function insertUploadedIPFSRecord(ipfsRecord: IPFSRecord): Promise<void> {
  try {
    const fields = Object.keys(ipfsRecord).join(', ')
    const placeholders = Object.keys(ipfsRecord).fill('?').join(', ')
    const values = db.extractValues(ipfsRecord)
    const sql =
      'INSERT INTO ipfsRecords (' +
      fields +
      ') VALUES (' +
      placeholders +
      ') ON CONFLICT (cycleEnd) DO UPDATE SET ' +
      'CID = excluded.CID, ' +
      'cycleStart = excluded.cycleStart, ' +
      'hash = excluded.hash, ' +
      'timestamp = excluded.timestamp, ' +
      'spaceDID = excluded.spaceDID'

    await db.run(ipfsRecordsDB, sql, values)
    if (config.VERBOSE) {
      console.log(
        `Successfully inserted ipfsRecord for cycle records from ${ipfsRecord.cycleStart} to ${ipfsRecord.cycleEnd}`
      )
    }
  } catch (e) {
    console.error(e)
    throw new Error(
      `Unable to Insert ipfsRecord for cycle records from ${ipfsRecord.cycleStart} to ${ipfsRecord.cycleEnd}`
    )
  }
}

export async function getLastUploadedIPFSRecord(): Promise<IPFSRecord> {
  try {
    const sql = `SELECT * FROM ipfsRecords ORDER BY cycleEnd DESC LIMIT 1`
    const lastUploadedDigest = (await db.get(ipfsRecordsDB, sql)) as IPFSRecord
    if (config.VERBOSE) {
      console.log('Last Digest Uploaded to IPFS: ')
      console.dir(lastUploadedDigest)
    }
    return lastUploadedDigest
  } catch (e) {
    console.error('Error while Fetching last uploaded IPFS Record: ', e)
    return null
  }
}

export const closeDatabase = async (): Promise<void> => {
  await close(ipfsRecordsDB, 'IPFSRecordsDB')
}
