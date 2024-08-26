import { Config } from '../Config'
import { Database } from 'sqlite3'
import { createDB, runCreate, close } from '../dbstore/sqlite3storage'
import { createDirectories } from '../Utils'

export let digesterDatabase: Database

export const initializeDB = async (config: Config): Promise<void> => {
  createDirectories(config.ARCHIVER_DB_DIR)
  digesterDatabase = await createDB(
    `${config.ARCHIVER_DB_DIR}/${config.ARCHIVER_DATA.txDigestDB}`,
    'TxDigestDB'
  )
  await runCreate(
    digesterDatabase,
    'CREATE TABLE if not exists `txDigests` (`cycleStart` NUMBER NOT NULL UNIQUE, `cycleEnd` NUMBER NOT NULL UNIQUE, `txCount` NUMBER NOT NULL, `hash` TEXT NOT NULL, PRIMARY KEY (`cycleStart`, `cycleEnd`))'
  )
}

export const closeDatabase = async (): Promise<void> => {
  await close(digesterDatabase, 'TxDigesterDB')
}
