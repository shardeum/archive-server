import { Config } from '../Config'
import { init, runCreate, close, digesterDatabase } from './sqlite3storage'

export const initializeDB = async (config: Config): Promise<void> => {
  await init(config)
  await runCreate(
    digesterDatabase,
    'CREATE TABLE if not exists `txDigests` (`cycleStart` NUMBER NOT NULL UNIQUE, `cycleEnd` NUMBER NOT NULL UNIQUE, `txCount` NUMBER NOT NULL, `hash` TEXT NOT NULL, PRIMARY KEY (`cycleStart`, `cycleEnd`))'
  )
}

export const closeDatabase = async (): Promise<void> => {
  await close(digesterDatabase, 'TxDigesterDB')
}
