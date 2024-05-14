import { P2P } from '@shardus/types'
import { config } from '../Config'
import { queryLatestCycleRecords } from '../dbstore/cycles'
import * as Crypto from '../Crypto'
import { ArchiverCycleResponse } from '../Data/Cycles'

let cachedCycleRecords: P2P.CycleCreatorTypes.CycleData[] = []
const signedCacheCycleRecords: Map<number, ArchiverCycleResponse> = new Map()
let lastCacheUpdateFromDBRunning = false

async function updateCacheFromDB(): Promise<void> {
  if (lastCacheUpdateFromDBRunning) {
    return
  }

  lastCacheUpdateFromDBRunning = true

  try {
    cachedCycleRecords = await queryLatestCycleRecords(config.REQUEST_LIMIT.MAX_CYCLES_PER_REQUEST)
  } catch (error) {
    console.log('Error updating latest cache: ', error)
  } finally {
    lastCacheUpdateFromDBRunning = false
  }
}

export async function addCyclesToCache(cycles: P2P.CycleCreatorTypes.CycleData[]): Promise<void> {
  if (cachedCycleRecords.length === 0) {
    await updateCacheFromDB()
  }

  for (const cycle of cycles) {
    cachedCycleRecords.unshift(cycle)
  }
  cycles.sort((a, b) => a.counter - b.counter)

  if (cachedCycleRecords.length > config.REQUEST_LIMIT.MAX_CYCLES_PER_REQUEST) {
    cachedCycleRecords.splice(config.REQUEST_LIMIT.MAX_CYCLES_PER_REQUEST)
  }
  signedCacheCycleRecords.clear()
}

export async function getLatestCycleRecordsFromCache(count: number): Promise<ArchiverCycleResponse> {
  if (cachedCycleRecords.length === 0) {
    await updateCacheFromDB()
  }
  if (signedCacheCycleRecords.has(count)) return signedCacheCycleRecords.get(count)

  const cycleInfo = cachedCycleRecords.slice(0, count)
  const signedCycleRecords = Crypto.sign({ cycleInfo })
  signedCacheCycleRecords.set(count, signedCycleRecords)
  return signedCycleRecords
}
