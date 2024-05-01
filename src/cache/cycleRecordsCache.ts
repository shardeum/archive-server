import { P2P } from '@shardus/types'
import { config } from '../Config'
import { queryLatestCycleRecords } from '../dbstore/cycles'

let cachedCycleRecords: P2P.CycleCreatorTypes.CycleData[] = []
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
  cycles.sort((a, b) => a.counter - b.counter);
  
  if (cachedCycleRecords.length === 0) {
    await updateCacheFromDB()
  }

  for (const cycle of cycles) {
    if(cycle.counter < cachedCycleRecords[0].counter) {
      continue
    }

    cachedCycleRecords.unshift(cycle)
  }

  if (cachedCycleRecords.length > config.REQUEST_LIMIT.MAX_CYCLES_PER_REQUEST) {
    cachedCycleRecords.splice(config.REQUEST_LIMIT.MAX_CYCLES_PER_REQUEST)
  }
}

export async function getLatestCycleRecords(count: number): Promise<P2P.CycleCreatorTypes.CycleData[]> {
  if (cachedCycleRecords.length === 0) {
    await updateCacheFromDB()
  }

  return cachedCycleRecords.slice(0, count)
}
