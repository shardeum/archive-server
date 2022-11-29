import * as db from './sqlite3storage'
import { extractValues, extractValuesFromArray } from './sqlite3storage'
import { P2P, StateManager } from '@shardus/types'
import * as Logger from '../Logger'
import { config } from '../Config'
import { DeSerializeFromJsonString, SerializeToJsonString } from '../utils/serialization'

export interface Cycle {
  counter: number
  cycleRecord: P2P.CycleCreatorTypes.CycleRecord
  cycleMarker: StateManager.StateMetaDataTypes.CycleMarker
}

export async function insertCycle(cycle: Cycle) {
  try {
    const fields = Object.keys(cycle).join(', ')
    const placeholders = Object.keys(cycle).fill('?').join(', ')
    const values = extractValues(cycle)
    let sql = 'INSERT OR REPLACE INTO cycles (' + fields + ') VALUES (' + placeholders + ')'
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Cycle', cycle.cycleRecord.counter, cycle.cycleMarker)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert cycle or it is already stored in to database',
      cycle.cycleRecord.counter,
      cycle.cycleMarker
    )
  }
}

export async function bulkInsertCycles(cycles: Cycle[]) {
  try {
    const fields = Object.keys(cycles[0]).join(', ')
    const placeholders = Object.keys(cycles[0]).fill('?').join(', ')
    const values = extractValuesFromArray(cycles)
    let sql = 'INSERT OR REPLACE INTO cycles (' + fields + ') VALUES (' + placeholders + ')'
    for (let i = 1; i < cycles.length; i++) {
      sql = sql + ', (' + placeholders + ')'
    }
    await db.run(sql, values)
    Logger.mainLogger.debug('Successfully inserted Cycles', cycles.length)
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to bulk insert Cycles', cycles.length)
  }
}

export async function updateCycle(marker: string, cycle: Cycle) {
  try {
    const sql = `UPDATE cycles SET counter = $counter, cycleRecord = $cycleRecord WHERE cycleMarker = $marker `
    await db.run(sql, {
      $counter: cycle.counter,
      $cycleRecord: cycle.cycleRecord && SerializeToJsonString(cycle.cycleRecord),
      $marker: marker,
    })
    if (config.VERBOSE) {
      Logger.mainLogger.debug('Updated cycle for counter', cycle.cycleRecord.counter, cycle.cycleMarker)
    }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error('Unable to update Cycle', cycle.cycleMarker)
  }
}

export async function queryCycleByMarker(marker: string) {
  try {
    const sql = `SELECT * FROM cycles WHERE cycleMarker=? LIMIT 1`
    const cycle: any = await db.get(sql, [marker])
    if (cycle) {
      if (cycle.cycleRecord) cycle.cycleRecord = DeSerializeFromJsonString(cycle.cycleRecord)
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle marker', cycle)
    }
    return cycle
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryLatestCycleRecords(count: number) {
  try {
    const sql = `SELECT * FROM cycles ORDER BY counter DESC LIMIT ${count ? count : 100}`
    let cycleRecords: any = await db.all(sql)
    if (cycleRecords.length > 0) {
      cycleRecords = cycleRecords.map((cycleRecord: any) => {
        if (cycleRecord.cycleRecord)
          cycleRecord.cycleRecord = DeSerializeFromJsonString(cycleRecord.cycleRecord)
        return cycleRecord.cycleRecord
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle latest', cycleRecords)
    }
    return cycleRecords
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryCycleRecordsBetween(start: number, end: number) {
  try {
    const sql = `SELECT * FROM cycles WHERE counter BETWEEN ? AND ? ORDER BY counter ASC`
    let cycleRecords: any = await db.all(sql, [start, end])
    if (cycleRecords.length > 0) {
      cycleRecords = cycleRecords.map((cycleRecord: any) => {
        if (cycleRecord.cycleRecord)
          cycleRecord.cycleRecord = DeSerializeFromJsonString(cycleRecord.cycleRecord)
        return cycleRecord.cycleRecord
      })
    }
    if (config.VERBOSE) {
      Logger.mainLogger.debug('cycle between', cycleRecords)
    }
    return cycleRecords
  } catch (e) {
    Logger.mainLogger.error(e)
  }
}

export async function queryCyleCount() {
  let cycles
  try {
    const sql = `SELECT COUNT(*) FROM cycles`
    cycles = await db.get(sql, [])
  } catch (e) {
    Logger.mainLogger.error(e)
  }
  if (config.VERBOSE) {
    Logger.mainLogger.debug('Cycle count', cycles)
  }
  if (cycles) cycles = cycles['COUNT(*)']
  else cycles = 0
  return cycles
}
