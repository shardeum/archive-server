import { Cycle, CycleChain } from './Data/Cycles'
import { Config } from './Config'
import * as Data from './Data/Data'
import knex = require('knex')
import { DataQueryResponse, ReceiptMapResult, socketServer, SummaryBlob } from './Data/Data'
import { Database, BaseModel, FS_Persistence_Adapter } from 'tydb'

let db: knex

export let Collection: any

export async function initStorage (config: Config) {
  // Get db file location from config
  let dbFile = config.ARCHIVER_DB

  Collection = new Database<Data.ArchivedCycle>({
    ref: dbFile,
    model: Data.ArchivedCycle,
    persistence_adapter: FS_Persistence_Adapter,
    autoCompaction: 10 * 30 * 1000, // database compaction every 10 cycles
  })
  await Collection.createIndex({ fieldName: 'cycleMarker', unique: true })
}

export async function insertArchivedCycle (archivedCycle: any) {
  console.log('Inserting archived cycle', archivedCycle.cycleRecord.counter, archivedCycle.cycleMarker)
  try {
    await Collection.insert([Data.ArchivedCycle.new(archivedCycle)])
    console.log('Successfully inserted archivedCycle', archivedCycle.cycleRecord.counter)
  } catch (e) {
    console.log('Unable to insert archive cycle or it is already stored in to database', archivedCycle.cycleRecord.counter, archivedCycle.cycleMarker)
  }
}

export async function updateReceiptMap (
  receiptMapResult: Data.ReceiptMapResult
) {
  if (!receiptMapResult) return
  // console.log(`Updating receipt map for cycle ${receiptMapResult.cycle}, partition ${receiptMapResult.partition}`, receiptMapResult.receiptMap)
  try {
    let parentCycle = CycleChain.get(receiptMapResult.cycle)

    if (!parentCycle) {
      console.log(
        'Unable find record with parent cycle with counter',
        receiptMapResult.cycle
      )
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(
      parentCycle.marker
    )

    if (!existingArchivedCycle) {
      console.log(
        'Unable find existing archived cycle with marker',
        parentCycle.marker
      )
      return
    }

    let newPartitionMaps: any = {}
    if (
      existingArchivedCycle.receipt &&
      existingArchivedCycle.receipt.partitionMaps
    ) {
      newPartitionMaps = { ...existingArchivedCycle.receipt.partitionMaps }
    }

    newPartitionMaps[receiptMapResult.partition] = receiptMapResult.receiptMap

    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: { $set: { 'receipt.partitionMaps': newPartitionMaps } },
    })
  } catch (e) {
    console.log('Unable to update receipt maps in archived cycle')
    console.log(e)
  }
}

export async function updateSummaryBlob (
  summaryBlob: SummaryBlob,
  cycle: number
) {
  if (!summaryBlob) return
  try {
    let parentCycle = CycleChain.get(cycle)

    if (!parentCycle) {
      console.log('Unable find record with parent cycle with counter', cycle)
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(
      parentCycle.marker
    )

    if (!existingArchivedCycle) {
      console.log(
        'Unable find existing archived cycle with marker',
        parentCycle.marker
      )
      return
    }

    let newPartitionBlobs: any = {}
    if (
      existingArchivedCycle.summary &&
      existingArchivedCycle.summary.partitionBlobs
    ) {
      newPartitionBlobs = { ...existingArchivedCycle.summary.partitionBlobs }
    }

    newPartitionBlobs[summaryBlob.partition] = summaryBlob

    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: { $set: { 'summary.partitionBlobs': newPartitionBlobs } },
    })
  } catch (e) {
    console.log('Unable to update summary blobs in archived cycle')
    console.log(e)
  }
}

export async function updateArchivedCycle(marker: string, field: string, data: any) {
  let updateObj: any = {}
  updateObj[field] = data
  await Collection.update({
    filter: { cycleMarker: marker },
    update: { $set: updateObj },
  })
}

export async function queryAllArchivedCycles (count?: number) {
  let archivedCycles = await Collection.find({
    filter: {},
    sort: {
      'cycleRecord.counter': -1,
    },
    limit: count ? count : null,
    project: {
      _id: 0,
    },
  })
  return archivedCycles
}

export async function queryAllArchivedCyclesBetween (start: number, end: number) {
  let archivedCycles = await Collection.find({
    filter: {
      $and: [
        { 'cycleRecord.counter': { $gte: start } },
        { 'cycleRecord.counter': { $lte: end } },
      ],
    },
    sort: {
      'cycleRecord.counter': -1,
    },
    limit: 5,
    project: {
      _id: 0,
    },
  })

  let cycleRecords = await Collection.find({
    filter: {
      $and: [
        { 'cycleRecord.counter': { $gte: start } },
        { 'cycleRecord.counter': { $lte: end } },
      ],
    },
    sort: {
      'cycleRecord.counter': -1,
    },
  })

  return archivedCycles
}

export async function queryAllCycleRecords () {
  let cycleRecords = await Collection.find({
    filter: {},
    sort: {
      'cycleRecord.counter': -1,
    },
    project: {
      _id: 0,
      cycleMarker: 0,
      receipt: 0,
      data: 0,
      summary: 0,
    },
  })
  return cycleRecords.map((item: any) => item.cycleRecord)
}

export async function queryLatestCycleRecords (count: number = 1) {
  let cycleRecords = await Collection.find({
    filter: {},
    sort: {
      'cycleRecord.counter': -1,
    },
    limit: count,
    project: {
      _id: 0,
      cycleMarker: 0,
      receipt: 0,
      data: 0,
      summary: 0,
    },
  })
  return cycleRecords.map((item: any) => item.cycleRecord)
}

export async function queryCycleRecordsBetween (start: number, end: number) {
  let cycleRecords = await Collection.find({
    filter: {
      $and: [
        { 'cycleRecord.counter': { $gte: start } },
        { 'cycleRecord.counter': { $lte: end } },
      ],
    },
    sort: {
      'cycleRecord.counter': -1,
    },
  })
  return cycleRecords.map((item: any) => item.cycleRecord)
}


export async function queryArchivedCycleByMarker (marker: string) {
  let archivedCycles = await Collection.find({
    filter: { cycleMarker: marker },
  })
  if (archivedCycles.length > 0) return archivedCycles[0]
}

export async function queryReceiptMapHash (counter: number, partition: number) {
  let foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (foundArchivedCycles[0].receipt && foundArchivedCycles[0].receipt.partitionHashes) {
      return foundArchivedCycles[0].receipt.partitionHashes[partition]
    }
  }
}

export async function querySummaryHash (counter: number, partition: number) {
  let foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (foundArchivedCycles[0].summary && foundArchivedCycles[0].summary.partitionHashes) {
      return foundArchivedCycles[0].summary.partitionHashes[partition]
    }
  }
}
