import { Cycle, CycleChain } from './Data/Cycles'
import { Config } from './Config'
import * as Data from './Data/Data'
import knex = require('knex')
import { StateHashes } from './Data/State'
import { ReceiptHashes } from './Data/Receipt'
import { SummaryHashes } from './Data/Summary'
import { DataQueryResponse, ReceiptMapResult, socketServer, SummaryBlob } from './Data/Data'
import { Database, BaseModel, FS_Persistence_Adapter } from 'tydb'

let db: knex

export let Collection: any

export async function initStorage (config: Config) {
  // Get db file location from config
  let dbFile = config.ARCHIVER_DB
  // Connect to a db
  db = knex({
    client: 'sqlite3',
    connection: {
      filename: dbFile,
    },
    useNullAsDefault: true
  })

  Collection = new Database<Data.ArchivedCycle>({
    ref: 'archiver-db',
    model: Data.ArchivedCycle,
    persistence_adapter: FS_Persistence_Adapter,
    autoCompaction: 60 * 60 * 1000, // ^ compaction every hour
  })

  await Collection.createIndex({ fieldName: 'cycleMarker', unique: true })

  // Create a cycles table if it doesn't exist
  // TODO: add safetyMode, safetyNum, networkId columns
  if ((await db.schema.hasTable('cycles')) === false) {
    await db.schema.createTable('cycles', table => {
      table.boolean('safetyMode')
      table.bigInteger('safetyNum')
      table.text('networkId')
      table.text('networkStateHash')
      table.json('networkDataHash')
      table.json('networkReceiptHash')
      table.json('networkSummaryHash')
      table.bigInteger('counter')
      table.json('certificate')
      table.text('previous')
      table.text('marker')
      table.bigInteger('start')
      table.bigInteger('duration')
      table.bigInteger('active')
      table.bigInteger('desired')
      table.bigInteger('expired')
      table.bigInteger('syncing')
      table.json('joined')
      table.json('joinedArchivers')
      table.json('joinedConsensors')
      table.json('refreshedArchivers')
      table.json('refreshedConsensors')
      table.json('activated')
      table.json('activatedPublicKeys')
      table.json('removed')
      table.json('returned')
      table.json('lost')
      table.json('refuted')
      table.json('apoptosized')
    })
    console.log('Cycle table created.')
  }

  // TODO: add safetyMode, safetyNum, networkId columns
  if ((await db.schema.hasTable('stateHashes')) === false) {
    await db.schema.createTable('stateHashes', table => {
      table.bigInteger('counter')
      table.json('partitionHashes')
      table.text('networkHash')
    })
    console.log('StateHashes table created.')
  }

  if ((await db.schema.hasTable('summaryHashes')) === false) {
    await db.schema.createTable('summaryHashes', table => {
      table.bigInteger('counter')
      table.json('summaryHashes')
      table.text('networkSummaryHash')
    })
    console.log('SummaryHashes table created.')
  }

  if ((await db.schema.hasTable('receiptHashes')) === false) {
    await db.schema.createTable('receiptHashes', table => {
      table.bigInteger('counter')
      table.json('receiptMapHashes')
      table.text('networkReceiptHash')
    })
    console.log('ReceiptHashes table created.')
  }

  if ((await db.schema.hasTable('receiptMap')) === false) {
    await db.schema.createTable('receiptMap', table => {
      table.bigInteger('cycle')
      table.bigInteger('partition')
      table.json('receiptMap')
      table.bigInteger('txCount')
      table.unique(['cycle', 'partition'])
    })
    console.log('receiptMap table created.')
  }

  if ((await db.schema.hasTable('summaryBlob')) === false) {
    await db.schema.createTable('summaryBlob', table => {
      table.bigInteger('latestCycle')
      table.bigInteger('cycle')
      table.bigInteger('counter')
      table.bigInteger('partition')
      table.bigInteger('errorNull')
      table.json('opaqueBlob')
      table.unique(['cycle', 'partition'])
    })
    console.log('SummaryBlob table created.')
  }
  console.log('Database is initialised.')
}
export async function insertArchivedCycle (archivedCycle: any) {
  try {
    await Collection.insert([Data.ArchivedCycle.new(archivedCycle)])
  } catch (e) {
    console.log('Unable to insert archived cycle')
    console.log(e)
  }
}

export async function updateReceiptMap (
  receiptMapResult: Data.ReceiptMapResult
) {
  if (!receiptMapResult) return
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

    console.log('Existing Archived cycle', existingArchivedCycle)

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

    console.log('newPartitionMaps', newPartitionMaps)

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

    console.log('Existing Archived cycle', existingArchivedCycle)

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

    console.log('newPartitionBlobs', newPartitionBlobs)

    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: { $set: { 'summary.partitionBlobs': newPartitionBlobs } },
    })
  } catch (e) {
    console.log('Unable to update summary blobs in archived cycle')
    console.log(e)
  }
}

export async function queryAllArchivedCycles () {
  let archivedCycles = await Collection.find({
    filter: {},
    project: {
      _id: 0,
      cycleMarker: 0,
      receipt: 0,
      data: 0,
      summary: 0,
    },
  })
  return archivedCycles
}

export async function queryAllCycleRecords () {
  let cycleRecords = await Collection.find({
    filter: {},
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

export async function queryArchivedCycleByMarker (marker: string) {
  let archivedCycles = await Collection.find({
    filter: { cycleMarker: marker },
  })
  if (archivedCycles.length > 0) return archivedCycles[0]
}

function strigifyCycleRecordFields (cycle: Cycle) {
  let stringifiedCycle: Cycle = { ...cycle }
  stringifiedCycle.joined =
    typeof cycle.joined !== 'string'
      ? JSON.stringify(cycle.joined)
      : cycle.joined
  stringifiedCycle.joinedArchivers =
    typeof cycle.joinedArchivers !== 'string'
      ? JSON.stringify(cycle.joinedArchivers)
      : cycle.joinedArchivers
  stringifiedCycle.joinedConsensors =
    typeof cycle.joinedConsensors !== 'string'
      ? JSON.stringify(cycle.joinedConsensors)
      : cycle.joinedConsensors
  stringifiedCycle.refreshedArchivers =
    typeof cycle.refreshedArchivers !== 'string'
      ? JSON.stringify(cycle.refreshedArchivers)
      : cycle.refreshedArchivers
  stringifiedCycle.refreshedConsensors =
    typeof cycle.refreshedConsensors !== 'string'
      ? JSON.stringify(cycle.refreshedConsensors)
      : cycle.refreshedConsensors
  stringifiedCycle.activated =
    typeof cycle.activated !== 'string'
      ? JSON.stringify(cycle.activated)
      : cycle.activated
  stringifiedCycle.activatedPublicKeys =
    typeof cycle.activatedPublicKeys !== 'string'
      ? JSON.stringify(cycle.activatedPublicKeys)
      : cycle.activatedPublicKeys
  stringifiedCycle.removed =
    typeof cycle.removed !== 'string'
      ? JSON.stringify(cycle.removed)
      : cycle.removed
  stringifiedCycle.returned =
    typeof cycle.returned !== 'string'
      ? JSON.stringify(cycle.returned)
      : cycle.returned
  stringifiedCycle.lost =
    typeof cycle.lost !== 'string' ? JSON.stringify(cycle.lost) : cycle.lost
  stringifiedCycle.refuted =
    typeof cycle.refuted !== 'string'
      ? JSON.stringify(cycle.refuted)
      : cycle.refuted
  stringifiedCycle.apoptosized =
    typeof cycle.apoptosized !== 'string'
      ? JSON.stringify(cycle.apoptosized)
      : cycle.apoptosized
  stringifiedCycle.networkDataHash =
      typeof cycle.networkDataHash !== 'string' ? JSON.stringify(cycle.networkDataHash) : cycle.networkDataHash
  stringifiedCycle.networkReceiptHash =
      typeof cycle.networkReceiptHash !== 'string' ? JSON.stringify(cycle.networkReceiptHash) : cycle.networkReceiptHash
  stringifiedCycle.networkSummaryHash =
      typeof cycle.networkSummaryHash !== 'string' ? JSON.stringify(cycle.networkSummaryHash) : cycle.networkSummaryHash
  return stringifiedCycle
}

export async function storeCycles (cycles: Cycle[]) {
  cycles.forEach(async cycle => {
    await db('cycles').insert(strigifyCycleRecordFields(cycle))
  })
}

export async function storeStateHashes (stateHashess: StateHashes[]) {
  stateHashess.forEach(async stateHash => {
      await db('stateHashes').insert(stateHash)
  })
}

export async function storeSummaryHashes (summaryHashes: SummaryHashes) {
  await db('summaryHashes').insert({
    ...summaryHashes,
    summaryHashes: JSON.stringify(summaryHashes.summaryHashes)
  })
}

export async function storeReceiptHashes (receiptHashes: ReceiptHashes) {
  await db('receiptHashes').insert({
    ...receiptHashes,
    receiptMapHashes: JSON.stringify(receiptHashes.receiptMapHashes)
  })
}

export async function storeReceiptMap (receiptMapResult: ReceiptMapResult) {
  try {
    await db('receiptMap').insert({
      ...receiptMapResult,
      receiptMap: JSON.stringify(receiptMapResult.receiptMap)
    })
    socketServer.emit('RECEIPT_MAP', receiptMapResult)
  } catch(e) {

  }
}

export async function storeSummaryBlob (summaryBlob: SummaryBlob, cycle: number) {
  try {
    await db('summaryBlob').insert({
      ...summaryBlob,
      opaqueBlob: JSON.stringify(summaryBlob.opaqueBlob),
      cycle
    })
  } catch(e) {

  }
}

export async function queryAllCycles () {
  let data = await db('cycles').select('*')
  return data
}

export async function queryLatestCycle (count = 1) {
  let data = await db('cycles')
    .select('*')
    .orderBy('counter', 'desc')
    .limit(count)
  return data
}

export async function queryCyclesBetween (start = 0, end = 0) {
  let data = await db('cycles')
    .select('*')
    .where('counter', '>=', start).andWhere('counter', '<=', end)
    .orderBy('counter', 'desc')
  return data
}

export async function queryAllStateHashes () {
  let data = await db('stateHashes').select('*')
  return data
}

export async function queryLatestStateHash (count = 1) {
  let data = await db('stateHashes')
    .select('*')
    .orderBy('counter', 'desc')
    .limit(count)
  return data
}

export async function queryReceiptMapHashesByCycle (cycle: number)  {
  let data = await db('receiptHashes')
    .select('*')
    .where('counter', cycle)
    .orderBy('counter', 'desc')
  if(data.length > 0) return data[0]
}

export async function querySummaryHashesByCycle (cycle: number)  {
  let data = await db('summaryHashes')
    .select('*')
    .where('counter', cycle)
    .orderBy('counter', 'desc')
  if(data.length > 0) return data[0]
}

export async function queryAllReceipts ()  {
  let data = await db('receiptMap')
    .select('*')
    .orderBy('cycle', 'asc')
  if(data.length > 0) return data
  return []
}
