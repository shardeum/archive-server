import { Cycle } from './Data/Cycles'
import knex = require('knex')
import { StateHashes } from './Data/State'
import { ReceiptHashes } from './Data/Receipt'
import { SummaryHashes } from './Data/Summary'
import { DataQueryResponse, ReceiptMapResult, socketServer, SummaryBlob } from './Data/Data'

let db: knex

export async function initStorage (dbFile: string) {
  // Connect to a db
  db = knex({
    client: 'sqlite3',
    connection: {
      filename: dbFile,
    },
  })

  // Create a cycles table if it doesn't exist
  // TODO: add safetyMode, safetyNum, networkId columns
  if ((await db.schema.hasTable('cycles')) === false) {
    await db.schema.createTable('cycles', table => {
      table.boolean('safetyMode')
      table.bigInteger('safetyNum')
      table.text('networkId')
      table.text('networkStateHash')
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
      table.bigInteger('cycle')
      table.bigInteger('partition')
      table.json('blob')
      table.unique(['cycle', 'partition'])
    })
    console.log('SummaryBlob table created.')
  }
}

export async function storeCycle (cycle: Cycle) {
  cycle.joined =
    typeof cycle.joined !== 'string'
      ? JSON.stringify(cycle.joined)
      : cycle.joined
  cycle.joinedArchivers =
    typeof cycle.joinedArchivers !== 'string'
      ? JSON.stringify(cycle.joinedArchivers)
      : cycle.joinedArchivers
  cycle.joinedConsensors =
    typeof cycle.joinedConsensors !== 'string'
      ? JSON.stringify(cycle.joinedConsensors)
      : cycle.joinedConsensors
  cycle.refreshedArchivers =
    typeof cycle.refreshedArchivers !== 'string'
      ? JSON.stringify(cycle.refreshedArchivers)
      : cycle.refreshedArchivers
  cycle.refreshedConsensors =
    typeof cycle.refreshedConsensors !== 'string'
      ? JSON.stringify(cycle.refreshedConsensors)
      : cycle.refreshedConsensors
  cycle.activated =
    typeof cycle.activated !== 'string'
      ? JSON.stringify(cycle.activated)
      : cycle.activated
  cycle.activatedPublicKeys =
    typeof cycle.activatedPublicKeys !== 'string'
      ? JSON.stringify(cycle.activatedPublicKeys)
      : cycle.activatedPublicKeys
  cycle.removed =
    typeof cycle.removed !== 'string'
      ? JSON.stringify(cycle.removed)
      : cycle.removed
  cycle.returned =
    typeof cycle.returned !== 'string'
      ? JSON.stringify(cycle.returned)
      : cycle.returned
  cycle.lost =
    typeof cycle.lost !== 'string' ? JSON.stringify(cycle.lost) : cycle.lost
  cycle.refuted =
    typeof cycle.refuted !== 'string'
      ? JSON.stringify(cycle.refuted)
      : cycle.refuted
  cycle.apoptosized =
    typeof cycle.apoptosized !== 'string'
      ? JSON.stringify(cycle.apoptosized)
      : cycle.apoptosized
  await db('cycles').insert(cycle)
}

export async function storeStateHashes (stateHashes: StateHashes) {
  console.log('Storing state hashes')
  await db('stateHashes').insert({
    ...stateHashes,
    partitionHashes: JSON.stringify(stateHashes.partitionHashes)
  })
}

export async function storeSummaryHashes (summaryHashes: SummaryHashes) {
  console.log('Storing summary hashes')
  await db('summaryHashes').insert({
    ...summaryHashes,
    summaryHashes: JSON.stringify(summaryHashes.summaryHashes)
  })
}

export async function storeReceiptHashes (receiptHashes: ReceiptHashes) {
  console.log('Storing receipt hashes')
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

export async function storeSummaryBlob (summaryBlob: SummaryBlob) {
  try {
    await db('summaryBlob').insert({
      ...summaryBlob,
      blob: JSON.stringify(summaryBlob.blob)
    })
    socketServer.emit('SUMMARY_BLOB', summaryBlob)
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
