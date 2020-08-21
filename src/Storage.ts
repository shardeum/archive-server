import { Cycle } from './Data/Cycles'
import knex = require('knex')
import { StateHashes } from './Data/State'

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
