import * as CycleDB from './dbstore/cycles'
import * as Cycles from './Data/Cycles'
import * as Logger from './Logger'
import * as NodeList from './NodeList'
import { ArchiverRefutesLostMsg, Record } from '@shardus/types/build/src/p2p/LostArchiverTypes'
import { config } from './Config'
import { calcIncomingTimes } from './Data/Data'
import { postJson } from './P2P'
import { sign } from './Crypto'
import { SignedObject } from '@shardus/types/build/src/p2p/P2PTypes'

let shouldSendRefutes = false

/**
 * Checks for the existence of our own public key in either the
 * 'refutedArchivers', 'lostArchivers', or 'removedArchivers' fields of the
 * supplied record.
 * If found in 'refutedArchivers', we'll stop sending refutes.
 * If found in 'lostArchivers', we'll schedule a refute in the next cycle's Q1.
 * If found in 'removedArchivers', we'll shut down.
 */
export function handleLostArchivers<R extends Record>(record: R): void {
  const debug = (...args: unknown[]): void => Logger.mainLogger.debug(...args)
  debug('>> handleLostArchivers()')
  debug('  config.ARCHIVER_PUBLIC_KEY: ' + config.ARCHIVER_PUBLIC_KEY)
  // debug('  record: ' + JSON.stringify(record, null, 2))

  if (record.refutedArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'refutedArchivers' field, stop sending refutes
    debug('archiver was found in `refutedArchivers` and will stop sending refutes')
    shouldSendRefutes = false
  } else if (record.lostArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'lostArchivers' field, schedule a refute in the next cycle's Q1
    debug("archiver was found in `lostArchivers` and will send a refute in the next cycle's Q1")
    shouldSendRefutes = true
    scheduleRefute()
  } else if (record.removedArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'removedArchivers' field, shut down
    debug('archiver was found in `removedArchivers`, shutting down')
    die()
  }
  debug('<< handleLostArchivers()')
}

/**
 * Schedules to send a refute during the next cycle's Q1.
 */
async function scheduleRefute(): Promise<void> {
  if (!shouldSendRefutes) {
    console.log('skipping refute scheduling')
    return
  }

  console.log('scheduling refute')

  const latestCycleInfo = await CycleDB.queryLatestCycleRecords(1)
  const latestCycle = latestCycleInfo[0]
  const { quarterDuration, startQ1 } = calcIncomingTimes(latestCycle)

  // ms until q1. add 500ms to make sure we're in q1
  const delay = (startQ1 + 4 * quarterDuration) - Date.now() + 500
  console.log(delay)
  setTimeout(sendRefute, delay)
}

/**
 * Sends a refute to 5 random active nodes.
 */
async function sendRefute(): Promise<void> {
  if (!shouldSendRefutes) {
    console.log('skipping refute sending')
    return
  }

  console.log('sending refute')

  const refuteMsg: SignedObject<ArchiverRefutesLostMsg> = sign({
    archiver: config.ARCHIVER_PUBLIC_KEY,
    cycle: Cycles.getCurrentCycleMarker(),
  })

  const nodes = NodeList.getRandomActiveNodes(5)

  for (const node of nodes) {
    try {
      await postJson(`http://${node.ip}:${node.port}/lost-archiver-refute`, refuteMsg)
    } catch (e) {
      Logger.mainLogger.warn(`Failed to send refute to ${node.ip}:${node.port}:`, e)
      scheduleRefute()
    }
  }
}

/**
 * Shuts down the archiver with exit code 2.
 */
function die(): void {
  Logger.mainLogger.debug(
    'Archiver was found in `removedArchivers` and will exit now without sending a leave request'
  )
  process.exit(2)
}
