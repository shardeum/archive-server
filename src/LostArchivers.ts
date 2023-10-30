import * as CycleDB from './dbstore/cycles'
import * as Cycles from './Data/Cycles'
import * as Logger from './Logger'
import * as NodeList from './NodeList'
import { ArchiverRefutesLostMsg, Record } from "@shardus/types/build/src/p2p/LostArchiverTypes";
import { config } from "./Config";
import { calcIncomingTimes } from './Data/Data';
import { postJson } from './P2P';

let shouldSendRefutes = false

export function handleLostArchivers<R extends Record>(record: R): void {
  if (record.refutedArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'refutedArchivers' field, stop sending refutes
    shouldSendRefutes = false
  } else if (record.lostArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'lostArchivers' field, schedule a refute in the next cycle's Q1
    shouldSendRefutes = true
    scheduleRefute()
  } else if (record.removedArchivers.some((publicKey) => publicKey === config.ARCHIVER_PUBLIC_KEY)) {
    // if self is in 'removedArchivers' field, shut down
    die()
  }
}

/// Schedules to send a refute during the next cycle's Q1
async function scheduleRefute(): Promise<void> {
  if (!shouldSendRefutes) return

  const latestCycleInfo = await CycleDB.queryLatestCycleRecords(1)
  const latestCycle = latestCycleInfo[0]
  const { startQ1 } = calcIncomingTimes(latestCycle)

  // ms until q1. add 500ms to make sure we're in q1
  const delay = startQ1 - Date.now() + 500
  setTimeout(sendRefute, delay)
}

async function sendRefute(): Promise<void> {
  if (!shouldSendRefutes) return

  const nodes = NodeList.getRandomActiveNodes(5)
  for (const node of nodes) {
    try {
      await postJson(`http://${node.ip}:${node.port}/lost-archiver-refute`, <ArchiverRefutesLostMsg>{
        archiver: config.ARCHIVER_PUBLIC_KEY,
        cycle: Cycles.getCurrentCycleMarker(),
      })
    } catch (e) {
      Logger.mainLogger.warn(`Failed to send refute to ${node.ip}:${node.port}:`, e)
    }
  }

  scheduleRefute()
}

function die(): void {
  Logger.mainLogger.debug('Archiver was found in `removedArchivers` and will exit now without sending a leave request')
  process.exit(2)
}
