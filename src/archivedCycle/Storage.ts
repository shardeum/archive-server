import { CycleChain } from '../Data/Cycles'
import { Config } from '../Config'
import * as StateMetaData from './StateMetaData'
import { Database, FS_Persistence_Adapter } from 'tydb'
import * as Logger from '../Logger'
import { StateManager } from '@shardus/types'
import { ArchivedCycle } from './StateMetaData'
import { P2P } from '@shardus/types'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export let Collection: Database<any>

interface CycleRecordDocument {
  cycleMarker?: string
  counter?: number
  cycleRecord: P2P.CycleCreatorTypes.CycleRecord
}

export async function initStorage(config: Config): Promise<void> {
  // Get db file location from config
  const dbFile = config.ARCHIVER_DB

  Collection = new Database<StateMetaData.ArchivedCycle>({
    ref: dbFile,
    model: StateMetaData.ArchivedCycle,
    persistence_adapter: FS_Persistence_Adapter,
    autoCompaction: 10 * 30 * 1000, // database compaction every 10 cycles
  })
  await Collection.createIndex({ fieldName: 'cycleMarker', unique: true })
}

export async function insertArchivedCycle(archivedCycle: StateMetaData.ArchivedCycle): Promise<void> {
  Logger.mainLogger.debug(
    'Inserting archived cycle',
    archivedCycle.cycleRecord.counter,
    archivedCycle.cycleMarker
  )
  try {
    await Collection.insert([StateMetaData.ArchivedCycle.new(archivedCycle)])
    Logger.mainLogger.debug('Successfully inserted archivedCycle', archivedCycle.cycleRecord.counter)
    // let updatedArchivedCycle = await Collection.find({
    //   filter: { cycleMarker: archivedCycle.cycleMarker },
    //   project: {
    //     _id: 0,
    //   },
    // })
    // let signedDataToSend = Crypto.sign({
    //   archivedCycles: updatedArchivedCycle,
    // })
    // if (updatedArchivedCycle) {
    //   if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    // }
  } catch (e) {
    Logger.mainLogger.error(e)
    Logger.mainLogger.error(
      'Unable to insert archive cycle or it is already stored in to database',
      archivedCycle.cycleRecord.counter,
      archivedCycle.cycleMarker
    )
  }
}

export async function updateReceiptMap(
  receiptMapResult: StateManager.StateManagerTypes.ReceiptMapResult
): Promise<void> {
  if (!receiptMapResult) return
  try {
    const parentCycle = CycleChain.get(receiptMapResult.cycle)

    if (!parentCycle) {
      Logger.mainLogger.error('Unable find record with parent cycle with counter', receiptMapResult.cycle)
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(parentCycle.marker)

    if (!existingArchivedCycle) {
      Logger.mainLogger.error('Unable find existing archived cycle with marker', parentCycle.marker)
      return
    }

    let newPartitionMaps: Record<string, unknown> = {}
    if (existingArchivedCycle.receipt && existingArchivedCycle.receipt.partitionMaps) {
      newPartitionMaps = { ...existingArchivedCycle.receipt.partitionMaps }
    }

    newPartitionMaps[receiptMapResult.partition] = receiptMapResult.receiptMap

    let newPartitionTxs: Record<string, unknown> = {}
    if (existingArchivedCycle.receipt && existingArchivedCycle.receipt.partitionTxs) {
      newPartitionTxs = { ...existingArchivedCycle.receipt.partitionTxs }
    }
    if (receiptMapResult.txsMapEVMReceipt) {
      for (const id of Object.keys(receiptMapResult.txsMapEVMReceipt)) {
        /* eslint-disable security/detect-object-injection */
        receiptMapResult.txsMap[id] = [...receiptMapResult.txsMap[id], receiptMapResult.txsMapEVMReceipt[id]]
      }
      newPartitionTxs[receiptMapResult.partition] = receiptMapResult.txsMap
    } else {
      newPartitionTxs[receiptMapResult.partition] = receiptMapResult.txsMap
    }
    // console.log('TxsMap', receiptMapResult.txsMap)
    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: {
        $set: {
          'receipt.partitionMaps': newPartitionMaps,
          'receipt.partitionTxs': newPartitionTxs,
        },
      },
    })
    // let updatedArchivedCycle = await Collection.find({
    //   filter: { cycleMarker: parentCycle.marker },
    //   project: {
    //     _id: 0,
    //   },
    // })
    // let signedDataToSend = Crypto.sign({
    //   archivedCycles: updatedArchivedCycle,
    // })
    // if (updatedArchivedCycle) {
    //   if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    // }
  } catch (e) {
    Logger.mainLogger.error('Unable to update receipt maps in archived cycle')
    Logger.mainLogger.error(e)
  }
}

export async function updateSummaryBlob(
  summaryBlob: StateManager.StateManagerTypes.SummaryBlob,
  cycle: number
): Promise<void> {
  if (!summaryBlob) return
  try {
    const parentCycle = CycleChain.get(cycle)

    if (!parentCycle) {
      Logger.mainLogger.error('Unable find record with parent cycle with counter', cycle)
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(parentCycle.marker)

    if (!existingArchivedCycle) {
      Logger.mainLogger.error('Unable find existing archived cycle with marker', parentCycle.marker)
      return
    }

    let newPartitionBlobs: Record<string, unknown> = {}
    if (existingArchivedCycle.summary && existingArchivedCycle.summary.partitionBlobs) {
      newPartitionBlobs = { ...existingArchivedCycle.summary.partitionBlobs }
    }

    newPartitionBlobs[summaryBlob.partition] = summaryBlob

    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: { $set: { 'summary.partitionBlobs': newPartitionBlobs } },
    })
    // let updatedArchivedCycle = await Collection.find({
    //   filter: { cycleMarker: parentCycle.marker },
    //   project: {
    //     _id: 0,
    //   },
    // })
    // let signedDataToSend = Crypto.sign({
    //   archivedCycles: updatedArchivedCycle,
    // })
    // if (updatedArchivedCycle) {
    //   if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    // }
  } catch (e) {
    Logger.mainLogger.error('Unable to update summary blobs in archived cycle')
    Logger.mainLogger.error(e)
  }
}

export async function updateArchivedCycle(marker: string, field: string, data: unknown): Promise<void> {
  const updateObj: Record<string, unknown> = {}
  updateObj[field] = data
  await Collection.update({
    filter: { cycleMarker: marker },
    update: { $set: updateObj },
  })
  // let updatedArchivedCycle = await Collection.find({
  //   filter: { cycleMarker: marker },
  //   project: {
  //     _id: 0,
  //   },
  // })
  // let signedDataToSend = Crypto.sign({
  //   archivedCycles: updatedArchivedCycle,
  // })
  // if (updatedArchivedCycle) {
  //   if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
  // }
}

export async function queryAllArchivedCycles(count?: number): Promise<ArchivedCycle[]> {
  const archivedCycles = await Collection.find({
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

export async function queryAllArchivedCyclesBetween(start: number, end: number): Promise<ArchivedCycle[]> {
  const archivedCycles = await Collection.find({
    filter: {
      $and: [{ 'cycleRecord.counter': { $gte: start } }, { 'cycleRecord.counter': { $lte: end } }],
    },
    sort: {
      'cycleRecord.counter': -1,
    },
    limit: end - start + 1,
    project: {
      _id: 0,
    },
  })

  // let cycleRecords = await Collection.find({
  //   filter: {
  //     $and: [
  //       { 'cycleRecord.counter': { $gte: start } },
  //       { 'cycleRecord.counter': { $lte: end } },
  //     ],
  //   },
  //   sort: {
  //     'cycleRecord.counter': -1,
  //   },
  // })

  return archivedCycles
}

export async function queryAllCycleRecords(): Promise<P2P.CycleCreatorTypes.CycleRecord[]> {
  const cycleRecords = await Collection.find({
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
  return cycleRecords.map((item: CycleRecordDocument) => item.cycleRecord)
}

export async function queryLatestCycleRecords(count = 1): Promise<P2P.CycleCreatorTypes.CycleRecord[]> {
  const cycleRecords = await Collection.find({
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
  return cycleRecords.map((item: CycleRecordDocument) => item.cycleRecord)
}

export async function queryCycleRecordsBetween(
  start: number,
  end: number
): Promise<P2P.CycleCreatorTypes.CycleRecord[]> {
  const cycleRecords = await Collection.find({
    filter: {
      $and: [{ 'cycleRecord.counter': { $gte: start } }, { 'cycleRecord.counter': { $lte: end } }],
    },
    sort: {
      'cycleRecord.counter': -1,
    },
  })
  return cycleRecords.map((item: CycleRecordDocument) => item.cycleRecord)
}

export async function queryArchivedCycleByMarker(marker: string): Promise<ArchivedCycle | undefined> {
  const archivedCycles = await Collection.find({
    filter: { cycleMarker: marker },
  })
  if (archivedCycles.length > 0) return archivedCycles[0]
  return undefined
}

export async function queryReceiptMapHash(counter: number, partition: number): Promise<string | undefined> {
  const foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (foundArchivedCycles[0].receipt && foundArchivedCycles[0].receipt.partitionHashes) {
      return foundArchivedCycles[0].receipt.partitionHashes[partition]
    }
  }
  return undefined
}

export async function querySummaryHash(counter: number, partition: number): Promise<string | undefined> {
  const foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (foundArchivedCycles[0].summary && foundArchivedCycles[0].summary.partitionHashes) {
      return foundArchivedCycles[0].summary.partitionHashes[partition]
    }
  }
  return undefined
}
