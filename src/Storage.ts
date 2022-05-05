import { Cycle, CycleChain } from './Data/Cycles'
import { Config } from './Config'
import * as Data from './Data/Data'
import { socketServer } from './Data/Data'
import { Database, BaseModel, FS_Persistence_Adapter } from 'tydb'
import * as Crypto from './Crypto'
import * as Logger from './Logger'
import { StateManager } from '@shardus/types'

export let Collection: any

export async function initStorage(config: Config) {
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

export async function insertArchivedCycle(archivedCycle: any) {
  Logger.mainLogger.debug(
    'Inserting archived cycle',
    archivedCycle.cycleRecord.counter,
    archivedCycle.cycleMarker
  )
  try {
    await Collection.insert([Data.ArchivedCycle.new(archivedCycle)])
    Logger.mainLogger.debug(
      'Successfully inserted archivedCycle',
      archivedCycle.cycleRecord.counter
    )
    let updatedArchivedCycle = await Collection.find({
      filter: { cycleMarker: archivedCycle.cycleMarker },
      project: {
        _id: 0,
      },
    })
    let signedDataToSend = Crypto.sign({
      archivedCycles: updatedArchivedCycle,
    })
    if (updatedArchivedCycle) {
      if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    }
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
  receiptMapResult: StateManager.StateManagerTypes.ReceiptMapResult | any
) {
  if (!receiptMapResult) return
  try {
    let parentCycle = CycleChain.get(receiptMapResult.cycle)

    if (!parentCycle) {
      Logger.mainLogger.error(
        'Unable find record with parent cycle with counter',
        receiptMapResult.cycle
      )
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(
      parentCycle.marker
    )

    if (!existingArchivedCycle) {
      Logger.mainLogger.error(
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

    let newPartitionTxs: any = {}
    if (
      existingArchivedCycle.receipt &&
      existingArchivedCycle.receipt.partitionTxs
    ) {
      newPartitionTxs = { ...existingArchivedCycle.receipt.partitionTxs }
    }
    if (receiptMapResult.txsMapEVMReceipt) {
      for (let id of Object.keys(receiptMapResult.txsMapEVMReceipt)) {
        receiptMapResult.txsMap[id] = [
          ...receiptMapResult.txsMap[id],
          receiptMapResult.txsMapEVMReceipt[id],
        ]
      }
      newPartitionTxs[receiptMapResult.partition] = receiptMapResult.txsMap
    } else {
      newPartitionTxs[receiptMapResult.partition] = receiptMapResult.txsMap
    }
    console.log('TxsMap', receiptMapResult.txsMap)
    await Collection.update({
      filter: { cycleMarker: parentCycle.marker },
      update: {
        $set: {
          'receipt.partitionMaps': newPartitionMaps,
          'receipt.partitionTxs': newPartitionTxs,
        },
      },
    })
    let updatedArchivedCycle = await Collection.find({
      filter: { cycleMarker: parentCycle.marker },
      project: {
        _id: 0,
      },
    })
    let signedDataToSend = Crypto.sign({
      archivedCycles: updatedArchivedCycle,
    })
    if (updatedArchivedCycle) {
      if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    }
  } catch (e) {
    Logger.mainLogger.error('Unable to update receipt maps in archived cycle')
    Logger.mainLogger.error(e)
  }
}

export async function updateSummaryBlob(
  summaryBlob: StateManager.StateManagerTypes.SummaryBlob,
  cycle: number
) {
  if (!summaryBlob) return
  try {
    let parentCycle = CycleChain.get(cycle)

    if (!parentCycle) {
      Logger.mainLogger.error(
        'Unable find record with parent cycle with counter',
        cycle
      )
      return
    }

    const existingArchivedCycle = await queryArchivedCycleByMarker(
      parentCycle.marker
    )

    if (!existingArchivedCycle) {
      Logger.mainLogger.error(
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
    let updatedArchivedCycle = await Collection.find({
      filter: { cycleMarker: parentCycle.marker },
      project: {
        _id: 0,
      },
    })
    let signedDataToSend = Crypto.sign({
      archivedCycles: updatedArchivedCycle,
    })
    if (updatedArchivedCycle) {
      if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
    }
  } catch (e) {
    Logger.mainLogger.error('Unable to update summary blobs in archived cycle')
    Logger.mainLogger.error(e)
  }
}

export async function updateArchivedCycle(
  marker: string,
  field: string,
  data: any
) {
  let updateObj: any = {}
  updateObj[field] = data
  await Collection.update({
    filter: { cycleMarker: marker },
    update: { $set: updateObj },
  })
  let updatedArchivedCycle = await Collection.find({
    filter: { cycleMarker: marker },
    project: {
      _id: 0,
    },
  })
  let signedDataToSend = Crypto.sign({
    archivedCycles: updatedArchivedCycle,
  })
  if (updatedArchivedCycle) {
    if (socketServer) socketServer.emit('ARCHIVED_CYCLE', signedDataToSend)
  }
}

export async function queryAllArchivedCycles(count?: number) {
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

export async function queryAllArchivedCyclesBetween(
  start: number,
  end: number
) {
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
    limit: end - start,
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

export async function queryAllCycleRecords() {
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

export async function queryLatestCycleRecords(count: number = 1) {
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

export async function queryCycleRecordsBetween(start: number, end: number) {
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

export async function queryArchivedCycleByMarker(marker: string) {
  let archivedCycles = await Collection.find({
    filter: { cycleMarker: marker },
  })
  if (archivedCycles.length > 0) return archivedCycles[0]
}

export async function queryReceiptMapHash(counter: number, partition: number) {
  let foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (
      foundArchivedCycles[0].receipt &&
      foundArchivedCycles[0].receipt.partitionHashes
    ) {
      return foundArchivedCycles[0].receipt.partitionHashes[partition]
    }
  }
}

export async function querySummaryHash(counter: number, partition: number) {
  let foundArchivedCycles = await Collection.find({
    filter: { 'cycleRecord.counter': counter },
  })
  if (foundArchivedCycles.length > 0) {
    if (
      foundArchivedCycles[0].summary &&
      foundArchivedCycles[0].summary.partitionHashes
    ) {
      return foundArchivedCycles[0].summary.partitionHashes[partition]
    }
  }
}
