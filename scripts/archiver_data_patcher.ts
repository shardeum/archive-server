import * as dbstore from '../src/dbstore'
import { config } from '../src/Config'
import { ArchiverNodeInfo } from '../src/State'
import { getJson } from '../src/P2P'
import * as ReceiptDB from '../src/dbstore/receipts'
import * as OriginalTxsDataDB from '../src/dbstore/originalTxsData'
import { storeReceiptData, storeOriginalTxData } from '../src/Data/Collector'
import { DataType } from '../src/Data/GossipData'

export type ArchiverNode = Omit<ArchiverNodeInfo, 'publicKey' | 'curvePk'>

// Set startCycle and endCycle to the cycle range you want to patch data for
const startCycle = 0
const endCycle = 0

// Set the active archivers from which the data can be pulled and patched
// Note: Specifying one archiver that has the complete data is enough, if multiple archivers are specified, it will pull data from all of them and patch.
const archivers: ArchiverNode[] = [
  {
    ip: '127.0.0.1',
    port: 4000,
  },
  {
    ip: '127.0.0.1',
    port: 4001,
  },
]

const runProgram = async (): Promise<void> => {
  await dbstore.initializeDB(config)

  const bucketSize = 100
  for (const archiver of archivers) {
    // Receipts
    let nextEnd = startCycle + bucketSize
    for (let i = startCycle; i <= endCycle; i++) {
      const downloadedReceiptCountByCycles = await fetchDataCountByCycles(
        archiver,
        DataType.RECEIPT,
        i,
        nextEnd
      )
      // console.log(downloadedReceiptCountByCycles)
      if (!downloadedReceiptCountByCycles || !downloadedReceiptCountByCycles.receipts) {
        console.log(`archiver ${archiver.ip}:${archiver.port} failed to respond`)
        break
      }
      const receiptsCountByCycles = await ReceiptDB.queryReceiptCountByCycles(i, nextEnd)
      // console.log(receiptsCountByCycles)
      for (let j = i; j <= nextEnd; j++) {
        const downloadedReceipts = downloadedReceiptCountByCycles.receipts.filter((d) => d.cycle === j)
        const existingReceipts = receiptsCountByCycles.filter((d) => d.cycle === j)
        // console.log(j,downloadedReceipts, existingReceipts)
        if (JSON.stringify(downloadedReceipts) !== JSON.stringify(existingReceipts)) {
          console.log(j, downloadedReceipts, existingReceipts)
          const receipts = await fetchDataForCycle(archiver, DataType.RECEIPT, j)
          if (receipts) {
            console.log(receipts.receipts?.length)
            await storeReceiptData(receipts.receipts, '', false)
          }
        }
      }
      i = nextEnd + 1
      nextEnd += bucketSize
      if (nextEnd > endCycle) nextEnd = endCycle
    }

    // OriginalTxsData
    nextEnd = startCycle + bucketSize
    for (let i = startCycle; i <= endCycle; i++) {
      const downloadedOriginalTxsDataCountByCycles = await fetchDataCountByCycles(
        archiver,
        DataType.ORIGINAL_TX_DATA,
        i,
        nextEnd
      )
      // console.log(downloadedOriginalTxsDataCountByCycles)
      if (!downloadedOriginalTxsDataCountByCycles || !downloadedOriginalTxsDataCountByCycles.originalTxs) {
        console.log(`archiver ${archiver.ip}:${archiver.port} failed to respond`)
        break
      }
      const originalTxsDataCountByCycles = await OriginalTxsDataDB.queryOriginalTxDataCountByCycles(
        i,
        nextEnd
      )
      // console.log(originalTxsDataCountByCycles)
      for (let j = i; j <= nextEnd; j++) {
        const downloadedOriginalTxsData = downloadedOriginalTxsDataCountByCycles.originalTxsData.filter(
          (d) => d.cycle === j
        )
        const existingOriginalTxsData = originalTxsDataCountByCycles.filter((d) => d.cycle === j)
        // console.log(j,downloadedOriginalTxsData, existingOriginalTxsData)
        if (JSON.stringify(downloadedOriginalTxsData) !== JSON.stringify(existingOriginalTxsData)) {
          console.log(j, downloadedOriginalTxsData, existingOriginalTxsData)
          const originalTxsData = await fetchDataForCycle(archiver, DataType.ORIGINAL_TX_DATA, j)
          if (originalTxsData) {
            console.log(originalTxsData.originalTxs?.length)
            await storeOriginalTxData(originalTxsData.originalTxs, '', false)
          }
        }
      }
      i = nextEnd + 1
      nextEnd += bucketSize
      if (nextEnd > endCycle) nextEnd = endCycle
    }

    // Cycle
    // TODO: Add cycle data patching
  }
}

const fetchDataCountByCycles = async (
  archiver: ArchiverNode,
  dataType: DataType,
  startCycle: number,
  endCycle: number
): Promise<any> => {
  const route = dataType === DataType.RECEIPT ? 'receipt' : 'originalTx'
  return await getJson(
    `http://${archiver.ip}:${archiver.port}/${route}?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`
  )
}

// TODO: Update as pagination query
const fetchDataForCycle = async (
  archiver: ArchiverNode,
  dataType: DataType,
  cycleNumber: number
): Promise<any> => {
  const route = dataType === DataType.RECEIPT ? 'receipt' : 'originalTx'
  return await getJson(
    `http://${archiver.ip}:${archiver.port}/${route}?startCycle=${cycleNumber}&endCycle=${cycleNumber}`
  )
}

runProgram()
