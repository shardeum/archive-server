import { verifyArchiverReceipt, ReceiptVerificationResult } from '../Data/Collector'
import { ChildMessageInterface } from '../primary-process'
import { config } from '../Config'
import { Utils as StringUtils } from '@shardus/types'
import { ArchiverReceipt } from '../dbstore/receipts'

export const initWorkerProcess = async (): Promise<void> => {
  console.log(`Worker ${process.pid} started`)
  let lastActivity = Date.now()

  // Worker processes
  process.on('message', async ({ type, data }: ChildMessageInterface) => {
    switch (type) {
      case 'receipt-verification': {
        if (!data.stringifiedReceipt) {
          console.error(`Worker ${process.pid} received invalid receipt for verification`, data)
          return
        }
        if (isNaN(data.requiredSignatures)) {
          console.error(`Worker ${process.pid} received invalid requiredSignatures for verification`, data)
          return
        }
        const receipt = StringUtils.safeJsonParse(data.stringifiedReceipt) as ArchiverReceipt
        // console.log(`Worker ${process.pid} verifying receipt`);
        let verificationResult: ReceiptVerificationResult = {
          success: false,
          failedReasons: [],
          nestedCounterMessages: [],
        }
        try {
          console.log(`Worker process ${process.pid} is verifying receipt`, receipt.tx.txId, receipt.tx.timestamp)
          verificationResult = await verifyArchiverReceipt(receipt, data.requiredSignatures)
        } catch (error) {
          console.error(`Error in Worker ${process.pid} while verifying receipt`, error)
          verificationResult.failedReasons.push('Error in Worker while verifying receipt')
          verificationResult.nestedCounterMessages.push('Error in Worker while verifying receipt')
        }
        process.send({
          type: 'receipt-verification',
          data: {
            txId: receipt.tx.txId,
            timestamp: receipt.tx.timestamp,
            verificationResult,
          },
        })
        break
      }
      default:
        console.log(`Worker ${process.pid} received unknown message type: ${type}`)
        console.log(data)
        break
    }
    lastActivity = Date.now()
  })
  process.send({ type: 'child_ready' })
  setInterval(() => {
    console.log(
      `lastActivityCheckTimeout: ${config.lastActivityCheckTimeout}, lastActivityCheckInterval: ${config.lastActivityCheckInterval}`
    )
    if (Date.now() - lastActivity > config.lastActivityCheckTimeout) {
      console.log(`Worker ${process.pid} is idle for more than 1 minute`)
      process.send({ type: 'child_close' })
    }
  }, config.lastActivityCheckInterval)
}

process.on('uncaughtException', (error) => {
  console.error(`Uncaught Exception in Child Process: ${process.pid}`, error)
})
