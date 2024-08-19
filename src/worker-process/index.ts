import { verifyArchiverReceipt, ReceiptVerificationResult } from '../Data/Collector'
import { ChildMessageInterface } from '../primary-process'

export const initWorkerProcess = async (): Promise<void> => {
  console.log(`Worker ${process.pid} started`)
  let lastActivity = Date.now()
  const lastActivityCheckInterval: number = 15 * 1000 // Interval to check last activity
  const lastActivityCheckTimeout: number = 30 * 1000 // Timeout to check last activity

  // Worker processes
  process.on('message', async ({ type, data }: ChildMessageInterface) => {
    switch (type) {
      case 'receipt-verification': {
        if (!data.receipt) {
          console.error(`Worker ${process.pid} received invalid receipt for verification`, data)
          return
        }
        // console.log(`Worker ${process.pid} verifying receipt`);
        let verificationResult: ReceiptVerificationResult = { success: false }
        try {
          verificationResult = await verifyArchiverReceipt(data.receipt)
        } catch (error) {
          console.error(`Error in Worker ${process.pid} while verifying receipt`, error)
          verificationResult.failedReason = 'Error in Worker while verifying receipt'
          verificationResult.nestedCounterMessage = 'Error in Worker while verifying receipt'
        }
        process.send({
          type: 'receipt-verification',
          data: {
            txId: data.receipt.tx.txId,
            timestamp: data.receipt.tx.timestamp,
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
  setInterval(() => {
    if (Date.now() - lastActivity > lastActivityCheckTimeout) {
      console.log(`Worker ${process.pid} is idle for more than 1 minute`)
      process.send({ type: 'clild_close' })
    }
  }, lastActivityCheckInterval)
}

process.on('uncaughtException', (error) => {
  console.error(`Uncaught Exception in Child Process: ${process.pid}`, error)
})
