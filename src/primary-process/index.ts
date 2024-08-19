import type { Cluster, Worker } from 'cluster'
import { cpus } from 'os'
import { ArchiverReceipt } from '../dbstore/receipts'
import { verifyArchiverReceipt, ReceiptVerificationResult } from '../Data/Collector'
import { config } from '../Config'
import { EventEmitter } from 'events'
import { shardValuesByCycle } from '../Data/Cycles'
import { StateManager } from '@shardus/types'

const MAX_WORKERS = cpus().length - 1 // Leaving 1 core for the master process

export interface ChildMessageInterface {
  type: string
  data: {
    receipt?: string
    success?: boolean
    err?: string
    txId?: string
    timestamp?: number
    verificationResult?: ReceiptVerificationResult
    cycle?: number
    shardValues?: StateManager.shardFunctionTypes.CycleShardData
  }
}

export let receivedReceiptCount = 0 // Variable to keep track of the number of receipts received
export let verifiedReceiptCount = 0 // Variable to keep track of the number of receipts verified
export let successReceiptCount = 0 // Variable to keep track of the number of receipts successful verification
export let failureReceiptCount = 0 // Variable to keep track of the number of receipts failed verification

let receiptLoadTraker = 0 // Variable to keep track of the receipt load within the last receiptLoadTrakerInterval
// Creating a worker pool
const workers: Worker[] = []
const extraWorkers = new Map<number, Worker>()
let currentWorker = 0

const emitter = new EventEmitter()

export const setupWorkerProcesses = (cluster: Cluster): void => {
  console.log(`Master ${process.pid} is running`)
  // Set interval to check receipt count every 15 seconds
  setInterval(() => {
    if (receiptLoadTraker < config.receiptLoadTrakerLimit) {
      console.log(`Receipt load is below the limit: ${receiptLoadTraker}/${config.receiptLoadTrakerLimit}`)
      // Kill the extra workers from the end of the array
      for (let i = workers.length - 1; i >= 0; i--) {
        // console.log(`Killing worker ${workers[i].process.pid} with index ${i}`);
        // workers[i].kill();
        // workers.pop();

        // Instead of killing the worker, move it to the extraWorkers map
        const worker = workers.pop()
        if (worker) extraWorkers.set(worker.process.pid, worker)
      }
      receiptLoadTraker = 0
      return
    }
    let neededWorkers = Math.ceil(receiptLoadTraker / config.receiptLoadTrakerLimit)
    if (neededWorkers > MAX_WORKERS) neededWorkers = MAX_WORKERS
    const currentWorkers = workers.length
    console.log(`Needed workers: ${neededWorkers}`, `Current workers: ${currentWorkers}`)
    if (neededWorkers > currentWorkers) {
      for (let i = currentWorkers; i < neededWorkers; i++) {
        const worker = cluster.fork()
        workers.push(worker)
        for (const [cycle, shardValues] of shardValuesByCycle) {
          worker.send({
            type: 'shardValuesByCycle',
            data: { cycle, shardValues },
          })
        }
        // results.set(worker.process.pid, { success: 0, failure: 0 })
        setupWorkerListeners(worker)
      }
    } else if (neededWorkers < currentWorkers) {
      // Kill the extra workers from the end of the array
      for (let i = currentWorkers - 1; i >= neededWorkers; i--) {
        // console.log(`Killing worker ${workers[i].process.pid} with index ${i}`);
        // workers[i].kill();
        // workers.pop();

        // Instead of killing the worker, move it to the extraWorkers map
        const worker = workers.pop()
        if (worker) extraWorkers.set(worker.process.pid, worker)
      }
    }
    console.log(
      `Adjusted worker count to ${workers.length}, based on ${receiptLoadTraker} receipts received.`
    )
    receiptLoadTraker = 0 // Reset the count
  }, config.receiptLoadTrakerInterval)
}

const setupWorkerListeners = (worker: Worker): void => {
  // Handle messages from workers
  const workerId = worker.process.pid
  worker.on('message', ({ type, data }: ChildMessageInterface) => {
    switch (type) {
      case 'receipt-verification': {
        // const result = results.get(workerId)
        // if (result) {
        //   verifiedReceiptCount++
        //   if (data.success) {
        //     result.success++
        //     successReceiptCount++
        //   } else {
        //     result.failure++
        //     failureReceiptCount++
        //   }
        // }
        const { txId, timestamp } = data
        emitter.emit(txId + timestamp, data.verificationResult)
        break
      }
      case 'clild_close':
        console.log(`Worker ${workerId} is requesting to close`)
        // Check if the worker is in the extraWorkers map
        if (extraWorkers.has(workerId)) {
          console.log(`Worker ${workerId} is in extraWorkers, killing it now`)
          const worker = extraWorkers.get(workerId)
          if (worker) worker.kill()
        } else {
          console.error(`Worker ${workerId} is not in extraWorkers`)
          // Check if the worker is in the workers array
          const workerIndex = workers.findIndex((worker) => worker.process.pid === workerId)
          if (workerIndex !== -1) {
            console.error(
              `Worker ${workerId} is in workers now, we can't kill it as it might be processing a receipt`
            )
          } else {
            console.error(`Worker ${workerId} is not in workers`)
          }
        }
        break
      default:
        console.log(`Worker ${process.pid} is sending unknown message type: ${type}`)
        console.log(data)
        break
    }
  })

  worker.on('exit', (code, signal) => {
    console.log(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`)
    let isExtraWorker = false
    if (extraWorkers.has(workerId)) {
      console.log(`Worker ${workerId} is in extraWorkers, removing it now`)
      isExtraWorker = true
      extraWorkers.get(workerId)?.kill()
      extraWorkers.delete(workerId)
    }
    // Remove the worker from the workers list if not present in extraWorkers
    const workerIndex = workers.findIndex((worker) => worker.process.pid === workerId)
    if (workerIndex !== -1) {
      if (isExtraWorker) {
        console.error(`Worker ${workerId} is in workers list as well`)
      }
      workers[workerIndex]?.kill()
      workers.splice(workerIndex, 1)
    } else {
      if (!isExtraWorker) console.error(`Worker ${workerId} is not in workers list`)
    }
  })
}

const forwardReceiptVerificationResult = (
  txId: string,
  timestamp: number,
  worker: Worker
): Promise<ReceiptVerificationResult> => {
  return new Promise((resolve) => {
    emitter.on(txId + timestamp, (result: ReceiptVerificationResult) => {
      resolve(result)
    })
    worker.on('exit', () => {
      resolve({
        success: false,
        failedReasons: [
          `Worker exited before sending the receipt verification result for ${txId} with timestamp ${timestamp}`,
        ],
        nestedCounterMessages: ['Worker exited before sending the receipt verification result'],
      })
    })
  })
}

export const offloadReceipt = async (
  txId: string,
  timestamp: number,
  receipt: string,
  receipt2: ArchiverReceipt
): Promise<ReceiptVerificationResult> => {
  receivedReceiptCount++ // Increment the counter for each receipt received
  receiptLoadTraker++ // Increment the receipt load tracker
  let verificationResult: ReceiptVerificationResult
  if (workers.length === 0) {
    verificationResult = await verifyArchiverReceipt(receipt2)
  } else {
    // Forward the request to a worker in a round-robin fashion
    let worker = workers[currentWorker]
    currentWorker = (currentWorker + 1) % workers.length
    if (!worker) {
      console.error('No worker available to process the receipt 1')
      worker = workers[currentWorker]
      currentWorker = (currentWorker + 1) % workers.length
      if (worker) {
        worker.send({
          type: 'receipt-verification',
          data: { receipt },
        })
        verificationResult = await forwardReceiptVerificationResult(txId, timestamp, worker)
      } else {
        console.error('No worker available to process the receipt 2')
        // Verifying the receipt in the main thread
        verificationResult = await verifyArchiverReceipt(receipt2)
      }
    } else {
      worker.send({
        type: 'receipt-verification',
        data: { receipt },
      })
      verificationResult = await forwardReceiptVerificationResult(txId, timestamp, worker)
    }
  }
  verifiedReceiptCount++
  if (verificationResult.success) {
    successReceiptCount++
  } else {
    failureReceiptCount++
  }
  return verificationResult
}
