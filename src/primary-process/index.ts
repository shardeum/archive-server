import type { Cluster, Worker } from 'cluster'
import { cpus } from 'os'
import { ArchiverReceipt } from '../dbstore/receipts'
import { verifyArchiverReceipt, ReceiptVerificationResult } from '../Data/Collector'
import { config } from '../Config'
import { EventEmitter } from 'events'
import { StateManager, Utils as StringUtils } from '@shardus/types'
import * as Utils from '../Utils'

const MAX_WORKERS = cpus().length - 1 // Leaving 1 core for the master process

export interface ChildMessageInterface {
  type: string
  data: {
    stringifiedReceipt?: string
    requiredSignatures?: number
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
let mainProcessReceiptTracker = 0 // receipt tracker for the receipts getting verified in the main process
// Creating a worker pool
const workers: Worker[] = []
const newWorkers = new Map<number, Worker>()
const extraWorkers = new Map<number, Worker>()
let currentWorker = 0

const emitter = new EventEmitter()

export const setupWorkerProcesses = (cluster: Cluster): void => {
  console.log(`Master ${process.pid} is running`)
  // Set interval to check receipt count every 15 seconds
  setInterval(async () => {
    for (const [, worker] of newWorkers) {
      worker.kill()
    }
    if (receiptLoadTraker < config.receiptLoadTrakerLimit) {
      if (config.workerProcessesDebugLog)
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
    } else {
      let neededWorkers = Math.ceil(receiptLoadTraker / config.receiptLoadTrakerLimit)
      if (neededWorkers > MAX_WORKERS) neededWorkers = MAX_WORKERS
      let currentWorkers = workers.length
      if (config.workerProcessesDebugLog)
        console.log(`Needed workers: ${neededWorkers}`, `Current workers: ${currentWorkers}`)
      if (neededWorkers > currentWorkers) {
        if (extraWorkers.size > 0) {
          if (config.workerProcessesDebugLog)
            console.log(`Extra workers available: ${extraWorkers.size}, moving them to workers list`)
          // Move the extra workers to the workers list
          for (const [pid, worker] of extraWorkers) {
            workers.push(worker)
            extraWorkers.delete(pid)
          }
          currentWorkers = workers.length
        }
        for (let i = currentWorkers; i < neededWorkers; i++) {
          const worker = cluster.fork()
          newWorkers.set(worker.process.pid, worker)
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
    }
    if (config.workerProcessesDebugLog)
      console.log(
        `Adjusted worker count to ${
          workers.length + newWorkers.size
        }, based on ${receiptLoadTraker} receipts received.`
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
        // console.log('receipt-verification', txId + timestamp)
        emitter.emit(txId + timestamp, data.verificationResult)
        break
      }
      case 'child_close':
        if (config.workerProcessesDebugLog) console.log(`Worker ${workerId} is requesting to close`)
        // Check if the worker is in the extraWorkers map
        if (extraWorkers.has(workerId)) {
          if (config.workerProcessesDebugLog)
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
      case 'child_ready':
        if (config.workerProcessesDebugLog) console.log(`Worker ${workerId} is ready for the duty`)
        // Check if the worker is in the newWorkers map
        if (newWorkers.has(workerId)) {
          console.log(`Worker ${workerId} is in newWorkers, moving it to the workers list`)
          workers.push(newWorkers.get(workerId))
          newWorkers.delete(workerId)
        } else {
          console.error(`Worker ${workerId}is not in the newWorkers list`)
        }
        break
      default:
        if (type && typeof type === 'string' && type.includes('axm')) {
          if (config.VERBOSE) {
            console.log(`Worker ${workerId} is sending axm message: ${type}`)
            console.log(data)
          }
          break
        }
        console.log(`Worker ${workerId} is sending unknown message type: ${type}`)
        console.log(data)
        break
    }
  })

  worker.on('exit', (code, signal) => {
    console.log(`Worker ${worker.process.pid} died with code ${code} and signal ${signal}`)
    let isExtraWorker = false
    if (extraWorkers.has(workerId)) {
      if (config.workerProcessesDebugLog)
        console.log(`Worker ${workerId} is in extraWorkers, removing it now`)
      isExtraWorker = true
      extraWorkers.get(workerId)?.kill()
      extraWorkers.delete(workerId)
    }
    let isNewWorker = false
    if (newWorkers.has(workerId)) {
      console.log(`Worker ${workerId} is in newWorkers, removing it now`)
      isNewWorker = true
      newWorkers.get(workerId)?.kill()
      newWorkers.delete(workerId)
    }
    // Remove the worker from the workers list if not present in extraWorkers
    const workerIndex = workers.findIndex((worker) => worker.process.pid === workerId)
    if (workerIndex !== -1) {
      if (isExtraWorker || isNewWorker) console.log(`Worker ${workerId} is in workers list as well`)
      workers[workerIndex]?.kill()
      workers.splice(workerIndex, 1)
    } else {
      if (!isExtraWorker && !isNewWorker) console.error(`Worker ${workerId} is not in workers list`)
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
      // console.log('forwardReceiptVerificationResult', txId, timestamp)
      resolve(result)
    })
    worker.on('exit', () => {
      resolve({
        success: false,
        failedReasons: [
          `Worker ${worker.process.pid} exited before sending the receipt verification result for ${txId} with timestamp ${timestamp}`,
        ],
        nestedCounterMessages: ['Worker exited before sending the receipt verification result'],
      })
    })
  })
}

export const offloadReceipt = async (
  txId: string,
  timestamp: number,
  requiredSignatures: number,
  receipt: ArchiverReceipt
): Promise<ReceiptVerificationResult> => {
  receivedReceiptCount++ // Increment the counter for each receipt received
  receiptLoadTraker++ // Increment the receipt load tracker
  let verificationResult: ReceiptVerificationResult

  // Check if offloading is disabled globally or for global modifications
  if (
    config.disableOffloadReceipt ||
    (config.disableOffloadReceiptForGlobalModification && receipt.globalModification)
  ) {
    mainProcessReceiptTracker++
    if (config.workerProcessesDebugLog) console.log('Verifying on the main program', txId, timestamp)
    verificationResult = await verifyArchiverReceipt(receipt, requiredSignatures)
    mainProcessReceiptTracker--
    verifiedReceiptCount++
    if (verificationResult.success) {
      successReceiptCount++
    } else {
      failureReceiptCount++
    }
    return verificationResult
  }

  // Existing logic for offloading
  if (workers.length === 0 && mainProcessReceiptTracker > config.receiptLoadTrakerLimit) {
    // If there are extra workers available, put them to the workers list
    if (extraWorkers.size > 0) {
      console.log(
        `offloadReceipt - Extra workers available: ${extraWorkers.size}, moving them to workers list`
      )
      // Move the extra workers to the workers list
      for (const [pid, worker] of extraWorkers) {
        workers.push(worker)
        extraWorkers.delete(pid)
      }
    }
    // // If there are still no workers available, add randon wait time (0-1 second) and proceed
    // if (workers.length === 0 && mainProcessReceiptTracker > config.receiptLoadTrakerLimit) {
    //   await Utils.sleep(Math.floor(Math.random() * 1000))
    // }
  }
  if (workers.length === 0) {
    mainProcessReceiptTracker++
    if (config.workerProcessesDebugLog) console.log('Verifying on the main program 1', txId, timestamp)
    verificationResult = await verifyArchiverReceipt(receipt, requiredSignatures)
    mainProcessReceiptTracker--
  } else {
    mainProcessReceiptTracker = 0
    // Forward the request to a worker in a round-robin fashion
    let worker = workers[currentWorker]
    currentWorker = (currentWorker + 1) % workers.length
    if (!worker) {
      console.error('No worker available to process the receipt 1')
      worker = workers[currentWorker]
      currentWorker = (currentWorker + 1) % workers.length
      if (worker) {
        console.log('Verifying on the worker process 2', txId, timestamp, worker.process.pid)
        const cloneReceipt = Utils.deepCopy(receipt)
        delete cloneReceipt.tx.originalTxData
        delete cloneReceipt.executionShardKey
        const stringifiedReceipt = StringUtils.safeStringify(cloneReceipt)
        worker.send({
          type: 'receipt-verification',
          data: { stringifiedReceipt, requiredSignatures },
        })
        verificationResult = await forwardReceiptVerificationResult(txId, timestamp, worker)
      } else {
        console.error('No worker available to process the receipt 2')
        // Verifying the receipt in the main thread
        console.log('Verifying on the main program 2', txId, timestamp)
        verificationResult = await verifyArchiverReceipt(receipt, requiredSignatures)
      }
    } else {
      if (config.workerProcessesDebugLog)
        console.log('Verifying on the worker process 1', txId, timestamp, worker.process.pid)
      const cloneReceipt = Utils.deepCopy(receipt)
      delete cloneReceipt.tx.originalTxData
      delete cloneReceipt.executionShardKey
      const stringifiedReceipt = StringUtils.safeStringify(cloneReceipt)
      worker.send({
        type: 'receipt-verification',
        data: { stringifiedReceipt, requiredSignatures },
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
