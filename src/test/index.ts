import { queryNodes } from './api/nodes'
import { queryArchivedCycles } from './api/archivedCycles'
import { queryCycles } from './api/cycles'
import * as MulitpleArchivers from './dataSync/mulitpleArchivers'

const ARCHIVER_HOST = '127.0.0.1'
const ARCHIVER_PORT = '4000'

const numberOfConsensors = 10
const numberOfArchivers = 1

// queryArchivedCycles(ARCHIVER_HOST, ARCHIVER_PORT, 5, 10, 20)

// queryCycles(ARCHIVER_HOST, ARCHIVER_PORT, 10, 10, 20)

// queryNodes(ARCHIVER_HOST, ARCHIVER_PORT, 5, 10)

const runTest = async () => {
  if (numberOfArchivers > 1) {
    await MulitpleArchivers.checkDataSyncBetweenArchivers(ARCHIVER_HOST, numberOfArchivers)
    await MulitpleArchivers.checkCyclesDataBetweenArchivers(ARCHIVER_HOST, numberOfArchivers)
    await MulitpleArchivers.checkReceiptsDataBetweenArchivers(ARCHIVER_HOST, numberOfArchivers)
  }
}

runTest()
