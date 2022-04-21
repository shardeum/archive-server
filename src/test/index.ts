import { queryNodes } from './api/nodes'
import { queryArchivedCycles } from './api/archivedCycles'
import { queryCycles } from './api/cycles'

const ARCHIVER_HOST = 'localhost'
const ARCHIVER_PORT = '4000'

queryArchivedCycles(ARCHIVER_HOST, ARCHIVER_PORT, 5, 10, 20)

queryCycles(ARCHIVER_HOST, ARCHIVER_PORT, 10, 10, 20)

queryNodes(ARCHIVER_HOST, ARCHIVER_PORT, 5, 10)
