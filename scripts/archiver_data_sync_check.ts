import * as crypto from '@shardus/crypto-utils'
import { writeFileSync } from 'fs'
import { join } from 'path'
import { postJson } from '../src/P2P'
import { config, overrideDefaultConfig } from '../src/Config'
import { ArchiverNodeInfo } from '../src/State'

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

crypto.init(config.ARCHIVER_HASH_KEY)

export type ArchiverNode = Omit<ArchiverNodeInfo, 'publicKey' | 'curvePk'>

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

const devAccount = {
  publicKey: '',
  secretKey: '',
}

const startCycle = 30000
const endCycle = 31415

// const URL = 'originalTx'
const URL = 'receipt'

const runProgram = async (): Promise<void> => {
  const limit = 100
  for (const archiver of archivers) {
    const archiverInfo = archiver.ip + ':' + archiver.port
    const responses = {}
    for (let i = startCycle; i < endCycle; ) {
      const nextEnd = i + limit
      console.log(i, nextEnd)

      const data: any = {
        startCycle: i,
        endCycle: nextEnd,
        type: 'tally',
        sender: devAccount.publicKey,
      }
      crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)
      const response: any = await postJson(`http://${archiverInfo}/${URL}`, data, 5000)
      if (!response) {
        console.log(`archiver ${archiverInfo} failed to respond`)
        continue
      }
      // console.log(response)
      if (responses[archiverInfo]) {
        const result = response.receipts ? response.receipts : response.originalTxs
        responses[archiverInfo] = [...responses[archiverInfo], ...result]
      } else {
        responses[archiverInfo] = response.receipts ? response.receipts : response.originalTxs
      }
      i = nextEnd + 1
    }
    // console.dir(responses, { depth: null })
    // save to file
    writeFileSync(`archiver_${archiverInfo}_${startCycle}_${endCycle}_${URL}.json`, JSON.stringify(responses))
  }
}

runProgram()
