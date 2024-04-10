import * as crypto from '@shardus/crypto-utils'
import { join } from 'path'
import { config, overrideDefaultConfig } from '../src/Config'
import { postJson, getJson } from '../src/P2P'
import { isEqual } from 'lodash'

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

crypto.init(config.ARCHIVER_HASH_KEY)

const devAccount = {
  publicKey: config.ARCHIVER_PUBLIC_KEY,
  secretKey: config.ARCHIVER_SECRET_KEY,
}
const ARCHIVER_URL = `http://127.0.0.1:4000`

const txId = ''
const full_receipt = false

const runProgram = async (): Promise<void> => {
  const res: any = await getJson(`${ARCHIVER_URL}/full-nodelist?activeOnly=true`)
  if (!res || !res.nodeList) throw new Error('No active nodes found')
  const nodeList = res.nodeList
  const promises: any[] = []
  for (const node of nodeList) {
    const data: any = {
      txId,
      full_receipt,
    }
    crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)
    promises.push(postJson(`http://${node.ip}:${node.port}/get-tx-receipt`, data))
  }
  Promise.allSettled(promises)
    .then((responses) => {
      const result = {}
      responses.forEach((response, index) => {
        if (response.status === 'fulfilled') {
          const { value } = response

          let found = false
          for (const key in result) {
            if (isEqual(result[key].data, value)) {
              result[key].count++
              result[key].nodes.push(nodeList[index])
              found = true
              break
            }
          }
          if (!found) {
            result[Object.keys(result).length + 1] = {
              count: 1,
              data: value,
              nodes: [nodeList[index]],
            }
          }
        }
      })
      console.dir(result, { depth: null })
    })
    .catch((error) => {
      // Handle any errors that occurred
      console.error(error)
    })
}

runProgram()
