import * as crypto from '@shardus/crypto-utils'
import fetch from 'node-fetch'
import { join } from 'path'
import { config, overrideDefaultConfig } from '../src/Config'

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

crypto.init(config.ARCHIVER_HASH_KEY)

const devAccount = {
  publicKey: config.ARCHIVER_PUBLIC_KEY,
  secretKey: config.ARCHIVER_SECRET_KEY,
}
const ARCHIVER_URL = `http://127.0.0.1:4000`

const data: any = {
  count: 1,
  sender: devAccount.publicKey,
}
crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)
// console.log(data)

// Update endpoints name ... totalData / cycleinfo / receipt / account / transaction
fetch(`${ARCHIVER_URL}/totalData`, {
  // fetch(`${ARCHIVER_URL}/cycleinfo`, {
  // fetch(`${ARCHIVER_URL}/receipt`, {
  // fetch(`${ARCHIVER_URL}/account`, {
  method: 'post',
  body: JSON.stringify(data),
  headers: { 'Content-Type': 'application/json' },
  timeout: 2000,
})
  .then(async (res) => {
    if (res.ok) console.log(await res.json())
    // if (res.ok) console.dir(await res.json(), { depth: null })
    else console.log(res.status)
  })
  .catch((err) => {
    console.log(err)
  })
