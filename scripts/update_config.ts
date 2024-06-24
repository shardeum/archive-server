import axios from 'axios'
import { join } from 'path'
import { Utils } from '@shardus/types'
import * as crypto from '@shardus/crypto-utils'
import { config, overrideDefaultConfig } from '../src/Config'

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

crypto.init(config.ARCHIVER_HASH_KEY)

const DEV_KEYS = {
  pk: config.ARCHIVER_PUBLIC_KEY,
  sk: config.ARCHIVER_SECRET_KEY,
}

function sign<T>(obj: T, sk: string, pk: string): T & any {
  const objCopy = JSON.parse(crypto.stringify(obj))
  crypto.signObj(objCopy, sk, pk)
  return objCopy
}

function createSignature(data: any, pk: string, sk: string): any {
  return sign({ ...data }, sk, pk)
}

const UPDATE_CONFIG = {
  /* Add Config properties that need to be updated here */
  VERBOSE: true,
  RATE_LIMIT: 200,
}

const INPUT = Utils.safeStringify(createSignature(UPDATE_CONFIG, DEV_KEYS.pk, DEV_KEYS.sk))

axios
  .patch('http://127.0.0.1:4000/set-config', INPUT, {
    headers: {
      'Content-Type': 'application/json',
    },
  })
  .then((response) => {
    console.log(response.data)
  })
  .catch((error) => {
    if (error.response) {
      console.error(error.response)
    } else {
      console.error(error.message)
    }
  })
