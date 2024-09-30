import * as cron from 'node-cron'
import { sleep } from '../Utils'
import { readFileSync } from 'fs'
import * as Crypto from '../Crypto'
import * as Logger from '../Logger'
import { join, resolve } from 'path'
import { startSaving } from '../saveConsoleOutput'
import { Utils as StringUtils } from '@shardus/types'
import { overrideDefaultConfig, config } from '../Config'
import { getLastProcessedTxDigest } from '../txDigester/txDigests'
import { getTxDigestsForACycleRange } from '../txDigester/txDigestFunctions'

import {
  init as initPublisher,
  uploadDigestToIPFS,
  processFailedDigestUploads,
  failedDigests,
  REST_PERIOD_BETWEEN_UPLOADS,
} from './ipfsPublisher'
import {
  initTxDigestDB,
  insertUploadedIPFSRecord,
  initializeIPFSRecordsDB,
  getLastUploadedIPFSRecord,
} from './dbStore'

let isCronActive = false

const configFile = join(process.cwd(), 'archiver-config.json')

;(async (): Promise<void> => {
  overrideDefaultConfig(configFile)
  const BASE_DIR = '.'
  const LOG_DIR = `${config.ARCHIVER_LOGS}/ipfsPublisher`

  Crypto.setCryptoHashKey(config.ARCHIVER_HASH_KEY)
  let logsConfig
  try {
    logsConfig = StringUtils.safeJsonParse(
      readFileSync(resolve(__dirname, '../../archiver-log.json'), 'utf8')
    )
    logsConfig.dir = LOG_DIR
  } catch (err) {
    console.log('Failed to parse archiver log file:', err)
  }

  Logger.initLogger(BASE_DIR, logsConfig)
  if (logsConfig.saveConsoleOutput) {
    startSaving(join(BASE_DIR, logsConfig.dir))
  }

  await initTxDigestDB(config)
  await initializeIPFSRecordsDB(config)
  await initPublisher()

  cron.schedule(config.txDigest.txCronSchedule, async () => {
    try {
      if (isCronActive) {
        console.log('IPFS Publisher Cron Job already running....')
        return
      }
      isCronActive = true
      console.log('Running IPFS Publisher Cron Job....')

      if (failedDigests.length > 0) {
        console.log(`⏳ Re-trying: ${failedDigests.length} Failed Digest Uploads...`)
        await processFailedDigestUploads()
      }

      const lastTxDigest = await getLastProcessedTxDigest()
      let latestDigestPushedToIPFS = (await getLastUploadedIPFSRecord()) || { cycleEnd: 0 }

      console.log(
        `Latest Cycle of Tx-Digest Processed vs Pushed to IPFS: ${lastTxDigest.cycleEnd} vs ${latestDigestPushedToIPFS.cycleEnd}`
      )

      if (latestDigestPushedToIPFS) {
        if (latestDigestPushedToIPFS.cycleEnd < lastTxDigest.cycleEnd) {
          const txDigests = await getTxDigestsForACycleRange(
            latestDigestPushedToIPFS.cycleEnd,
            lastTxDigest.cycleEnd
          )
          console.log('TX-Digests to Upload: ', txDigests.length)
          for (const digest of txDigests) {
            const uploadedDigest = await uploadDigestToIPFS(digest)
            if (uploadedDigest) {
              await insertUploadedIPFSRecord(uploadedDigest)
              latestDigestPushedToIPFS = uploadedDigest
              console.log(
                `✅ Tx-Digest till Cycle ${latestDigestPushedToIPFS.cycleEnd} pushed to IPFS (CID: ${uploadedDigest.cid}).`
              )
              console.log(`Waiting for ${REST_PERIOD_BETWEEN_UPLOADS}ms before next upload...`)
              await sleep(REST_PERIOD_BETWEEN_UPLOADS)
            }
          }
        }
      }
      isCronActive = false
    } catch (error) {
      isCronActive = false
      console.error('Error in ipfsPublisher Cron Job:', error)
    }
  })
})()
