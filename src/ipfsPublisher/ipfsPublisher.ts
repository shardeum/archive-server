import type { Client } from '@web3-storage/w3up-client'
import { BlobLike } from '@web3-storage/w3up-client/dist/src/types'

import { join } from 'path'
import { Blob } from 'buffer'
import { mainLogger } from '../Logger'
import { IPFSRecord, insertUploadedIPFSRecord } from './dbStore'
import { overrideDefaultConfig, config } from '../Config'
import { TransactionDigest } from '../txDigester/txDigests'

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { W3SClient, Email } = require(join(process.cwd(), '/src/ipfsPublisher/esm/bundle.js'))

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

const { adminEmail, rootDID } = config.txDigest.web3Storage

type Web3StorageRootDID = `did:${string}:${string}`
type Web3StorageAdminEmail = `${string}@${string}`

let client: Client
let lastUploadTime: number | null = null
let isPublisherActive = false
export const failedDigests: TransactionDigest[] = []

// Note: This can be removed in production or in cases where uploads will happen once in 5 or more cycles/minutes
export const REST_PERIOD_BETWEEN_UPLOADS = 1000 * 30 // 30 seconds

// Upload a file to the specified space DID
export const uploadDigestToIPFS = async (data: TransactionDigest): Promise<IPFSRecord> | null => {
  try {
    if (!isUploadPossible()) {
      console.log(
        `Publisher cannot upload to IPFS: ${
          isPublisherActive ? 'Another Upload in progress.' : 'Rest Period is Active.'
        }`
      )
      return null
    }
    isPublisherActive = true
    await client.setCurrentSpace(rootDID as Web3StorageRootDID)
    console.log(
      `⏳ Uploading TX-Digest for Cycle Range ${data.cycleStart} to ${data.cycleEnd} | Space-DID: ${rootDID}`
    )

    const { cycleStart: cs, cycleEnd: ce, txCount: tc, hash: h } = data
    const optimisedJSON = { cs, ce, tc, h }

    const cid = await client.uploadFile(
      new Blob([JSON.stringify(optimisedJSON)], { type: 'application/json' }) as BlobLike
    )
    mainLogger.log(
      `✅ Uploaded to IPFS Successfully for Cycles (${data.cycleStart} to ${data.cycleEnd}) @ https://${cid}.ipfs.w3s.link`
    )
    lastUploadTime = Date.now()
    removeFailedDigest(data)
    const storageUsed = await client.currentSpace()?.usage.get()
    console.log(`${Number(storageUsed!.ok)} bytes used on Web3.Storage.`)
    isPublisherActive = false
    delete data.txCount
    return { ...data, cid: cid.toString(), spaceDID: client.currentSpace()?.did(), timestamp: lastUploadTime }
  } catch (error) {
    isPublisherActive = false
    mainLogger.error(
      `❌ Error while Uploading Digest for Cycles: ${data.cycleStart} to ${data.cycleEnd} w/ Hash: ${data.hash}) to IPFS:`
    )
    mainLogger.error(error)
    addFailedDigest(data)
    return null
  }
}

const addFailedDigest = (digest: TransactionDigest): void => {
  const index = failedDigests.findIndex((failedDigest) => failedDigest.hash === digest.hash)
  if (index === -1) {
    failedDigests.push(digest)
  }
}

const removeFailedDigest = (digest: TransactionDigest): void => {
  const index = failedDigests.findIndex((failedDigest) => failedDigest.hash === digest.hash)
  if (index > -1) {
    failedDigests.splice(index, 1)
  }
}

const isUploadPossible = (): boolean => {
  // If no upload has happened yet, allow the upload
  if (lastUploadTime === null) return !isPublisherActive

  const isRestPeriodOver = Date.now() - lastUploadTime > REST_PERIOD_BETWEEN_UPLOADS
  return !isPublisherActive && isRestPeriodOver
}

// Checks if the client machine is authorised by the admin to interact with Web3.Storage provider
const isAuthWithEmail = (email: Web3StorageAdminEmail): boolean => {
  const accounts = client.accounts()
  return Object.prototype.hasOwnProperty.call(accounts, Email(email))
}

const maskedEmail = (email: string): string => {
  const [name, domain] = email.split('@')
  return `${name.slice(0, 1)}${'*'.repeat(name.length - 2)}${name.slice(-1)}@${domain}`
}

export const processFailedDigestUploads = async (): Promise<void> => {
  try {
    if (failedDigests.length > 0) {
      for (const failedDigest of failedDigests) {
        const uploadedDigest = await uploadDigestToIPFS(failedDigest)
        if (!uploadedDigest) {
          console.error(
            `❌ Failed to upload Digest for Cycle Range ${failedDigest.cycleStart} to ${failedDigest.cycleEnd}. Failed Digests: ${failedDigests.length}`
          )
        } else {
          await insertUploadedIPFSRecord(uploadedDigest)
          failedDigests.splice(failedDigests.indexOf(failedDigest), 1)
        }
      }
    }
  } catch (error) {
    mainLogger.error(`❌ Error in processFailedDigestUploads():-`)
    mainLogger.error(error)
  }
}

export const init = async (): Promise<void> => {
  try {
    isPublisherActive = true
    console.log('Initialising IPFS publisher...')
    if (!adminEmail || !rootDID) {
      console.error('❌ adminEmail or rootDID cannot be empty. Values:')
      console.error({ adminEmail, rootDID })
      return
    }
    client = await W3SClient.create()

    console.log(`Logging into Web3.Storage with Account: ${maskedEmail(adminEmail)}`)
    if (!isAuthWithEmail(adminEmail as Web3StorageAdminEmail)) {
      console.log(`⏳ Owner of ${adminEmail} needs to approve the Web3.Storage Auth request to proceed.`)
      await client.login(adminEmail as Web3StorageAdminEmail)
      console.log(`✅ Web3.Storage Login Successful!`)
    }

    await client.setCurrentSpace(rootDID as Web3StorageRootDID)
    console.log(`Current Space set to DID: ${rootDID}`)
    isPublisherActive = false
    return
  } catch (error) {
    isPublisherActive = false
    mainLogger.error(`❌ Error while initializing Web3Storage client:-`)
    mainLogger.error(error)
    return
  }
}
