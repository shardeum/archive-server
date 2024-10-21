import { join } from 'path'
import { Blob } from 'buffer'
import { TransactionDigest } from './txDigests'
import type { Client } from '@web3-storage/w3up-client'
import { overrideDefaultConfig, config } from '../Config'
import { mainLogger } from '../Logger'
import { BlobLike } from '@web3-storage/w3up-client/dist/src/types'
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { W3SClient, Email } = require(join(process.cwd(), '/src/utils/esm/bundle.js'))

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

const { adminEmail, rootDID } = config.txDigest.web3Storage

type Web3StorageRootDID = `did:${string}:${string}`
type Web3StorageAdminEmail = `${string}@${string}`

let client: Client
let lastUploadTime: number | null = null
let isPublisherActive = false

// Note: This can be removed in production or in cases where uploads will happen once in 5 or more cycles/minutes
const REST_PERIOD_BETWEEN_UPLOADS = 1000 * 60 * 5 // 5 minutes

// Upload a file to the specified space DID
export const uploadDigestToIPFS = async (data: TransactionDigest): Promise<void> => {
  try {
    if (!isUploadPossible()) {
      console.log(
        `❌ Publisher cannot upload to IPFS: ${
          isPublisherActive ? 'Another Upload in progress.' : 'Rest Period is Active.'
        }`
      )
      return
    }
    isPublisherActive = true
    console.log(`Uploading TX Digest for Cycle Range ${data.cycleStart} to ${data.cycleEnd}`)
    await client.setCurrentSpace(rootDID as Web3StorageRootDID)
    console.log(`Uploading Data to Root-DID: ${rootDID}`)

    const { cycleStart: cs, cycleEnd: ce, txCount: tc, hash: h } = data
    const optimisedJSON = { cs, ce, tc, h }

    const cid = await client.uploadFile(
      new Blob([JSON.stringify(optimisedJSON)], { type: 'application/json' }) as BlobLike
    )
    console.log(
      `✅ Uploaded to IPFS Successfully for Cycles (${data.cycleStart} to ${data.cycleEnd}) @ https://${cid}.ipfs.w3s.link`
    )
    lastUploadTime = Date.now()
    const storageUsed = await client.currentSpace()?.usage.get()
    console.log(`${Number(storageUsed!.ok)} bytes used on Web3.Storage.`)
    isPublisherActive = false
  } catch (error) {
    isPublisherActive = false
    console.error(
      `❌ Error while Uploading Digest for Cycles: ${data.cycleStart} to ${data.cycleEnd} w/ Hash: ${data.hash}) to IPFS:`
    )
    console.error(error)
    return
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
