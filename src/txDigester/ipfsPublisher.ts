// Import necessary libraries and types
import { join } from 'path'
import { Blob } from 'buffer'
import { TransactionDigest } from './txDigests'
import * as W3SClient from '@web3-storage/w3up-client'
import { fromEmail } from '@web3-storage/did-mailto'
import { BlobLike } from '@web3-storage/w3up-client/dist/src/types'

import { overrideDefaultConfig, config } from '../Config'
import { mainLogger } from '../Logger'

const configFile = join(process.cwd(), 'archiver-config.json')
overrideDefaultConfig(configFile)

const { admin_email, root_did } = config.txDigest.web3Storage

type Web3StorageRootDID = `did:${string}:${string}`
type Web3StorageAdminEmail = `${string}@${string}`

let client: W3SClient.Client

// Upload a file to the specified space DID
export const uploadDigestToIPFS = async (data: TransactionDigest): Promise<void> => {
  try {
    mainLogger.info(`Uploading TX Digest for Cycle Range ${data.cycleStart} to ${data.cycleEnd}`)
    await client.setCurrentSpace(root_did as Web3StorageRootDID)
    console.log(`Uploading Data to Root-DID: ${root_did}`)

    const { cycleStart: cs, cycleEnd: ce, txCount: tc, hash: h } = data
    const optimisedJSON = { cs, ce, tc, h }

    const cid = await client.uploadFile(
      new Blob([JSON.stringify(optimisedJSON)], { type: 'application/json' }) as BlobLike
    )
    mainLogger.info(`✅ Uploaded to IPFS Successfully | CID: ${cid}`)
    console.log(
      `✅ Tx-Digest for Cycle Range (${data.cycleStart} to ${data.cycleEnd}) @ https://w3s.link/ipfs/${cid}`
    )
    const storageUsed = await client.currentSpace()?.usage.get()
    mainLogger.info(`${Number(storageUsed!.ok)} bytes used on Web3.Storage.`)
  } catch (error) {
    console.error('Error while Uploading to IPFS: ', error)
  }
}

/**
 * Checks if the email has subscribed to a plan on the Web3.Storage Portal
 * We'll most likely be Subscribed to the Starter/Free Plan (5GB of storage)
 */
const isSubscriptionActive = async (account: W3SClient.Account.Account): Promise<boolean> => {
  const plan = await account.plan.get()
  if (!plan.ok) {
    console.error(`❌ ${admin_email} does not have an active plan subscribed on Web3.Storage.`)
    return false
  }
  return true
}

// Checks if the client machine is authorised by the admin to interact with Web3.Storage provider
const isAuthWithEmail = async (email: Web3StorageAdminEmail): Promise<boolean> => {
  const accounts = client.accounts()
  return Object.prototype.hasOwnProperty.call(accounts, fromEmail(email))
}

export const init = async (): Promise<void> => {
  try {
    console.log('CONFIG: ')
    console.log({ admin_email, root_did })
    console.log('Initialising IPFS publisher...')
    if (!admin_email || !root_did) {
      console.error('❌ admin_email or root_did cannot be empty. Values:')
      console.error({ admin_email, root_did })
      return
    }
    client = await W3SClient.create()

    console.log(`Logging into Web3.Storage with Account: ${admin_email}...`)
    if (!isAuthWithEmail(admin_email as Web3StorageAdminEmail))
      console.log(`⏳ Owner of ${admin_email} needs to approve the Web3.Storage Auth request to proceed.`)

    const account = await client.login(admin_email as Web3StorageAdminEmail)
    if (!isSubscriptionActive(account)) {
      console.error(`❌ ${admin_email} does not have a payment plan on Web3.Storage. Terminating...`)
      return
    }

    await client.setCurrentSpace(root_did as Web3StorageRootDID)
    console.log(`✅ Login Successful! | Current Space DID: ${root_did}`)
    return
  } catch (error) {
    mainLogger.error(`Error while initializing Web3Storage client:-`)
    mainLogger.error(error)
    return
  }
}
