import fastify = require('fastify')
import * as Crypto from '../Crypto'
import * as Data from './Data'
import * as State from '../State'
import * as P2P from '../P2P'
import { config, Config } from '../Config'

let gossipCollector = new Map()

export async function sendGossip (type: string, payload: any) {
  let archivers: State.ArchiverNodeInfo[] = [...State.activeArchivers]

  if (archivers.length === 0) return
  const gossipPayload = { type, data: payload, sender: config.ARCHIVER_PUBLIC_KEY }

  // console.log(`Start of sendGossipIn(${JSON.stringify(gossipPayload)})`)

  archivers = archivers.sort((a: any, b: any) => a.publicKey - b.publicKey)

  // TODO: check if need to select random archivers instead of sending to all other archivers
  let recipients: State.ArchiverNodeInfo[] = archivers.filter(
    a => a.publicKey !== config.ARCHIVER_PUBLIC_KEY
  )

  if (recipients.length === 0) {
    console.log('There is no other archivers to send our gossip')
    return
  }

  try {
    console.log(
      `GossipingIn ${type} request to these nodes: ${JSON.stringify(
        recipients.map(node => node.ip + ':' + node.port + `/gossip-${type}`)
      )}`
    )
    await tell(recipients, `gossip-${type}`, gossipPayload, true)
  } catch (ex) {
    console.log(ex)
    console.log('Fail to gossip')
  }
  // console.log(`End of sendGossipIn(${JSON.stringify(payload)})`)
}

async function tell (
  nodes: State.ArchiverNodeInfo[],
  route: string,
  message: any,
  logged = false
) {
  let InternalTellCounter = 0
  const promises = []
  for (const node of nodes) {
    InternalTellCounter++
    const url = `http://${node.ip}:${node.port}/${route}`
    const promise = P2P.postJson(url, message)
    promise.catch(err => {
      console.log(err)
    })
    promises.push(promise)
  }
  try {
    await Promise.all(promises)
  } catch (err) {
    console.log('Network: ' + err)
  }
}

export function convertStateMetadataToHashArray(STATE_METATDATA: any) {
  let hashCollector: any = {}
  STATE_METATDATA.stateHashes.forEach((h:any) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter
      }
    }
    hashCollector[h.counter]['stateHashes'] = h
  })
  STATE_METATDATA.receiptHashes.forEach((h:any) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter
      }
    }
    hashCollector[h.counter]['receiptHashes'] = h
  })
  STATE_METATDATA.summaryHashes.forEach((h:any) => {
    if (!hashCollector[h.counter]) {
      hashCollector[h.counter] = {
        counter: h.counter
      }
    }
    hashCollector[h.counter]['summaryHashes'] = h
  })
  return Object.values(hashCollector)
}

export function addHashesGossip (
  sender: string,
  gossip: any
) {
  let counter = gossip.counter
  if (gossipCollector.has(counter)) {
    let existingGossips = gossipCollector.get(counter)
    existingGossips[sender] = gossip
  } else {
    let obj: any = {}
    obj[sender] = gossip
    gossipCollector.set(counter, obj)
  }
  let totalGossip = gossipCollector.get(counter)
  if (
    totalGossip &&
    Object.keys(totalGossip).length > 0.5 * State.activeArchivers.length
  ) {
    setTimeout(() => {
      processGossip(counter)
      gossipCollector.delete(counter)
    }, 500)
  }
}

function processGossip(counter: number) {
  console.log('Processing gossips for counter', counter, gossipCollector.get(counter))
  let gossips = gossipCollector.get(counter)
  if (!gossips) {
    return
  }
  let gossipCounter: any = {}
  for (let sender in gossips) {
    let hashedGossip = Crypto.hashObj(gossips[sender])
    if(!gossipCounter[hashedGossip]) {
      gossipCounter[hashedGossip] = {
        count: 1,
        gossip: gossips[sender]
      } 
    } else {
      gossipCounter[hashedGossip].count += 1
    }
  }
  // console.log('gossipCounter', gossipCounter)
  let gossipWithHighestCount: any
  let highestCount = 0
  let hashWithHighestCounter: any
  for (let key in gossipCounter) {
    if (gossipCounter[key].count > highestCount) {
      gossipWithHighestCount = gossipCounter[key].gossip
      hashWithHighestCounter = key
      highestCount = gossipCounter[key].count 
    }
  }
  let ourHashes = Data.StateMetaDataMap.get(counter)
  if (!ourHashes) {
    console.log(`Unable to find our stored statemetadata hashes for counter ${counter}`)
    return
  }
  if (hashWithHighestCounter && hashWithHighestCounter !== Crypto.hashObj(ourHashes)) {
    console.log('our hash is different from other archivers hashes. Storing the correct hashes')
    Data.processStateMetaData(gossipWithHighestCount)
    Data.replaceDataSender(Data.currentDataSender)
  }
}
