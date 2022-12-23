import * as Crypto from '../../Crypto'
import { config } from '../../Config'
import { calculateNetworkHash } from '../../archivedCycle/StateMetaData'
import { getJson } from '../../P2P'

Crypto.setCryptoHashKey(config.ARCHIVER_HASH_KEY)

export async function queryArchivedCycles(ip: any, port: any, count: number, start: number, end: number) {
  let res: any = await getJson(`http://${ip}:${port}/full-archive/${count}`)
  console.log(res)
  validateArchivedCycle(res)

  res = await getJson(`http://${ip}:${port}/full-archive?start=${start}&end=${end}`)
  console.log(res)
  validateArchivedCycle(res)
}

const validateArchivedCycle = (res) => {
  const archivedCycles = res['archivedCycles']

  for (let i = 0; i < archivedCycles.length; i++) {
    // console.log(
    //   archivedCycles[i].receipt
    //     ? archivedCycles[i].receipt.partitionTxs[0]
    //     : 'no receipt'
    // )
    const result = valueCheck(archivedCycles[i])
    if (result.length > 0) {
      console.log(
        `Archived Cyle ${archivedCycles[i].cycleRecord.counter} doesn't have these values --> ${result}`
      )
    }
  }
}

const expectedValue = {
  cycleMarker: 'ff4a09332190573b706add1a8e5cd513adf85acfea7aa39cc94383ce2e3620a8',
  cycleRecord: {
    activated: [],
    activatedPublicKeys: [],
    active: 5,
    apoptosized: [],
    counter: 80,
    desired: 2000,
    duration: 16,
    expired: 5,
    joined: [],
    joinedArchivers: [],
    joinedConsensors: [],
    leavingArchivers: [],
    lost: [],
    marker: 'ff4a09332190573b706add1a8e5cd513adf85acfea7aa39cc94383ce2e3620a8',
    networkDataHash: [[Object]],
    networkId: '411b3e07ac4e2f1faadb081e9eea762c943699c4691bd011588398f533ae1ed1',
    networkReceiptHash: [[Object]],
    networkStateHash: '47de6951e9907a2e187ecd077c82ea7e568745b48a3616b1d7f7c48a363f683c',
    networkSummaryHash: [[Object]],
    previous: 'f4944a99bdf7b8ce9b7244d6a4e6eeed92ba778156478927590d46d8f41e8243',
    refreshedArchivers: [[Object]],
    refreshedConsensors: [[Object], [Object]],
    refuted: [],
    removed: [],
    returned: [],
    safetyMode: false,
    safetyNum: 0,
    start: 1650372600,
    syncing: 0,
  },
  data: {
    networkHash: '4bed936c3b08bd7b23e03b1fdefa32e7f01bdfa1fdd288617ed70feebf4feda2',
    parentCycle: 'ff4a09332190573b706add1a8e5cd513adf85acfea7aa39cc94383ce2e3620a8',
    partitionHashes: {
      '0': 'aef4a946d7268af2f12604d9b6e8e9fd4c5d528cc6b37e1c60ec4225b100ce34',
      '1': '9e3cf6e8cc73349cdd1875dbaa43e2be2d468bbc90a99e748f3af62625962332',
      '2': '0ee512ffed3cd68cc4695a9ed332e57be8bd4cc4565d61393fa4e2f19286af3d',
      '3': '9ee75e6b7d6eb50fe6cbab806a3dcd3b9bfddf54b51ad0afa95dd2d9a6b96c79',
      '4': '18d83747e4d9bbc8acfdebcaca8d1892461d574fe95f10d8b492b7ef94afd9c6',
      '-1': '11800a39aecdafc33f365c772529ee9b823415232acd252c660fc8256d41e21c',
    },
  },
  receipt: {
    networkHash: '97ab6b4502137e30788d97659d232e8c1077b394323402fbdb4512c76be6e0c7',
    parentCycle: 'ff4a09332190573b706add1a8e5cd513adf85acfea7aa39cc94383ce2e3620a8',
    partitionHashes: {
      '0': 'de8723bf28eb9ae6d92f7b26b12b4d77edc61362c6735b942c45eb89a1271ed9',
      '1': 'afaff32d4929857416fe17a088be9db717738dfba0103280a579b6a4dd716ba7',
      '2': 'f97b663ea6485283cbf5b9fe4f7061f4a4388237db4e204a4879dd4568b04a7c',
      '3': '439fd0a12c08861408aafbd2db15cfc546bb51fd80737bf4d86f2f587fd617ea',
      '4': 'bc073ecbefad4cc13200566f5753c0154da044a58c2d84a5d25f130a3d7987e8',
      '-1': '573808deeba726aaec51c2fe4df03130516189de016b9c53448261a5b5c88b1d',
    },
    partitionMaps: { '0': {}, '1': {}, '2': {}, '3': {}, '4': {} },
    partitionTxs: { '0': {}, '1': {}, '2': {}, '3': {}, '4': {} },
  },
  summary: {
    networkHash: '78c7d4bfca718a92b57a31832c1c8460f43dee960b5f4cf4bbdae3bcce2deb6d',
    parentCycle: 'ff4a09332190573b706add1a8e5cd513adf85acfea7aa39cc94383ce2e3620a8',
    partitionBlobs: {},
    partitionHashes: {},
  },
}

const valueCheck = (obj) => {
  const valueNotFound = []
  for (let i of Object.keys(expectedValue)) {
    // console.log(i, typeof obj[i], typeof expectedValue[i])
    if (!obj.hasOwnProperty(i) && typeof obj[i] !== typeof expectedValue[i]) {
      valueNotFound.push(i)
    }

    if (i === 'data' || i === 'receipt' || i === 'summary') {
      if (obj[i]) {
        const networkHash = obj[i].networkHash
        // console.log(obj[i].networkHash, obj[i].partitionHashes)
        const calculatedHash = calculateNetworkHash(obj[i].partitionHashes)
        if (networkHash !== calculatedHash) {
          console.log(
            `The specified networkHash and calculatedHash of ${i} are different in Archived Cyle ${obj.cycleRecord.counter}`
          )
        }
        if (i === 'receipt') {
          for (let partitionId in Object.values(obj[i].partitionMaps)) {
            // console.log(
            //   obj[i].partitionMaps[partitionId],
            //   obj[i].partitionTxs[partitionId]
            // )
            let txCount = 0
            for (let data of Object.values(obj[i].partitionMaps[partitionId])) {
              const a: any = data
              txCount += parseInt(a.length)
            }
            const partitionBlock = {
              cycle: obj.cycleRecord.counter,
              partition: parseInt(partitionId),
              receiptMap: obj[i].partitionMaps[partitionId],
              txsMap: obj[i].partitionTxs[partitionId],
              txCount: txCount,
            }
            // console.log(partitionBlock)
            // console.log(
            //   obj[i].partitionHashes[partitionId],
            //   Crypto.hashObj(partitionBlock)
            // )
            if (obj[i].partitionHashes[partitionId] !== Crypto.hashObj(partitionBlock)) {
              console.log(
                `The specified partitionHash and calculatedHash of ${i} are different in Archived Cyle ${obj.cycleRecord.counter}, ${partitionId}`
              )
            }
          }
        }
      }
    }
  }
  return valueNotFound
}
