import { getJson } from '../../P2P'

interface ReceiptResponse {
  receipts: Array<{ cycle: number; receipts: unknown[] }>
}

interface TotalData {
  totalCycles: number
  totalAccounts: number
  totalTransactions: number
  totalReceipts: number
}

interface CycleInfoResponse {
  cycleInfo: unknown
}

export async function checkDataSyncBetweenArchivers(ip, numberOfArchivers): Promise<void> {
  const dataInfos = {
    archiverInfo: [],
    totalCycles: [],
    totalAccounts: [],
    totalTransactions: [],
    totalReceipts: [],
  }

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    const res = (await getJson(`http://${archiverURL}/totalData`)) as TotalData
    if (typeof res === 'object' && res !== null && 'cycleInfo' in res) {
      dataInfos.archiverInfo.push(archiverURL)
      dataInfos.totalCycles.push(res.totalCycles)
      dataInfos.totalAccounts.push(res.totalAccounts)
      dataInfos.totalTransactions.push(res.totalTransactions)
      dataInfos.totalReceipts.push(res.totalReceipts)
    } else console.log(`Fail to fetch totalData from archiver ${archiverURL}`)
  }

  if (dataInfos.archiverInfo.length > 1) {
    const expectedTotalCycles = dataInfos.totalCycles[0]
    const totalCyclesIsMatched = dataInfos.totalCycles.every((cycle) => cycle === expectedTotalCycles)
    if (totalCyclesIsMatched) console.log('TotalCycles is matched!')

    const expectedTotalAccounts = dataInfos.totalAccounts[0]
    const totalAccountsIsMatched = dataInfos.totalAccounts.every((cycle) => cycle === expectedTotalAccounts)
    if (totalAccountsIsMatched) console.log('TotalAccounts is matched!')

    const expectedTotalTransactions = dataInfos.totalTransactions[0]
    const totalTransactionsIsMatched = dataInfos.totalTransactions.every(
      (cycle) => cycle === expectedTotalTransactions
    )
    if (totalTransactionsIsMatched) console.log('TotalTransactions is matched!')

    const expectedTotalReceipts = dataInfos.totalReceipts[0]
    const totalReceiptsIsMatched = dataInfos.totalReceipts.every((cycle) => cycle === expectedTotalReceipts)
    if (totalReceiptsIsMatched) console.log('TotalReceipts is matched!')
  }
}

export async function checkCyclesDataBetweenArchivers(ip, numberOfArchivers): Promise<void> {
  const dataInfos = {}

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    const res = (await getJson(`http://${archiverURL}/cycleinfo/100`)) as CycleInfoResponse
    if (res) {
      // eslint-disable-next-line security/detect-object-injection
      dataInfos[archiverURL] = res.cycleInfo
    } else console.log(`Fail to fetch cycle data from archiver ${archiverURL}`)
  }

  if (Object.keys(dataInfos).length > 0) {
    const archiverInfos = Object.keys(dataInfos)
    const expectedCycles = dataInfos[archiverInfos[0]]
    let allCyclesAreMatched = true
    for (let i = 0; i < expectedCycles.length; i++) {
      // eslint-disable-next-line security/detect-object-injection
      const cycleInfo = expectedCycles[i]
      const cycleInfoToMatch = JSON.stringify(cycleInfo)
      for (let j = 1; j < archiverInfos.length; j++) {
        // console.log(cycleInfo.counter, dataInfos[archiverInfos[j]][i].counter)
        // eslint-disable-next-line security/detect-object-injection
        if (cycleInfoToMatch !== JSON.stringify(dataInfos[archiverInfos[j]][i])) {
          allCyclesAreMatched = false
          console.log(`Cycle ${cycleInfo.counter} is not matched between archivers!`)
        }
      }
    }
    if (allCyclesAreMatched) console.log('All the latest 100 cycles are match!')
  }
}

export async function checkReceiptsDataBetweenArchivers(ip, numberOfArchivers): Promise<void> {
  const randomPort = getRndInteger(0, numberOfArchivers - 1)
  const randomArchiver = ip + ':' + (4000 + randomPort)
  const response = (await getJson(`http://${randomArchiver}/receipt/1`)) as ReceiptResponse
  const latestReceiptInfo = response.receipts[0]
  const endCycle = latestReceiptInfo.cycle
  const startCycle = latestReceiptInfo.cycle - 10

  const dataInfos = {
    archiverInfo: [],
    cycles: {},
  }

  for (let i = startCycle; i <= endCycle; i++) {
    // eslint-disable-next-line security/detect-object-injection
    dataInfos.cycles[i] = []
  }

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    const res = (await getJson(
      `http://${archiverURL}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`
    )) as ReceiptResponse
    if (res && res.receipts) {
      dataInfos.archiverInfo.push(archiverURL)
      for (const receiptInfo of res.receipts) {
        dataInfos.cycles[receiptInfo.cycle].push(receiptInfo.receipts)
      }
      // Place 0 for cycles that are not in the query
      for (let j = startCycle; j <= endCycle; j++) {
        let found = false
        for (const receiptInfo of res.receipts) {
          if (j === receiptInfo.cycle) found = true
        }
        // eslint-disable-next-line security/detect-object-injection
        if (!found) dataInfos.cycles[j].push(0)
      }
    } else console.log(`Fail to fetch receipt data between cycles from archiver ${archiverURL}`)
  }

  if (dataInfos.archiverInfo.length > 1) {
    let allReceiptsAreMatched = true
    for (let i = startCycle; i <= endCycle; i++) {
      // eslint-disable-next-line security/detect-object-injection
      const expectedReceipts = dataInfos.cycles[i][0]
      // eslint-disable-next-line security/detect-object-injection
      const receiptsIsMatched = dataInfos.cycles[i].every((receipts) => receipts === expectedReceipts)
      if (!receiptsIsMatched) {
        allReceiptsAreMatched = false
        console.log(`Receipts Count do not match in cycle ${i}`)
      }
    }
    if (allReceiptsAreMatched)
      console.log(`All receipts count of cycles between ${startCycle} - ${endCycle} are matched!`)
  }
}

function getRndInteger(min, max): number {
  return Math.floor(Math.random() * (max - min + 1)) + min
}
