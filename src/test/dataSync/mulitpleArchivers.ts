import { getJson } from '../../P2P'

export async function checkDataSyncBetweenArchivers(ip, numberOfArchivers) {
  let dataInfos = {
    archiverInfo: [],
    totalCycles: [],
    totalAccounts: [],
    totalTransactions: [],
    totalReceipts: [],
  }

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    let res: any = await getJson(`http://${archiverURL}/totalData`)
    if (res) {
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

export async function checkCyclesDataBetweenArchivers(ip, numberOfArchivers) {
  let dataInfos = {}

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    let res: any = await getJson(`http://${archiverURL}/cycleinfo/100`)
    if (res) {
      dataInfos[archiverURL] = res.cycleInfo
    } else console.log(`Fail to fetch cycle data from archiver ${archiverURL}`)
  }

  if (Object.keys(dataInfos).length > 0) {
    const archiverInfos = Object.keys(dataInfos)
    const expectedCycles = dataInfos[archiverInfos[0]]
    let allCyclesAreMatched = true
    for (let i = 0; i < expectedCycles.length; i++) {
      const cycleInfo = expectedCycles[i]
      const cycleInfoToMatch = JSON.stringify(cycleInfo)
      for (let j = 1; j < archiverInfos.length; j++) {
        // console.log(cycleInfo.counter, dataInfos[archiverInfos[j]][i].counter)
        if (cycleInfoToMatch !== JSON.stringify(dataInfos[archiverInfos[j]][i])) {
          allCyclesAreMatched = false
          console.log(`Cycle ${cycleInfo.counter} is not matched between archivers!`)
        }
      }
    }
    if (allCyclesAreMatched) console.log('All the latest 100 cycles are match!')
  }
}

export async function checkReceiptsDataBetweenArchivers(ip, numberOfArchivers) {
  const randomPort = getRndInteger(0, numberOfArchivers - 1)
  const randomArchiver = ip + ':' + (4000 + randomPort)
  let response: any = await getJson(`http://${randomArchiver}/receipt/1`)
  const latestReceiptInfo = response.receipts[0]
  const endCycle = latestReceiptInfo.cycle
  const startCycle = latestReceiptInfo.cycle - 10

  let dataInfos = {
    archiverInfo: [],
    cycles: {},
  }

  for (let i = startCycle; i <= endCycle; i++) {
    dataInfos.cycles[i] = []
  }

  for (let i = 0; i < numberOfArchivers; i++) {
    const archiverURL = ip + ':' + (4000 + i)
    let res: any = await getJson(
      `http://${archiverURL}/receipt?startCycle=${startCycle}&endCycle=${endCycle}&type=tally`
    )
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
        if (!found) dataInfos.cycles[j].push(0)
      }
    } else console.log(`Fail to fetch receipt data between cycles from archiver ${archiverURL}`)
  }

  if (dataInfos.archiverInfo.length > 1) {
    let allReceiptsAreMatched = true
    for (let i = startCycle; i <= endCycle; i++) {
      const expectedReceipts = dataInfos.cycles[i][0]
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

function getRndInteger(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min
}
