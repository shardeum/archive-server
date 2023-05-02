const fetch = require('node-fetch');
// import fetch from 'node-fetch';

const activeArchivers = ['3.76.80.217', '194.195.223.142', '45.79.193.36', '45.79.108.24']

const runProgram = () => {
    const promises = activeArchivers.map((archiver) =>
        fetch(`http://${archiver}:4000/full-nodelist?activeOnly=true`, {
            method: 'get',
            headers: { 'Content-Type': 'application/json' },
            timeout: 5000,
        }).then((res) => res.json())
    )

    const tmp = [];

    Promise.allSettled(promises)
        .then((responses) => {
            let i = 0
            for (const response of responses) {
                const archiver = activeArchivers[i]
                if (response.status === 'fulfilled') {
                    const res = response.value
                    if (res && res.nodeList && res.nodeList.length > 0) {
                        tmp.push(res.nodeList.length)
                    } else {
                        console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                    }
                } else {
                    console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                }
                i++
            }
            console.log('tmp', tmp)
        })
        .catch((error) => {
            // Handle any errors that occurred
            console.error(error)
        })

}

const runProgram2 = () => {
    const promises = activeArchivers.map((archiver) =>
        fetch(`http://${archiver}:4000/cycleinfo/1`, {
            method: 'get',
            headers: { 'Content-Type': 'application/json' },
            timeout: 5000,
        }).then((res) => res.json())
    )

    const tmp = [];

    Promise.allSettled(promises)
        .then((responses) => {
            let i = 0
            for (const response of responses) {
                const archiver = activeArchivers[i]
                if (response.status === 'fulfilled') {
                    const res = response.value
                    if (res && res.cycleInfo && res.cycleInfo.length > 0) {
                        tmp.push(res.cycleInfo[0].counter)
                    } else {
                        console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                    }
                } else {
                    console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                }
                i++
            }
            console.log('tmp', tmp)
        })
        .catch((error) => {
            // Handle any errors that occurred
            console.error(error)
        })

}


const runProgram3 = () => {
    const promises = activeArchivers.map((archiver) =>
        fetch(`http://${archiver}:4000/receipt?startCycle=52400&endCycle=52420&type=tally`, {
            method: 'get',
            headers: { 'Content-Type': 'application/json' },
            timeout: 5000,
        }).then((res) => res.json())
    )

    let tmp1 = [];
    let tmp2 = [];
    let tmp3 = [];
    let tmp4 = [];

    Promise.allSettled(promises)
        .then((responses) => {
            let i = 0
            for (const response of responses) {
                const archiver = activeArchivers[i]
                if (response.status === 'fulfilled') {
                    const res = response.value
                    if (res && res.receipts) {
                        if (i === 0) {
                            tmp1.push(res.receipts)
                        } else if (i === 1) {
                            tmp2.push(res.receipts)
                        } else if (i === 2) {
                            tmp3.push(res.receipts)
                        } else if (i === 3) {
                            tmp4.push(res.receipts)
                        }
                        // tmp.push(res.receipts)
                    } else {
                        console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                    }
                } else {
                    console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                }
                i++
            }
            console.log('tmp1', JSON.stringify(tmp1))
            console.log('tmp2', JSON.stringify(tmp2))
            console.log('tmp3', JSON.stringify(tmp3))
            console.log('tmp4', JSON.stringify(tmp4))
        })
        .catch((error) => {
            // Handle any errors that occurred
            console.error(error)
        })

}

const runProgram4 = () => {
    const promises = activeArchivers.map((archiver) =>
        fetch(`http://${archiver}:4000/totalData`, {
            method: 'get',
            headers: { 'Content-Type': 'application/json' },
            timeout: 5000,
        }).then((res) => res.json())
    )

    let tmp1 = [];
    let tmp2 = [];
    let tmp3 = [];
    let tmp4 = [];

    Promise.allSettled(promises)
        .then((responses) => {
            let i = 0
            for (const response of responses) {
                const archiver = activeArchivers[i]
                if (response.status === 'fulfilled') {
                    const res = response.value
                    if (res) {
                        if (i === 0) {
                            tmp1.push(res)
                        } else if (i === 1) {
                            tmp2.push(res)
                        } else if (i === 2) {
                            tmp3.push(res)
                        } else if (i === 3) {
                            tmp4.push(res)
                        }
                    } else {
                        console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                    }
                } else {
                    console.log(`Archiver ${archiver.ip}:${archiver.port} is not responding`)
                }
                i++
            }
            console.log('tmp1', JSON.stringify(tmp1))
            console.log('tmp2', JSON.stringify(tmp2))
            console.log('tmp3', JSON.stringify(tmp3))
            console.log('tmp4', JSON.stringify(tmp4))
        })
        .catch((error) => {
            // Handle any errors that occurred
            console.error(error)
        })

}

runProgram()

runProgram2()

runProgram3()

runProgram4()