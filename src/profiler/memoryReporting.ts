import { statisticsInstance } from "../statistics";

const NS_PER_SEC = 1e9

const os = require('os')
import * as fastify from 'fastify'

const process = require('process')

// process.hrtime.bigint()

interface MemoryReporting { }

type CounterMap = Map<string, CounterNode>
interface CounterNode {
  count: number
  subCounters: CounterMap
}

export let memoryReportingInstance: MemoryReporting

type MemItem = {
  category: string
  subcat: string
  itemKey: string
  count: number
}

class MemoryReporting {
  report: MemItem[]
  lastCPUTimes: any[]
  server: fastify.FastifyInstance

  constructor(server: fastify.FastifyInstance) {
    memoryReportingInstance = this
    this.report = []
    this.server = server
    this.lastCPUTimes = this.getCPUTimes()
  }

  registerEndpoints() {
    this.server.get('/memory', (req, res) => {
      let toMB = 1 / 1000000
      let report = process.memoryUsage()
      let outputStr = ''
      outputStr += `System Memory Report.  Timestamp: ${Date.now()}\n`
      outputStr += `rss: ${(report.rss * toMB).toFixed(2)} MB\n`
      outputStr += `heapTotal: ${(report.heapTotal * toMB).toFixed(2)} MB\n`
      outputStr += `heapUsed: ${(report.heapUsed * toMB).toFixed(2)} MB\n`
      outputStr += `external: ${(report.external * toMB).toFixed(2)} MB\n`
      outputStr += `arrayBuffers: ${(report.arrayBuffers * toMB).toFixed(
        2
      )} MB\n\n\n`

      this.gatherReport()
      outputStr = this.reportToStream(this.report, outputStr, 0)
      res.send(outputStr)
    })

    // this.server.get('memory-gc', (req, res) => {
    //     res.write(`System Memory Report.  Timestamp: ${Date.now()}\n`)
    //     try {
    //         if (global.gc) {
    //             global.gc();
    //             res.write('garbage collected!');
    //         } else {
    //             res.write('No access to global.gc.  run with node --expose-gc');
    //         }
    //     } catch (e) {
    //         res.write('ex:No access to global.gc.  run with node --expose-gc');
    //     }
    //     res.end()
    // })
  }

  updateCpuPercent() {
    let cpuPercent = memoryReportingInstance.cpuPercent()
    statisticsInstance.setManualStat('cpuPercent', cpuPercent)
  }

  addToReport(
    category: string,
    subcat: string,
    itemKey: string,
    count: number
  ) {
    let obj = { category, subcat, itemKey, count }
    this.report.push(obj)
  }

  reportToStream(report: MemItem[], outputStr: string, indent: any) {
    let indentText = '___'.repeat(indent)
    for (let item of report) {
      let { category, subcat, itemKey, count } = item
      let countStr = `${count}`
      if (itemKey === 'cpuPercent' || itemKey === 'cpuAVGPercent') countStr += ' %'
      outputStr += `${countStr.padStart(10)} ${category} ${subcat} ${itemKey}\n`
    }
    return outputStr
  }

  gatherReport() {
    this.report = []
    this.systemProcessReport()
  }

  getCPUTimes() {
    const cpus = os.cpus()
    let times = []

    for (let cpu of cpus) {
      let timeObj: any = {}
      let total = 0
      for (const [key, value] of Object.entries(cpu.times)) {
        let time = Number(value)
        total += time
        timeObj[key] = value
      }
      timeObj['total'] = total

      times.push(timeObj)
    }
    return times
  }

  cpuPercent() {
    let currentTimes = this.getCPUTimes()

    let deltaTimes: any = []
    let percentTimes: any = []

    let percentTotal = 0

    for (let i = 0; i < currentTimes.length; i++) {
      const currentTimeEntry = currentTimes[i]
      const lastTimeEntry = this.lastCPUTimes[i]
      let deltaTimeObj: any = {}
      for (const [key, value] of Object.entries(currentTimeEntry)) {
        deltaTimeObj[key] = currentTimeEntry[key] - lastTimeEntry[key]
      }
      deltaTimes.push(deltaTimeObj)

      for (const [key, value] of Object.entries(currentTimeEntry)) {
        percentTimes[key] = deltaTimeObj[key] / deltaTimeObj['total']
      }

      percentTotal += percentTimes['user'] || 0
      percentTotal += percentTimes['nice'] || 0
      percentTotal += percentTimes['sys'] || 0
    }

    this.lastCPUTimes = currentTimes
    let percentUsed = percentTotal / currentTimes.length
    return percentUsed
  }

  systemProcessReport() {
    this.addToReport('Process', 'CPU', 'cpuPercent', this.cpuPercent() * 100)
    let avgCPU = statisticsInstance.getAverage('cpuPercent')
    this.addToReport('Process', 'CPU', 'cpuAVGPercent', avgCPU * 100)
    let report = process.resourceUsage()
    for (const [key, value] of Object.entries(report)) {
      this.addToReport('Process', 'Details', key, value as number)
    }
  }
}

export default MemoryReporting
