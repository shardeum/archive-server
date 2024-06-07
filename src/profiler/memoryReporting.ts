import { statisticsInstance } from '../statistics'

import * as os from 'os'
import * as fastify from 'fastify'
import { resourceUsage } from 'process'
import { getActiveNodeCount } from '../NodeList'
import { spawn } from 'child_process'
import * as process from 'process'
import { isDebugMiddleware } from '../DebugMode'
import { Utils as StringUtils } from '@shardus/types'

type CounterMap = Map<string, CounterNode>
interface CounterNode {
  count: number
  subCounters: CounterMap
}

export let memoryReportingInstance: MemoryReporting

export function setMemoryReportingInstance(instance: MemoryReporting): void {
  memoryReportingInstance = instance
}

type MemItem = {
  category: string
  subcat: string
  itemKey: string
  count: number
}

class MemoryReporting {
  report: MemItem[]
  lastCPUTimes: object[]
  server: fastify.FastifyInstance

  constructor(server: fastify.FastifyInstance) {
    this.report = []
    this.server = server
    this.lastCPUTimes = this.getCPUTimes()
  }

  registerEndpoints(): void {
    this.server.get(
      '/memory',
      {
        preHandler: async (_request, reply) => {
          isDebugMiddleware(_request, reply)
        },
      },
      (req, res) => {
        const toMB = 1 / 1000000
        const report = process.memoryUsage()
        let outputStr = ''
        outputStr += `System Memory Report.  Timestamp: ${Date.now()}\n`
        outputStr += `rss: ${(report.rss * toMB).toFixed(2)} MB\n`
        outputStr += `heapTotal: ${(report.heapTotal * toMB).toFixed(2)} MB\n`
        outputStr += `heapUsed: ${(report.heapUsed * toMB).toFixed(2)} MB\n`
        outputStr += `external: ${(report.external * toMB).toFixed(2)} MB\n`
        outputStr += `arrayBuffers: ${(report.arrayBuffers * toMB).toFixed(2)} MB\n\n\n`

        this.gatherReport()
        outputStr = this.reportToStream(this.report, outputStr)
        res.send(outputStr)
      }
    )

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

    this.server.get(
      '/top',
      {
        preHandler: async (_request, reply) => {
          isDebugMiddleware(_request, reply)
        },
      },
      (req, res) => {
        const top = spawn('top', ['-n', '10'])
        top.stdout.on('data', (dataBuffer) => {
          res.send(dataBuffer.toString())
          top.kill()
        })
        top.on('close', (code) => {
          console.log(`child process exited with code ${code}`)
        })
        top.stderr.on('data', (data) => {
          console.log('top command error', data)
          res.send('top command error')
          top.kill()
        })
      }
    )

    this.server.get(
      '/df',
      {
        preHandler: async (_request, reply) => {
          isDebugMiddleware(_request, reply)
        },
      },
      (req, res) => {
        const df = spawn('df')
        df.stdout.on('data', (dataBuffer) => {
          res.send(dataBuffer.toString())
          df.kill()
        })
        df.on('close', (code) => {
          console.log(`child process exited with code ${code}`)
        })
        df.stderr.on('data', (data) => {
          console.log('df command error', data)
          res.send('df command error')
          df.kill()
        })
      }
    )
  }

  updateCpuPercent(): void {
    const cpuPercent = memoryReportingInstance.cpuPercent()
    statisticsInstance.setManualStat('cpuPercent', cpuPercent)
  }

  addToReport(category: string, subcat: string, itemKey: string, count: number): void {
    const obj = { category, subcat, itemKey, count }
    this.report.push(obj)
  }

  reportToStream(report: MemItem[], outputStr: string): string {
    for (const item of report) {
      const { category, subcat, itemKey, count } = item
      let countStr = `${count}`
      if (itemKey === 'cpuPercent' || itemKey === 'cpuAVGPercent') countStr += ' %'
      outputStr += `${countStr.padStart(10)} ${category} ${subcat} ${itemKey}\n`
    }
    return outputStr
  }

  gatherReport(): void {
    this.report = []
    this.stateReport()
    this.systemProcessReport()
  }

  getCPUTimes(): object[] {
    const cpus = os.cpus()
    const times = []

    for (const cpu of cpus) {
      const timeObj = {}
      let total = 0
      for (const [key, value] of Object.entries(cpu.times)) {
        const time = Number(value)
        total += time
        // eslint-disable-next-line security/detect-object-injection
        timeObj[key] = value
      }
      timeObj['total'] = total

      times.push(timeObj)
    }
    return times
  }

  cpuPercent(): number {
    const currentTimes = this.getCPUTimes()

    const deltaTimes = []
    const percentTimes = []

    let percentTotal = 0

    for (let i = 0; i < currentTimes.length; i++) {
      // eslint-disable-next-line security/detect-object-injection
      const currentTimeEntry = currentTimes[i]
      // eslint-disable-next-line security/detect-object-injection
      const lastTimeEntry = this.lastCPUTimes[i]
      const deltaTimeObj = {}
      for (const key of Object.keys(currentTimeEntry)) {
        // eslint-disable-next-line security/detect-object-injection
        deltaTimeObj[key] = currentTimeEntry[key] - lastTimeEntry[key]
      }
      deltaTimes.push(deltaTimeObj)

      for (const key of Object.keys(currentTimeEntry)) {
        // eslint-disable-next-line security/detect-object-injection
        percentTimes[key] = deltaTimeObj[key] / deltaTimeObj['total']
      }

      percentTotal += percentTimes['user'] || 0
      percentTotal += percentTimes['nice'] || 0
      percentTotal += percentTimes['sys'] || 0
    }

    this.lastCPUTimes = currentTimes
    const percentUsed = percentTotal / currentTimes.length
    return percentUsed
  }

  roundTo3decimals(num: number): number {
    return Math.round((num + Number.EPSILON) * 1000) / 1000
  }

  stateReport(): void {
    const numActiveNodes = getActiveNodeCount()
    this.addToReport('P2P', 'Nodelist', 'numActiveNodes', numActiveNodes)
  }

  systemProcessReport(): void {
    this.addToReport('Process', 'CPU', 'cpuPercent', this.roundTo3decimals(this.cpuPercent() * 100))

    const avgCPU = statisticsInstance.getAverage('cpuPercent')
    this.addToReport('Process', 'CPU', 'cpuAVGPercent', this.roundTo3decimals(avgCPU * 100))
    const multiStats = statisticsInstance.getMultiStatReport('cpuPercent')

    multiStats.allVals.forEach(function (val: number, index: number) {
      // eslint-disable-next-line security/detect-object-injection
      multiStats.allVals[index] = Math.round(val * 100)
    })
    multiStats.min = this.roundTo3decimals(multiStats.min * 100)
    multiStats.max = this.roundTo3decimals(multiStats.max * 100)
    multiStats.avg = this.roundTo3decimals(multiStats.avg * 100)

    this.addToReport('Process', 'CPU', `cpu: ${StringUtils.safeStringify(multiStats)}`, 1)

    const report = resourceUsage()
    for (const [key, value] of Object.entries(report)) {
      this.addToReport('Process', 'Details', key, value)
    }
  }
}

export default MemoryReporting
