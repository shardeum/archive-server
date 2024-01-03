import { nestedCountersInstance } from './nestedCounters'
import * as fastify from 'fastify'

const profilerSelfReporting = false

interface Profiler {
  sectionTimes: object
  // instance: Profiler
}

export let profilerInstance: Profiler

export function setProfilerInstance(instance: Profiler): void {
  profilerInstance = instance
}

class Profiler {
  sectionTimes: object
  eventCounters: Map<string, Map<string, number>>
  stackHeight: number
  netInternalStackHeight: number
  netExternalStackHeight: number
  server: fastify.FastifyInstance

  constructor(server: fastify.FastifyInstance) {
    this.sectionTimes = {}
    this.eventCounters = new Map()
    this.stackHeight = 0
    this.netInternalStackHeight = 0
    this.netExternalStackHeight = 0
    this.server = server

    this.profileSectionStart('_total', true)
    this.profileSectionStart('_internal_total', true)
  }

  registerEndpoints(): void {
    this.server.get('/perf', (req, res) => {
      const result = this.printAndClearReport()
      res.send(result)
    })
  }

  profileSectionStart(sectionName: string, internal = false): void {
    // eslint-disable-next-line security/detect-object-injection
    let section = this.sectionTimes[sectionName]

    if (section != null && section.started === true) {
      if (profilerSelfReporting) nestedCountersInstance.countEvent('profiler-start-error', sectionName)
      return
    }

    if (section == null) {
      section = { name: sectionName, total: BigInt(0), c: 0, internal }
      // eslint-disable-next-line security/detect-object-injection
      this.sectionTimes[sectionName] = section
    }

    section.start = process.hrtime.bigint()
    section.started = true
    section.c++

    if (internal === false) {
      nestedCountersInstance.countEvent('profiler', sectionName)

      this.stackHeight++
      if (this.stackHeight === 1) {
        this.profileSectionStart('_totalBusy', true)
        this.profileSectionStart('_internal_totalBusy', true)
      }
      if (sectionName === 'net-internl') {
        this.netInternalStackHeight++
        if (this.netInternalStackHeight === 1) {
          this.profileSectionStart('_internal_net-internl', true)
        }
      }
      if (sectionName === 'net-externl') {
        this.netExternalStackHeight++
        if (this.netExternalStackHeight === 1) {
          this.profileSectionStart('_internal_net-externl', true)
        }
      }
    }
  }

  profileSectionEnd(sectionName: string, internal = false): void {
    // eslint-disable-next-line security/detect-object-injection
    const section = this.sectionTimes[sectionName]
    if (section == null || section.started === false) {
      if (profilerSelfReporting) nestedCountersInstance.countEvent('profiler-end-error', sectionName)
      return
    }

    section.end = process.hrtime.bigint()

    section.total += section.end - section.start
    section.started = false

    if (internal === false) {
      if (profilerSelfReporting) nestedCountersInstance.countEvent('profiler-end', sectionName)

      this.stackHeight--
      if (this.stackHeight === 0) {
        this.profileSectionEnd('_totalBusy', true)
        this.profileSectionEnd('_internal_totalBusy', true)
      }
      if (sectionName === 'net-internl') {
        this.netInternalStackHeight--
        if (this.netInternalStackHeight === 0) {
          this.profileSectionEnd('_internal_net-internl', true)
        }
      }
      if (sectionName === 'net-externl') {
        this.netExternalStackHeight--
        if (this.netExternalStackHeight === 0) {
          this.profileSectionEnd('_internal_net-externl', true)
        }
      }
    }
  }

  //TODO - this is not used anywhere
  // getTotalBusyInternal(): any {
  //   if (profilerSelfReporting) nestedCountersInstance.countEvent('profiler-note', 'getTotalBusyInternal')

  //   this.profileSectionEnd('_internal_total', true)
  //   const internalTotalBusy = this.sectionTimes['_internal_totalBusy']
  //   const internalTotal = this.sectionTimes['_internal_total']
  //   const internalNetInternl = this.sectionTimes['_internal_net-internl']
  //   const internalNetExternl = this.sectionTimes['_internal_net-externl']
  //   let duty = BigInt(0)
  //   let netInternlDuty = BigInt(0)
  //   let netExternlDuty = BigInt(0)
  //   if (internalTotalBusy != null && internalTotal != null) {
  //     if (internalTotal.total > BigInt(0)) {
  //       duty = (BigInt(100) * internalTotalBusy.total) / internalTotal.total
  //     }
  //   }
  //   if (internalNetInternl != null && internalTotal != null) {
  //     if (internalTotal.total > BigInt(0)) {
  //       netInternlDuty = (BigInt(100) * internalNetInternl.total) / internalTotal.total
  //     }
  //   }
  //   if (internalNetExternl != null && internalTotal != null) {
  //     if (internalTotal.total > BigInt(0)) {
  //       netExternlDuty = (BigInt(100) * internalNetExternl.total) / internalTotal.total
  //     }
  //   }
  //   this.profileSectionStart('_internal_total', true)

  //   //clear these timers
  //   internalTotal.total = BigInt(0)
  //   internalTotalBusy.total = BigInt(0)
  //   if (internalNetInternl) internalNetInternl.total = BigInt(0)
  //   if (internalNetExternl) internalNetExternl.total = BigInt(0)

  //   return {
  //     duty: Number(duty) * 0.01,
  //     netInternlDuty: Number(netInternlDuty) * 0.01,
  //     netExternlDuty: Number(netExternlDuty) * 0.01,
  //   }
  // }

  clearTimes(): void {
    for (const key in this.sectionTimes) {
      if (key.startsWith('_internal')) continue

      if (Object.prototype.hasOwnProperty.call(this.sectionTimes, key)) {
        // eslint-disable-next-line security/detect-object-injection
        const section = this.sectionTimes[key]
        section.total = BigInt(0)
      }
    }
  }

  printAndClearReport(): string {
    this.profileSectionEnd('_total', true)

    let result = 'Profile Sections:\n'
    const divider = BigInt(1e6) // will get us ms

    const totalSection = this.sectionTimes['_total']
    console.log('totalSection from printAndClearReport', totalSection)

    const lines = []
    for (const key in this.sectionTimes) {
      if (key.startsWith('_internal')) continue

      if (Object.prototype.hasOwnProperty.call(this.sectionTimes, key)) {
        // eslint-disable-next-line security/detect-object-injection
        const section = this.sectionTimes[key]

        // result += `${section.name}: total ${section.total /
        //   divider} avg:${section.total / (divider * BigInt(section.c))} ,  ` // ${section.total} :

        let duty = BigInt(0)
        if (totalSection.total > BigInt(0)) {
          duty = (BigInt(100) * section.total) / totalSection.total
        }
        const totalMs = section.total / divider
        const dutyStr = `${duty}`.padStart(4)
        const totalStr = `${totalMs}`.padStart(13)
        const line = `${dutyStr}% ${section.name.padEnd(30)}, ${totalStr}ms, #:${section.c}`
        //section.total = BigInt(0)

        lines.push({ line, totalMs })
      }
    }

    lines.sort((l1, l2) => Number(l2.totalMs - l1.totalMs))

    result = result + lines.map((line) => line.line).join('\n')

    this.clearTimes()

    this.profileSectionStart('_total', true)
    return result
  }
}

export default Profiler
