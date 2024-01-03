import * as path from 'path'
import * as fs from 'fs'
import { Readable } from 'stream'
import { EventEmitter } from 'events'
import * as utils from '../Utils'
import { nestedCountersInstance } from '../profiler/nestedCounters'
export let statisticsInstance: Statistics

interface Statistics {
  intervalDuration: number
  context: unknown
  counterDefs: string[]
  watcherDefs: { [key: string]: () => unknown }
  timerDefs: string[]
  manualStatDefs: string[]
  interval: NodeJS.Timeout | null
  snapshotWriteFns: Array<() => string>
  stream: Readable | null
  streamIsPushable: boolean
  counters: { [key: string]: CounterRing }
  watchers: { [key: string]: WatcherRing }
  timers: { [key: string]: TimerRing }
  manualStats: { [name: string]: ManualRing }
}

interface Config {
  interval?: number
  save?: boolean
  // Add other properties as needed
}

class Statistics extends EventEmitter {
  constructor(
    baseDir: string,
    config: Config,
    {
      counters = [],
      watchers = {},
      timers = [],
      manualStats = [],
    }: {
      counters: string[]
      watchers: { [key: string]: () => unknown }
      timers: string[]
      manualStats: string[]
    },
    context: unknown
  ) {
    super()
    this.intervalDuration = config.interval || 1
    this.intervalDuration = this.intervalDuration * 1000
    this.context = context
    this.counterDefs = counters
    this.watcherDefs = watchers
    this.timerDefs = timers
    this.manualStatDefs = manualStats
    this.initialize()

    this.interval = null
    this.snapshotWriteFns = []
    this.stream = null
    this.streamIsPushable = false
    // Assigning the instance of the Statistics class to the global variable 'statisticsInstance'.
    // This is done to implement the singleton pattern, where only one instance of the Statistics class is created and used throughout the application.
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    statisticsInstance = this
    if (config.save) {
      // Pipe stream to file
      const file = path.join(baseDir, 'statistics.tsv')
      // Ignoring the non-literal fs filename rule because 'baseDir' is a trusted source and not user input.
      // eslint-disable-next-line security/detect-non-literal-fs-filename
      const fileWriteStream = fs.createWriteStream(file)
      const statsReadStream = this.getStream()
      statsReadStream.pipe(fileWriteStream)
    }
  }

  initialize(): void {
    this.counters = this._initializeCounters(this.counterDefs)
    this.watchers = this._initializeWatchers(this.watcherDefs, this.context)
    this.timers = this._initializeTimers(this.timerDefs)
    this.manualStats = this._initializeManualStats(this.manualStatDefs)
  }

  getStream(): Readable | null {
    this.stream = new Readable()
    this.stream._read = (): void => {
      this.streamIsPushable = true
    }
    return this.stream
  }

  writeOnSnapshot(writeFn: () => string, context: unknown): void {
    this.snapshotWriteFns.push(writeFn.bind(context))
  }

  startSnapshots(): void {
    console.log('Starting statistics snapshots...')
    const tabSeperatedHeaders = 'Name\tValue\tTime\n'
    this._pushToStream(tabSeperatedHeaders)
    if (!this.interval) this.interval = setInterval(this._takeSnapshot.bind(this), this.intervalDuration)
  }

  stopSnapshots(): void {
    if (this.interval) clearInterval(this.interval)
    this.interval = null
  }

  // Increments the given CounterRing's count
  incrementCounter(counterName: string): void {
    // eslint-disable-next-line security/detect-object-injection
    const counter = this.counters[counterName]
    if (!counter) throw new Error(`Counter '${counterName}' is undefined.`)
    counter.increment()
    nestedCountersInstance.countEvent('statistics', counterName)
  }

  setManualStat(manualStatName: string, value: number): void {
    // eslint-disable-next-line security/detect-object-injection
    const ring = this.manualStats[manualStatName]
    if (!ring) throw new Error(`manualStat '${manualStatName}' is undefined.`)
    ring.manualSetValue(value)
    //nestedCountersInstance.countEvent('statistics', manualStatName)
  }

  // Returns the current count of the given CounterRing
  getCurrentCount(counterName: string): number {
    // eslint-disable-next-line security/detect-object-injection
    const counter = this.counters[counterName]
    if (!counter) throw new Error(`Counter '${counterName}' is undefined.`)
    return counter.count
  }

  // Returns the current total of the given CounterRing
  getCounterTotal(counterName: string): number {
    // eslint-disable-next-line security/detect-object-injection
    const counter = this.counters[counterName]
    if (!counter) throw new Error(`Counter '${counterName}' is undefined.`)
    return counter.total
  }

  // Returns the result of the given WatcherRings watchFn
  getWatcherValue(watcherName: string): unknown {
    // eslint-disable-next-line security/detect-object-injection
    const watcher = this.watchers[watcherName]
    if (!watcher) throw new Error(`Watcher '${watcherName}' is undefined.`)
    return watcher.watchFn()
  }

  // Starts an entry for the given id in the given TimerRing
  startTimer(timerName: string, id: string): void {
    // eslint-disable-next-line security/detect-object-injection
    const timer = this.timers[timerName]
    if (!timer) throw new Error(`Timer '${timerName}' is undefined.`)
    timer.start(id)
  }

  // Stops an entry for the given id in the given TimerRing
  stopTimer(timerName: string, id: string): void {
    // eslint-disable-next-line security/detect-object-injection
    const timer = this.timers[timerName]
    if (!timer) throw new Error(`Timer '${timerName}' is undefined.`)
    timer.stop(id)
  }

  // Returns the current average of all elements in the given WatcherRing, CounterRing, or TimerRing
  getAverage(name: string): number {
    const ringHolder =
      this.counters[name] || this.watchers[name] || this.timers[name] || this.manualStats[name] // eslint-disable-line security/detect-object-injection
    if (!ringHolder.ring) throw new Error(`Ring holder '${name}' is undefined.`)
    return ringHolder.ring.average()
  }

  getMultiStatReport(name: string): { min: number; max: number; avg: number; allVals: number[] } {
    const ringHolder =
      this.counters[name] || this.watchers[name] || this.timers[name] || this.manualStats[name] // eslint-disable-line security/detect-object-injection
    if (!ringHolder.ring) throw new Error(`Ring holder '${name}' is undefined.`)

    return ringHolder.ring.multiStats()
  }

  // Returns the value of the last element of the given WatcherRing, CounterRing, or TimerRing
  getPreviousElement(name: string): unknown {
    const ringHolder = this.counters[name] || this.watchers[name] || this.timers[name] // eslint-disable-line security/detect-object-injection
    if (!ringHolder.ring) throw new Error(`Ring holder '${name}' is undefined.`)
    return ringHolder.ring.previous()
  }

  _initializeCounters(counterDefs: string[] = []): { [key: string]: CounterRing } {
    const counters: { [key: string]: CounterRing } = {}
    for (const name of counterDefs) {
      // eslint-disable-next-line security/detect-object-injection
      counters[name] = new CounterRing(60)
    }
    return counters
  }

  _initializeWatchers(
    watcherDefs: { [key: string]: () => unknown } = {},
    context: unknown
  ): { [key: string]: WatcherRing } {
    const watchers: { [key: string]: WatcherRing } = {}
    for (const name in watcherDefs) {
      // eslint-disable-next-line security/detect-object-injection
      const watchFn = watcherDefs[name]
      // eslint-disable-next-line security/detect-object-injection
      watchers[name] = new WatcherRing(60, watchFn, context)
    }
    return watchers
  }

  _initializeTimers(timerDefs: string[] = []): { [key: string]: TimerRing } {
    const timers: { [key: string]: TimerRing } = {}
    for (const name of timerDefs) {
      // eslint-disable-next-line security/detect-object-injection
      timers[name] = new TimerRing(60)
    }
    return timers
  }

  _initializeManualStats(counterDefs: string[] = []): { [key: string]: ManualRing } {
    const manualStats: { [key: string]: ManualRing } = {}
    for (const name of counterDefs) {
      // eslint-disable-next-line security/detect-object-injection
      manualStats[name] = new ManualRing(60) //should it be a config
    }
    return manualStats
  }

  _takeSnapshot(): void {
    const time = new Date().toISOString()
    let tabSeperatedValues = ''

    for (const counter in this.counters) {
      // eslint-disable-next-line security/detect-object-injection
      this.counters[counter].snapshot()
      tabSeperatedValues += `${counter}-average\t${this.getAverage(counter)}\t${time}\n`
      tabSeperatedValues += `${counter}-total\t${this.getCounterTotal(counter)}\t${time}\n`
    }
    for (const watcher in this.watchers) {
      // eslint-disable-next-line security/detect-object-injection
      this.watchers[watcher].snapshot()
      tabSeperatedValues += `${watcher}-average\t${this.getAverage(watcher)}\t${time}\n`
      tabSeperatedValues += `${watcher}-value\t${this.getWatcherValue(watcher)}\t${time}\n`
    }
    for (const timer in this.timers) {
      // eslint-disable-next-line security/detect-object-injection
      this.timers[timer].snapshot()
      tabSeperatedValues += `${timer}-average\t${this.getAverage(timer) / 1000}\t${time}\n`
    }

    for (const writeFn of this.snapshotWriteFns) {
      tabSeperatedValues += writeFn()
    }

    this._pushToStream(tabSeperatedValues)
    this.emit('snapshot')
  }

  _pushToStream(data: unknown): void {
    if (this.stream && this.streamIsPushable) {
      this.streamIsPushable = this.stream.push(data)
    }
  }
}

interface Ring {
  elements: unknown[]
  index: number
}

class Ring {
  constructor(length: number) {
    this.elements = new Array(length)
    this.index = 0
  }
  multiStats(): { min: number; max: number; avg: number; allVals: number[] } {
    let sum = 0
    let total = 0
    let min = Number.MAX_VALUE
    let max = Number.MIN_VALUE
    const allVals = []
    for (const element of this.elements) {
      if (_exists(element)) {
        const val = Number(element)
        sum += val
        total++

        if (val < min) {
          min = val
        }
        if (val > max) {
          max = val
        }
        allVals.push(val)
      }
    }
    const avg = total > 0 ? sum / total : 0
    return { min, max, avg, allVals }
  }
  save(value: unknown): void {
    this.elements[this.index] = value
    this.index = ++this.index % this.elements.length
  }
  average(): number {
    let sum = 0
    let total = 0
    console.log('elements', this.elements)
    for (const element of this.elements) {
      if (_exists(element)) {
        sum += Number(element)
        total++
      }
    }
    return total > 0 ? sum / total : 0
  }
  previous(): number {
    const prevIndex = (this.index < 1 ? this.elements.length : this.index) - 1
    // eslint-disable-next-line security/detect-object-injection
    return (this.elements[prevIndex] as number) || 0
  }
}

interface CounterRing {
  count: number
  total: number
  ring: Ring
}

class CounterRing {
  constructor(length: number) {
    this.count = 0
    this.total = 0
    this.ring = new Ring(length)
  }
  increment(): void {
    ++this.count
    ++this.total
  }
  snapshot(): void {
    this.ring.save(this.count)
    this.count = 0
  }
}

interface WatcherRing {
  watchFn: () => unknown
  ring: Ring
}

class WatcherRing {
  constructor(length: number, watchFn: (...args: unknown[]) => unknown, context: unknown) {
    this.watchFn = watchFn.bind(context)
    this.ring = new Ring(length)
  }
  snapshot(): void {
    const value = this.watchFn()
    this.ring.save(value)
  }
}

interface TimerRing {
  ids: Record<string, number>
  ring: Ring
}

class TimerRing {
  constructor(length: number) {
    this.ids = {}
    this.ring = new Ring(length)
  }
  start(id: string): void {
    // eslint-disable-next-line security/detect-object-injection
    if (!this.ids[id]) {
      // eslint-disable-next-line security/detect-object-injection
      this.ids[id] = Date.now()
    }
  }
  stop(id: string): void {
    // eslint-disable-next-line security/detect-object-injection
    const entry = this.ids[id]
    if (entry) {
      // eslint-disable-next-line security/detect-object-injection
      delete this.ids[id]
    }
  }
  snapshot(): void {
    // Calc median duration of all entries in ids
    const durations: number[] = []
    for (const id in this.ids) {
      // eslint-disable-next-line security/detect-object-injection
      const startTime = this.ids[id]
      // console.log('START_TIME ', startTime, 'ID', id)
      const duration = Date.now() - startTime
      utils.insertSorted(durations, duration, (a: number, b: number) => a - b)
    }
    const median = utils.computeMedian(durations, false)
    // Save median
    this.ring.save(median)
  }
}

interface ManualRing {
  ring: Ring
}

class ManualRing {
  constructor(length: number) {
    this.ring = new Ring(length)
  }
  manualSetValue(value: unknown): void {
    this.ring.save(value)
  }
  snapshot(): void {
    // intentionally left blank
  }
}

/**
 * Check for a variable that is not undefined or null
 * @param thing The parameter to check
 */
function _exists(thing: unknown): boolean {
  return typeof thing !== 'undefined' && thing !== null
}

export default Statistics
