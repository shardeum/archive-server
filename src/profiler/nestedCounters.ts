import { Stream } from 'stream'

const NS_PER_SEC = 1e9
import * as fastify from 'fastify'
import { stringifyReduce } from './StringifyReduce'
import * as core from '@shardus/crypto-utils'

// process.hrtime.bigint()

interface NestedCounters {}

type CounterMap = Map<string, CounterNode>
interface CounterNode {
  count: number
  subCounters: CounterMap
}

export let nestedCountersInstance: NestedCounters

class NestedCounters {
  eventCounters: Map<string, CounterNode>
  rareEventCounters: Map<string, CounterNode>
  infLoopDebug: boolean
  server: fastify.FastifyInstance

  constructor(server: fastify.FastifyInstance) {
    // this.sectionTimes = {}
    this.eventCounters = new Map()
    this.rareEventCounters = new Map()
    nestedCountersInstance = this
    this.infLoopDebug = false
    this.server = server
  }

  registerEndpoints() {
    this.server.get('/counts', (req, res) => {
      let outputStr = ''
      let arrayReport = this.arrayitizeAndSort(this.eventCounters)
      outputStr += `${Date.now()}\n`
      outputStr = this.printArrayReport(arrayReport, outputStr, 0)
      res.send(outputStr)
    })
    this.server.get('/counts-reset', (req, res) => {
      this.eventCounters = new Map()
      res.send(`counts reset ${Date.now()}`)
    })

    this.server.get('/debug-inf-loop', (req, res) => {
      res.send('starting inf loop, goodbye')
      let counter = 1
      this.infLoopDebug = true
      while (this.infLoopDebug) {
        let s = 'asdf'
        let s2 = stringifyReduce({ test: [s, s, s, s, s, s, s] })
        let s3 = stringifyReduce({ test: [s2, s2, s2, s2, s2, s2, s2] })
        core.hash(s3)
        counter++
      }
    })

    this.server.get('/debug-inf-loop-off', (req, res) => {
      this.infLoopDebug = false
      res.send('stopping inf loop, who knows if this is possible')
    })
  }

  countEvent(category1: string, category2: string, count: number = 1) {
    let counterMap: CounterMap = this.eventCounters

    let nextNode: CounterNode
    if (counterMap.has(category1) === false) {
      nextNode = { count: 0, subCounters: new Map() }
      counterMap.set(category1, nextNode)
    } else {
      nextNode = <CounterNode>counterMap.get(category1)
    }
    nextNode.count += count
    counterMap = nextNode.subCounters

    //unrolled loop to avoid memory alloc
    category1 = category2
    if (counterMap.has(category1) === false) {
      nextNode = { count: 0, subCounters: new Map() }
      counterMap.set(category1, nextNode)
    } else {
      nextNode = <CounterNode>counterMap.get(category1)
    }
    nextNode.count += count
    counterMap = nextNode.subCounters
  }

  countRareEvent(category1: string, category2: string, count: number = 1) {
    // trigger normal event counter
    this.countEvent(category1, category2, count)

    // start counting rare event
    let counterMap: CounterMap = this.rareEventCounters

    let nextNode: CounterNode = { count: 0, subCounters: new Map() }
    if (!counterMap.has(category1)) {
      nextNode = { count: 0, subCounters: new Map() }
      counterMap.set(category1, nextNode)
    } else {
      nextNode = <CounterNode>counterMap.get(category1)
    }
    nextNode.count += count
    counterMap = nextNode.subCounters

    //unrolled loop to avoid memory alloc
    category1 = category2
    if (counterMap.has(category1) === false) {
      nextNode = { count: 0, subCounters: new Map() }
      counterMap.set(category1, nextNode)
    } else {
      nextNode = <CounterNode>counterMap.get(category1)
    }
    nextNode.count += count
    counterMap = nextNode.subCounters
  }

  arrayitizeAndSort(counterMap: any): any[] {
    let array = []
    for (let key of counterMap.keys()) {
      let valueObj = counterMap.get(key)

      let newValueObj = { key, count: valueObj.count, subArray: null }
      // newValueObj.key = key
      array.push(newValueObj)

      let subArray = []
      if (valueObj.subCounters != null) {
        subArray = this.arrayitizeAndSort(valueObj.subCounters)
      }

      // if (valueObj.count != null && valueObj.logLen != null) {
      //   valueObj.avgLen = valueObj.logLen / valueObj.count
      // }

      // @ts-ignore
      newValueObj.subArray = subArray
      // delete valueObj['subCounters']
    }

    array.sort((a, b) => b.count - a.count)
    return array
  }

  printArrayReport(arrayReport: any[], outputStr: string, indent = 0) {
    let indentText = '___'.repeat(indent)
    for (let item of arrayReport) {
      let { key, count, subArray, avgLen, logLen } = item
      let countStr = `${count}`
      outputStr += `${countStr.padStart(10)} ${indentText} ${key}\n`
      if (subArray != null && subArray.length > 0) {
        outputStr = this.printArrayReport(subArray, outputStr, indent + 1)
      }
    }
    return outputStr
  }
}

export default NestedCounters
