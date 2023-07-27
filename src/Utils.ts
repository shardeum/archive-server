import * as util from 'util'
import * as Logger from './Logger'

export function safeParse<Type>(fallback: Type, json: string, msg?: string): Type {
  if (typeof json === 'object' && json !== null) {
    return json
  }
  try {
    return JSON.parse(json)
  } catch (err) {
    console.warn(msg ? msg : err)
    return fallback
  }
}

// From: https://stackoverflow.com/a/19270021
export function getRandom<T>(arr: T[], n: number): T[] {
  let len = arr.length
  const taken = new Array(len)
  if (n > len) {
    n = len
  }
  const result = new Array(n)
  while (n--) {
    const x = Math.floor(Math.random() * len)
    result[n] = arr[x in taken ? taken[x] : x]
    taken[x] = --len in taken ? taken[len] : len
  }
  return result
}

export type QueryFunction<Node, Response> = (node: Node) => Promise<Response>

export type VerifyFunction<Result> = (result: Result) => boolean

export type EqualityFunction<Value> = (val1: Value, val2: Value) => boolean

export type CompareFunction<Result> = (result: Result) => Comparison

export enum Comparison {
  BETTER,
  EQUAL,
  WORSE,
  ABORT,
}

export interface CompareQueryError<Node> {
  node: Node
  error: string
}

export type CompareFunctionResult<Node> = Array<CompareQueryError<Node>>

export interface SequentialQueryError<Node> {
  node: Node
  error: Error
  response?: unknown
}

export interface SequentialQueryResult<Node> {
  result: unknown
  errors: Array<SequentialQueryError<Node>>
}

export function shuffleArray<T>(array: T[]) {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1))
    ;[array[i], array[j]] = [array[j], array[i]]
  }
}

export const robustPromiseAll = async (promises: any) => {
  // This is how we wrap a promise to prevent it from rejecting directing in the Promise.all and causing a short circuit
  const wrapPromise = async (promise: any) => {
    // We are trying to await the promise, and catching any rejections
    // We return an array, the first index being resolve, and the second being an error
    try {
      const result = await promise
      return [result]
    } catch (e) {
      return [null, e]
    }
  }

  const wrappedPromises = []
  // We wrap all the promises we received and push them to an array to be Promise.all'd
  for (const promise of promises) {
    wrappedPromises.push(wrapPromise(promise))
  }
  const resolved = []
  const errors = []
  // We await the wrapped promises to finish resolving or rejecting
  const wrappedResults = await Promise.all(wrappedPromises)
  // We iterate over all the results, checking if they resolved or rejected
  for (const wrapped of wrappedResults) {
    const [result, err] = wrapped
    // If there was an error, we push it to our errors array
    if (err) {
      errors.push(err)
      continue
    }
    // Otherwise, we were able to resolve so we push it to the resolved array
    resolved.push(result)
  }
  // We return two arrays, one of the resolved promises, and one of the errors
  return [resolved, errors]
}

type TallyItem<N, R> = {
  value: R // Response type is from a template
  count: number
  nodes: N[] // Shardus.Node[] Not using this because robustQuery uses a generic Node, maybe it should be non generic?
}

export async function robustQuery<Node = unknown, Response = unknown>(
  nodes: Node[] = [],
  queryFn: QueryFunction<Node, Response>,
  equalityFn: EqualityFunction<Response> = util.isDeepStrictEqual,
  redundancy = 3,
  shuffleNodes = true
) {
  if (nodes.length === 0) throw new Error('No nodes given.')
  if (typeof queryFn !== 'function') {
    throw new Error(`Provided queryFn ${queryFn} is not a valid function.`)
  }
  if (redundancy < 1) redundancy = 3
  if (redundancy > nodes.length) redundancy = nodes.length

  class Tally {
    winCount: number
    equalFn: EqualityFunction<Response>
    items: Array<{
      value: Response
      count: number
      nodes: Node[]
    }>
    constructor(winCount: number, equalFn: EqualityFunction<Response>) {
      this.winCount = winCount
      this.equalFn = equalFn
      this.items = []
    }
    add(newItem: Response, node: Node) {
      if (newItem === null) return null
      // We search to see if we've already seen this item before
      for (const item of this.items) {
        // If the value of the new item is not equal to the current item, we continue searching
        if (!this.equalFn(newItem, item.value)) continue
        // If the new item is equal to the current item in the list,
        // we increment the current item's counter and add the current node to the list
        item.count++
        item.nodes.push(node)
        // Here we check our win condition if the current item's counter was incremented
        // If we meet the win requirement, we return an array with the value of the item,
        // and the list of nodes who voted for that item
        if (item.count >= this.winCount) {
          return [item.value, item.nodes]
        }
        // Otherwise, if the win condition hasn't been met,
        // We return null to indicate no winner yet
        return null
      }
      // If we made it through the entire items list without finding a match,
      // We create a new item and set the count to 1
      this.items.push({ value: newItem, count: 1, nodes: [node] })
      // Finally, we check to see if the winCount is 1,
      // and return the item we just created if that is the case
      if (this.winCount === 1) return [newItem, [node]]
      else return null
    }
    getHighestCount() {
      if (!this.items.length) return 0
      let highestCount = 0
      for (const item of this.items) {
        if (item.count > highestCount) {
          highestCount = item.count
        }
      }
      return highestCount
    }
    getHighestCountItem() {
      if (!this.items.length) return {}
      let highestCount = 0
      let highestIndex = 0
      let i = 0
      for (const item of this.items) {
        if (item.count > highestCount) {
          highestCount = item.count
          highestIndex = i
        }
        i += 1
      }
      return this.items[highestIndex]
    }
  }
  const responses = new Tally(redundancy, equalityFn)
  let errors = 0

  // [TODO] - Change the way we shuffle the array.
  //     This is not scaleable, if the size of the nodes array is over 100 we should create an array of
  //     indexes and shuffle that. Or maybe use a function that treats the array as a ring and starts at
  //     a random offset in the ring and a random direction. Or use a function that visits every element
  //     in the array once in a random order.
  nodes = [...nodes]
  if (shuffleNodes === true) {
    shuffleArray(nodes)
  }
  const nodeCount = nodes.length

  const queryNodes = async (nodes: Node[]) => {
    // Wrap the query so that we know which node it's coming from
    const wrappedQuery = async (node: any) => {
      const response = await queryFn(node)
      return { response, node }
    }

    // We create a promise for each of the first `redundancy` nodes in the shuffled array
    const queries = []
    for (let i = 0; i < nodes.length; i++) {
      const node = nodes[i]
      queries.push(wrappedQuery(node))
    }
    const [results, errs] = await robustPromiseAll(queries)

    let finalResult
    for (const result of results) {
      const { response, node } = result
      if (responses === null) continue // ignore null response; can be null if we tried to query ourself
      finalResult = responses.add(response, node)
      if (finalResult) break
    }

    for (const err of errs) {
      Logger.mainLogger.error('p2p/Utils:robustQuery:queryNodes:', err)
      errors += 1
    }

    if (!finalResult) return null
    return finalResult
  }

  let finalResult = null
  let tries = 0
  while (!finalResult) {
    tries += 1
    const toQuery = redundancy - responses.getHighestCount()
    if (nodes.length < toQuery) {
      Logger.mainLogger.error('In robustQuery stopping since we ran out of nodes to query.')
      break
    }
    const nodesToQuery = nodes.splice(0, toQuery)
    finalResult = await queryNodes(nodesToQuery)
    if (tries >= 20) {
      Logger.mainLogger.error('In robustQuery stopping after 20 tries.')
      console.trace()
      break
    }
  }
  if (finalResult) {
    // Logger.mainLogger.debug(`In robustQuery stopping since we got a finalResult:${JSON.stringify(finalResult)}`)
    return finalResult
  } else {
    // TODO:  We return the item that had the most nodes reporting it. However, the caller should know
    //        what the count was. We should return [item, count] so that caller gets both.
    //        This change would require also changing all the places it is called.
    Logger.mainLogger.error(
      `Could not get ${redundancy} ${redundancy > 1 ? 'redundant responses' : 'response'} from ${nodeCount} ${
        nodeCount !== 1 ? 'nodes' : 'node'
      }. Encountered ${errors} query errors.`
    )
    console.trace()
    return responses.getHighestCountItem()
  }
}

export async function sequentialQuery<Node = unknown, Response = unknown>(
  nodes: Node[],
  queryFn: QueryFunction<Node, Response>,
  verifyFn: VerifyFunction<Response> = () => true
): Promise<SequentialQueryResult<Node>> {
  nodes = [...nodes]
  shuffleArray(nodes)

  let result: any
  const errors: Array<SequentialQueryError<Node>> = []

  for (const node of nodes) {
    try {
      const response = await queryFn(node)
      if (verifyFn(response) === false) {
        errors.push({
          node,
          error: new Error('Response failed verifyFn'),
          response,
        })
        continue
      }
      result = response
    } catch (error: any) {
      errors.push({
        node,
        error,
      })
    }
  }
  return {
    result,
    errors,
  }
}

export const deepCopy = (obj: any) => {
  if (typeof obj !== 'object') {
    throw Error('Given element is not of type object.')
  }
  return JSON.parse(JSON.stringify(obj))
}

export const insertSorted = function (arr: any[], item: any, comparator?: Function) {
  let i = binarySearch(arr, item, comparator)
  if (i < 0) {
    i = -1 - i
  }
  arr.splice(i, 0, item)
}

export const computeMedian = (arr: number[] = [], sort = true) => {
  if (sort) {
    arr.sort((a: any, b: any) => a - b)
  }
  const len = arr.length
  switch (len) {
    case 0: {
      return 0
    }
    case 1: {
      return arr[0]
    }
    default: {
      const mid = len / 2
      if (len % 2 === 0) {
        return arr[mid]
      } else {
        return (arr[Math.floor(mid)] + arr[Math.ceil(mid)]) / 2
      }
    }
  }
}

export const binarySearch = function (arr: any[], el: any, comparator?: Function) {
  if (comparator == null) {
    // Emulate the default Array.sort() comparator
    comparator = (a: any, b: any) => {
      return a.toString() > b.toString() ? 1 : a.toString() < b.toString() ? -1 : 0
    }
  }
  let m = 0
  let n = arr.length - 1
  while (m <= n) {
    const k = (n + m) >> 1
    const cmp = comparator(el, arr[k])
    if (cmp > 0) {
      m = k + 1
    } else if (cmp < 0) {
      n = k - 1
    } else {
      return k
    }
  }
  return -m - 1
}

// fail safe and fast
// this function will pick non-repeating multiple random elements from an array if given amount n > 1
//(partial) fisher-yates shuffle
export function getRandomItemFromArr<T>(
  arr: T[],
  nodeRejectPercentage: number = 0,
  n: number = 1
): T[] | undefined {
  if (!Array.isArray(arr)) return undefined
  if (arr.length === 0) return undefined

  let result: T[] = new Array(n),
    len: number = arr.length,
    taken: number[] = new Array(len)

  const oldNodesToAvoid = Math.floor(nodeRejectPercentage * len / 100) 

  if (n > len || n <= 1) {
    const randomIndex = Math.floor(Math.random() * arr.length)
    return [arr[randomIndex]]
    // we can throw an error but no
    // let's just return one random item in this case for the safety
  }

  while (n--) {
    const x = Math.floor(oldNodesToAvoid + Math.random() * (len - oldNodesToAvoid))
    result[n] = arr[x in taken ? taken[x] : x]
    taken[x] = --len in taken ? taken[len] : len
  }
  return result
}

export async function sleep(time: number) {
  Logger.mainLogger.debug('sleeping for', time)
  return new Promise((resolve: any) => {
    setTimeout(() => {
      resolve(true)
    }, time)
  })
}

/*
inp is the input object to be checked
def is an object defining the expected input
{name1:type1, name1:type2, ...}
name is the name of the field
type is a string with the first letter of 'string', 'number', 'Bigint', 'boolean', 'array' or 'object'
type can end with '?' to indicate that the field is optional and not required
---
Example of def:
{fullname:'s', age:'s?',phone:'sn'}
---
Returns a string with the first error encountered or and empty string ''.
Errors are: "[name] is required" or "[name] must be, [type]"
*/
export function validateTypes(inp: any, def: any) {
  if (inp === undefined) return 'input is undefined'
  if (inp === null) return 'input is null'
  if (typeof inp !== 'object') return 'input must be object, not ' + typeof inp
  const map: any = {
    string: 's',
    number: 'n',
    boolean: 'b',
    bigint: 'B',
    array: 'a',
    object: 'o',
  }
  const imap: any = {
    s: 'string',
    n: 'number',
    b: 'boolean',
    B: 'bigint',
    a: 'array',
    o: 'object',
  }
  const fields = Object.keys(def)
  for (let name of fields) {
    const types = def[name]
    const opt = types.substr(-1, 1) === '?' ? 1 : 0
    if (inp[name] === undefined && !opt) return name + ' is required'
    if (inp[name] !== undefined) {
      if (inp[name] === null && !opt) return name + ' cannot be null'
      let found = 0
      let be = ''
      for (let t = 0; t < types.length - opt; t++) {
        let it = map[typeof inp[name]]
        it = Array.isArray(inp[name]) ? 'a' : it
        let is = types.substr(t, 1)
        if (it === is) {
          found = 1
          break
        } else be += ', ' + imap[is]
      }
      if (!found) return name + ' must be' + be
    }
  }
  return ''
}

/**
 * Checks whether the given thing is undefined
 */
export function isUndefined(thing: unknown) {
  return typeof thing === 'undefined'
}
