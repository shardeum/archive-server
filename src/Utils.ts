import * as util from 'util'

export function safeParse<Type>(
  fallback: Type,
  json: string,
  msg?: string
): Type {
  try {
    return JSON.parse(json)
  } catch (err) {
    console.warn(msg ? msg : err)
    return fallback
  }
}


export type QueryFunction<Node, Response> = (node: Node) => Promise<Response>

export type VerifyFunction<Result> = (result: Result) => boolean

export type EqualityFunction<Value> = (val1: Value, val2: Value) => boolean

export type CompareFunction<Result> = (result: Result) => Comparison

export enum Comparison {
  BETTER,
  EQUAL,
  WORSE,
  ABORT 
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
    const wrappedQuery = async (node:any) => {
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
      if (responses === null) continue  // ignore null response; can be null if we tried to query ourself
      finalResult = responses.add(response, node)
      if (finalResult) break
    }

    for (const err of errs) {
      console.log('p2p/Utils:robustQuery:queryNodes:', err)
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
    if (nodes.length < toQuery){
      console.log('In robustQuery stopping since we ran out of nodes to query.')
      break
    }
    const nodesToQuery = nodes.splice(0, toQuery)
    finalResult = await queryNodes(nodesToQuery)
    if (tries>=20){
      console.log('In robustQuery stopping after 20 tries.')
      console.trace()
      break
    }
  }
  if (finalResult) {
   // console.log(`In robustQuery stopping since we got a finalResult:${JSON.stringify(finalResult)}`)
    return finalResult
  }
  else{
  // TODO:  We return the item that had the most nodes reporting it. However, the caller should know
  //        what the count was. We should return [item, count] so that caller gets both.
  //        This change would require also changing all the places it is called.
    console.log(
    `Could not get ${redundancy} ${
      redundancy > 1 ? 'redundant responses' : 'response'
    } from ${nodeCount} ${
      nodeCount !== 1 ? 'nodes' : 'node'
    }. Encountered ${errors} query errors.`
    )
    console.trace()
    return responses.getHighestCountItem()
  }
}

export const deepCopy = (obj: any) => {
  if (typeof obj !== 'object') {
    throw Error('Given element is not of type object.')
  }
  return JSON.parse(JSON.stringify(obj))
}