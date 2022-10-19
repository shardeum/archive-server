import { getJson } from '../../P2P'

export async function queryNodes(ip: any, port: any, start: number, end: number) {
  let result: any = await getJson(`http://${ip}:${port}/nodelist`)
  console.log(result)

  result = await getJson(`http://${ip}:${port}/full-nodelist`)
  console.log(result)

  result = await getJson(`http://${ip}:${port}/nodeids`)
  console.log(result)

  result = await getJson(`http://${ip}:${port}/lost?start=${start}&end=${end}`)
  console.log(result)

  result = await getJson(`http://${ip}:${port}/nodeinfo`)
  console.log(result)
}
