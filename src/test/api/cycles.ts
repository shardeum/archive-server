import { getJson } from '../../P2P'

export async function queryCycles(
  ip: string,
  port: string,
  count: number,
  start: number,
  end: number
): Promise<void> {
  let res: unknown = await getJson(`http://${ip}:${port}/cycleinfo/${count}`)
  console.log(res)

  res = await getJson(`http://${ip}:${port}/cycleinfo?start=${start}&end=${end}`)
  console.log(res)

  // const cycleInfo = res['cycleInfo']

  // for (let i = 0; i < cycleInfo.length; i++) {
  //   console.log(cycleInfo[i])
  // }
}
