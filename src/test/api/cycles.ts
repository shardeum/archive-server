import { getJson } from '../../P2P'

export async function queryCycles(
  ip: any,
  port: any,
  count: number,
  start: number,
  end: number
) {
  let res: any = await getJson(`http://${ip}:${port}/cycleinfo/${count}`)
  console.log(res)

  res = await getJson(
    `http://${ip}:${port}/cycleinfo?start=${start}&end=${end}`
  )
  console.log(res)

  // const cycleInfo = res['cycleInfo']

  // for (let i = 0; i < cycleInfo.length; i++) {
  //   console.log(cycleInfo[i])
  // }
}
