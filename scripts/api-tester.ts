import * as crypto from '@shardus/crypto-utils'
import fetch from 'node-fetch'

crypto.init('69fa4195670576c0160d660c3be36556ff8d504725be8a59b5a96509e0c994bc')

const devAccount = {
  publicKey: '',
  secretKey: '',
}

const data: any = {
  count: 100,
  sender: devAccount.publicKey,
}
crypto.signObj(data, devAccount.secretKey, devAccount.publicKey)
// console.log(data)

fetch('http://127.0.0.1:4000/cycleinfo', {
  method: 'post',
  body: JSON.stringify(data),
  headers: { 'Content-Type': 'application/json' },
  timeout: 2000,
})
  .then(async (res) => {
    if (res.ok) console.log(await res.json())
    else console.log(res.status)
  })
  .catch((err) => {
    console.log(err)
  })
