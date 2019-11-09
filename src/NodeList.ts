import { config } from './Config';
import { crypto } from './Crypto';

export interface NodeInfo {
  ip: string;
  port: number;
}

export interface SignedList {
  nodeList: NodeInfo[];
}

const list: NodeInfo[] = [];

export function isFirst(): boolean {
  return list.length <= 0;
}

export function addNode(node: NodeInfo) {
  list.push(node);
}

export function getSignedList(): SignedList {
  const signedList = {
    nodeList: list,
  };
  crypto.signObj(
    signedList,
    config.ARCHIVER_SECRET_KEY,
    config.ARCHIVER_PUBLIC_KEY
  );
  return signedList;
}
