/// <reference types="node" />
import { Server, IncomingMessage, ServerResponse } from 'http';
import fastify = require('fastify');
import * as Crypto from '../Crypto';
import * as NodeList from '../NodeList';
import * as Cycles from './Cycles';
import { Transaction } from './Transactions';
import { Partition } from './Partitions';
export declare enum DataTypes {
    CYCLE = "cycle",
    TRANSACTION = "transaction",
    PARTITION = "partition",
    ALL = "all"
}
interface NewData extends Crypto.TaggedMessage {
    type: Exclude<DataTypes, DataTypes.ALL>;
    data: Cycles.Cycle[] | Transaction[] | Partition[];
}
interface DataSender {
    nodeInfo: NodeList.ConsensusNodeInfo;
    type: DataTypes;
    contactTimeout?: NodeJS.Timeout;
}
export declare function addDataSenders(...senders: DataSender[]): void;
export declare const routePostNewdata: fastify.RouteOptions<Server, IncomingMessage, ServerResponse, fastify.DefaultQuery, fastify.DefaultParams, fastify.DefaultHeaders, NewData>;
export {};
