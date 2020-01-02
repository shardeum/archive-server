/// <reference types="node" />
import { Server, IncomingMessage, ServerResponse } from 'http';
import { EventEmitter } from 'events';
import fastify = require('fastify');
import * as Crypto from '../Crypto';
import * as NodeList from '../NodeList';
import { Cycle } from './Cycles';
import { Transaction } from './Transactions';
import { Partition } from './Partitions';
export declare type ValidTypes = Cycle | Transaction | Partition;
export declare enum TypeNames {
    CYCLE = "CYCLE",
    TRANSACTION = "TRANSACTION",
    PARTITION = "PARTITION"
}
export declare type TypeName<T extends ValidTypes> = T extends Cycle ? TypeNames.CYCLE : T extends Transaction ? TypeNames.TRANSACTION : TypeNames.PARTITION;
export declare type TypeIndex<T extends ValidTypes> = T extends Cycle ? Cycle['counter'] : T extends Transaction ? Transaction['id'] : Partition['hash'];
export interface DataRequest<T extends ValidTypes> {
    type: TypeName<T>;
    lastData: TypeIndex<T>;
}
interface DataResponse<T extends ValidTypes> {
    type: TypeName<T>;
    data: T[];
}
export declare function createDataRequest<T extends ValidTypes>(type: TypeName<T>, lastData: TypeIndex<T>, recipientPk: Crypto.types.publicKey): DataRequest<T> & Crypto.TaggedMessage;
interface DataSender<T extends ValidTypes> {
    nodeInfo: NodeList.ConsensusNodeInfo;
    type: TypeName<T>;
    contactTimeout?: NodeJS.Timeout;
}
export declare const emitter: EventEmitter;
export declare function addDataSenders(...senders: Array<DataSender<ValidTypes>>): void;
export declare const routePostNewdata: fastify.RouteOptions<Server, IncomingMessage, ServerResponse, fastify.DefaultQuery, fastify.DefaultParams, fastify.DefaultHeaders, DataResponse<ValidTypes> & Crypto.TaggedMessage>;
export {};
