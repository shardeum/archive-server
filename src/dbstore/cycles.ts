import * as db from './sqlite3storage';
import { extractValues } from './sqlite3storage';
import { P2P, StateManager } from '@shardus/types';

export interface Cycle {
    counter: number;
    cycleRecord: P2P.CycleCreatorTypes.CycleRecord;
    cycleMarker: StateManager.StateMetaDataTypes.CycleMarker;
}

export async function insertCycle(cycle: Cycle) {
    try {
        const fields = Object.keys(cycle).join(', ');
        const placeholders = Object.keys(cycle).fill('?').join(', ');
        const values = extractValues(cycle);
        let sql =
            'INSERT OR REPLACE INTO cycles (' +
            fields +
            ') VALUES (' +
            placeholders +
            ')';
        await db.run(sql, values);
        console.log(
            'Successfully inserted Cycle',
            cycle.cycleRecord.counter,
            cycle.cycleMarker
        );
    } catch (e) {
        console.log(e);
        console.log(
            'Unable to insert cycle or it is already stored in to database',
            cycle.cycleRecord.counter,
            cycle.cycleMarker
        );
    }
}

export async function updateCycle(marker: string, cycle: Cycle) {
    try {
        const sql = `UPDATE cycles SET counter = $counter, cycleRecord = $cycleRecord WHERE cycleMarker = $marker `;
        await db.run(sql, {
            $counter: cycle.counter,
            $cycleRecord: cycle.cycleRecord && JSON.stringify(cycle.cycleRecord),
            $marker: marker,
        });
        console.log(
            'Updated cycle for counter',
            cycle.cycleRecord.counter,
            cycle.cycleMarker
        );
    } catch (e) {
        console.log(e);
        console.log('Unable to update Cycle', cycle.cycleMarker);
    }
}

export async function queryCycleByMarker(marker: string) {
    try {
        const sql = `SELECT * FROM cycles WHERE cycleMarker=? LIMIT 1`;
        const cycle: any = await db.get(sql, [marker]);
        if (cycle) {
            if (cycle.cycleRecord)
                cycle.cycleRecord = JSON.parse(cycle.cycleRecord);
        }
        console.log('cycle marker', cycle)
        return cycle;
    } catch (e) {
        console.log(e);
    }
}

export async function queryLatestCycleRecords(count) {
    try {
        const sql = `SELECT * FROM cycles ORDER BY counter DESC LIMIT ${count ? count : 100}`;
        let cycleRecords: any = await db.all(sql);
        if (cycleRecords.length > 0) {
            cycleRecords = cycleRecords.map((cycleRecord: any) => {
                if (cycleRecord.cycleRecord)
                    cycleRecord.cycleRecord = JSON.parse(cycleRecord.cycleRecord);
                return cycleRecord.cycleRecord;
            });
        }
        console.log('cycle latest', cycleRecords);
        return cycleRecords;
    } catch (e) {
        console.log(e);
    }
}

export async function queryCycleRecordsBetween(start: number, end: number) {
    try {
        const sql = `SELECT * FROM cycles WHERE counter BETWEEN ? AND ? ORDER BY counter ASC`;
        let cycleRecords: any = await db.all(sql, [start, end]);
        if (cycleRecords.length > 0) {
            cycleRecords = cycleRecords.map((cycleRecord: any) => {
                if (cycleRecord.cycleRecord)
                    cycleRecord.cycleRecord = JSON.parse(cycleRecord.cycleRecord);
                return cycleRecord.cycleRecord;
            });
        }
        console.log('cycle between', cycleRecords);
        return cycleRecords;
    } catch (e) {
        console.log(e);
    }
}

export async function queryCyleCount() {
    let cycles;
    try {
        const sql = `SELECT COUNT(*) FROM cycles`;
        cycles = await db.get(sql, []);
    } catch (e) {
        console.log(e);
    }
    console.log('Cycle count', cycles);
    if (cycles) cycles = cycles['COUNT(*)'];
    else cycles = 0;
    return cycles;
}