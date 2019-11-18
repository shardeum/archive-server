"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const knex = require("knex");
let db;
async function initStorage(dbFile) {
    // Connect to a db
    db = knex({
        client: 'sqlite3',
        connection: {
            filename: dbFile,
        },
    });
    // Create a cycles table if it doesn't exist
    if ((await db.schema.hasTable('cycles')) === false) {
        await db.schema.createTable('cycles', table => {
            table.bigInteger('counter');
            table.json('certificate');
            table.text('previous');
            table.text('marker');
            table.bigInteger('start');
            table.bigInteger('duration');
            table.bigInteger('active');
            table.bigInteger('desired');
            table.bigInteger('expired');
            table.json('joined');
            table.json('joinedArchivers');
            table.json('activated');
            table.json('removed');
            table.json('returned');
            table.json('lost');
            table.json('refuted');
            table.json('apoptosized');
        });
    }
}
exports.initStorage = initStorage;
async function storeCycle(cycle) {
    cycle.joined = typeof cycle.joined !== 'string' ? JSON.stringify(cycle.joined) : cycle.joined;
    cycle.joinedArchivers = typeof cycle.joinedArchivers !== 'string' ? JSON.stringify(cycle.joinedArchivers) : cycle.joinedArchivers;
    cycle.activated = typeof cycle.activated !== 'string' ? JSON.stringify(cycle.activated) : cycle.activated;
    cycle.removed = typeof cycle.removed !== 'string' ? JSON.stringify(cycle.removed) : cycle.removed;
    cycle.returned = typeof cycle.returned !== 'string' ? JSON.stringify(cycle.returned) : cycle.returned;
    cycle.lost = typeof cycle.lost !== 'string' ? JSON.stringify(cycle.lost) : cycle.lost;
    cycle.refuted = typeof cycle.refuted !== 'string' ? JSON.stringify(cycle.refuted) : cycle.refuted;
    cycle.apoptosized = typeof cycle.apoptosized !== 'string' ? JSON.stringify(cycle.apoptosized) : cycle.apoptosized;
    await db('cycles').insert(cycle);
}
exports.storeCycle = storeCycle;
//# sourceMappingURL=Storage.js.map