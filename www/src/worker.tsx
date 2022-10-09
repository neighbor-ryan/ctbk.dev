import {WorkerHttpvfs} from "sql.js-httpvfs/dist/db";
import {createDbWorker} from "sql.js-httpvfs";
import {build, Filter} from "./query";
import {Sort} from "./sorts";

export const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
export const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);
export const DefaultChunkSize = 8 * 1024 * 1024

type Opts = {
    url: string
    chunkSize?: number
    ready?: (worker: WorkerHttpvfs) => void
}
export class Worker {
    url: string
    chunkSize: number
    worker: Promise<WorkerHttpvfs>
    ready?: (worker: WorkerHttpvfs) => void
    requests: { [query: string]: { promise?: Promise<any> } }

    constructor({ url, chunkSize = DefaultChunkSize, ready }: Opts) {
        this.url = url
        this.chunkSize = chunkSize
        this.ready = ready
        this.worker = this.initWorker()
        this.requests = {}
    }

    async initWorker(): Promise<WorkerHttpvfs> {
        const { url, chunkSize } = this
        try {
            console.log("Fetching DB…", this.url);
            const worker: Promise<WorkerHttpvfs> = createDbWorker(
                [
                    {
                        from: "inline",
                        config: {
                            serverMode: "full",
                            url,
                            requestChunkSize: chunkSize,
                        },
                    },
                ],
                workerUrl.toString(),
                wasmUrl.toString()
            );
            console.log("setting worker")
            worker.then(this.ready)
            return worker
        } catch (error) {
            throw error
        }
    }

    async count({ table, limit, offset, filters }: {
        table: string,
        limit?: number,
        offset?: number,
        filters?: Filter[],
    }): Promise<number> {
        const query = build(
            {
                table,
                limit,
                offset,
                count: 'rowCount',
                filters,
            }
        )
        if (query in this.requests) {
            const promise = this.requests[query].promise as Promise<number>
            if (!promise) {
                throw Error(`Promise race: ${query}`)
            }
            console.log("cached query:", query)
            return promise
        }
        this.requests[query] = {}
        console.log("query:", query)
        const count = (
            this.worker
                .then((worker) => worker.db.query(query))
                .then((rows) => {
                    delete this.requests[query]
                    return rows as {  rowCount: number }[]
                })
                .then(([ { rowCount } ]) => rowCount)
        )
        this.requests[query].promise = count
        return count
    }

    async fetch<Row>({ table, limit, offset, sorts, filters, }: {
        table: string,
        limit?: number,
        offset?: number,
        sorts?: Sort[],
        filters?: Filter[],
    }): Promise<Row[]> {
        const query = build(
            {
                table,
                limit,
                offset,
                sorts,
                filters,
            }
        )
        if (query in this.requests) {
            const promise = this.requests[query].promise as Promise<Row[]>
            if (!promise) {
                throw Error(`Promise race: ${query}`)
            }
            return promise
        }
        console.log("query:", query)
        this.requests[query] = {}
        const rows = (
            this.worker
                .then((worker) => worker.db.query(query))
                .then((rows) => {
                    delete this.requests[query]
                    return rows as Row[]
                })
        )
        this.requests[query].promise = rows
        return rows
    }
}
