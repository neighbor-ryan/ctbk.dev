import { createDbWorker } from "sql.js-httpvfs";
import React from 'react';
import ReactDOM from 'react-dom'
import { Component, createElement } from "react";
import Plot from 'react-plotly.js';
import $ from 'jquery';

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

// async function load() {
//   const url = "https://ctbk.s3.amazonaws.com/aggregated/ymrgtb_cd_201306:202107.sqlite";
//   console.log("Fetching DB:", url);
//   const worker = await createDbWorker(
//     [
//       {
//         from: "inline",
//         config: {
//           serverMode: "full",
//           url: url,
//           requestChunkSize: 4096,
//         },
//       },
//     ],
//     workerUrl.toString(),
//     wasmUrl.toString()
//   );
//
//   const result = await worker.db.query(`select * from agg limit 10`);
//
//   const dataDiv = document.getElementById("data")
//   if (dataDiv) {
//       dataDiv.textContent = JSON.stringify(result);
//   }
// }

// load();

type State = {
    data: any[] | null
}

class App extends Component<any, State> {
    async componentDidMount() {
        const url = "https://ctbk.s3.amazonaws.com/aggregated/ymrgtb_cd_201306:202107.sqlite";
        console.log("Fetching DB:", url);
        const worker = await createDbWorker(
            [
                {
                    from: "inline",
                    config: {
                        serverMode: "full",
                        url: url,
                        requestChunkSize: 4096,
                    },
                },
            ],
            workerUrl.toString(),
            wasmUrl.toString()
        );

        const data = await worker.db.query(`select * from agg`);
        this.setState({ data: data })
    }

    constructor(props: any) {
        super(props);
        this.state = { data: null }
    }

    render() {
        const data: (null | ({ Month: Date, Count: number }[])) = this.state['data'];
        if (!data) {
            return <div>Loading…</div>
        }
        let agg = new Map<string, number>()
        data.forEach((r) => {
            const month: Date = r['Month'];
            const key: string = month.toString();
            const cur = agg.get(key)
            const count = r['Count'];
            if (cur === undefined) {
                console.log("Month:",month)
                agg.set(key, count);
            } else {
                agg.set(key, cur + count)
            }
            // if (!(key in agg)) {
            //     agg.set(key, 0);
            // }
            // const cur: number = agg.get(key)
            // agg.set(key, cur + count);
        })
        const months: string[] = Array.from(agg.keys());
        const counts: number[] = Array.from(agg.values());
        // const months = data.map((r: any) => r['Month'])
        // const counts = data.map((r: any) => r['Count'])
        console.log(months, counts)
        return (
            <Plot
                data={
                    [
                        {
                            x: months,
                            y: counts,
                            type: 'bar',
                        },
                    ]
                }
                layout={{
                    //width: 320, height: 240,
                    title: 'Citibike Rides By Month'
                }}
            />
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
