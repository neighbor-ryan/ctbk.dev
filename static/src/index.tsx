import { createDbWorker } from "sql.js-httpvfs";
import React from 'react';
import ReactDOM from 'react-dom'
import { Component, createElement } from "react";
import Plot from 'react-plotly.js';
import $ from 'jquery';
import {OhclData, PlotData} from "plotly.js";
// import './index.css';

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

// async function load() {
//   const url = "https://ctbk.s3.amazonaws.com/aggregated/ymrgtb_cd_201306:202109.sqlite";
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
    region: 'JC' | 'NYC' | 'All'
    json: null | any
}

// const jsonMode = true;
const jsonMode = false;

class App extends Component<any, State> {
    async componentDidMount() {
        if (jsonMode) {
            fetch('/assets/plot.json')
                .then(response => response.json())
                .then(json => {
                    console.log("got json:", json);
                    this.setState({json: json,})
                });
        } else {
            // const url = "https://ctbk.s3.amazonaws.com/aggregated/ymrgtb_cd_201306:202110.sqlite";
            const url = "/assets/ymrgtb_cd_201306:202110.sqlite";
            console.log("Fetching DB…", url);
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
            console.log("Fetched db:", url);
            this.setState({data: data});
        }
    }

    constructor(props: any) {
        super(props);
        this.state = { data: null, region: 'JC', json: null, }
    }

    render() {
        const state = this.state;
        const data: (null | ({ Month: Date, Count: number, Region: 'JC'|'NYC' }[])) = state['data'];
        const region = state['region'];
        const json = state['json']
        if (json) {
            console.log("found json");
            return (
                <div id="plot">
                    <Plot
                        data={json['data']}
                        layout={json['layout']}
                    />
                </div>
            )
        }
        if (!data) {
            return <div>Loading…</div>
        }
        let agg = new Map<string, number>()
        data.forEach((r) => {
            if (!(region == 'All' || region == r['Region'])) {
                // console.log('Skipping region:', r['Region']);
                return;
            }
            const month: Date = r['Month'];
            const key: string = month.toString();
            const cur = agg.get(key);
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

        // const plotData: PlotData = {
        //     x: months,
        //     y: counts,
        //     type: 'bar',
        //     marker: {
        //         color: '#88aaff',
        //     },
        //     // xperiod: "M1",
        //     // xperiodalignment: "start",
        //     // fillcolor: '#ff0000', //'#88aaff',
        // }

        return (
            <div id="plot">
                <Plot
                    data={[
                        {
                            x: months,
                            y: counts,
                            type: 'bar',
                            marker: {
                                color: '#88aaff',
                            },
                        },
                    ]}
                    layout={{
                        // xaxis: {
                        //
                        // }

                        //width: '100%',
                        //width: 320, height: 240,
                        title: 'Citibike Rides By Month',
                        // yaxis_gridcolor: '#DDDDDD',
                        yaxis: {
                            gridcolor: '#DDDDDD',
                        },
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: {
                            size: 18,
                        },
                        shapes: [{
                            type: 'line',
                            x0: '2018-01-01',
                            y0: 0,
                            x1: '2018-01-01',
                            yref: 'paper',
                            y1: 100000,
                            line: {
                                color: 'red',
                                width: 1.5,
                                dash: 'dot'
                            }
                        }],
                    }}
                    // color_discrete_sequence={['#88aaff']}
                />
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
