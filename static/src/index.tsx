import { createDbWorker } from "sql.js-httpvfs";
import React from 'react';
import ReactDOM from 'react-dom'
import { Component, createElement } from "react";
import Plot from 'react-plotly.js';
import $ from 'jquery';
import {Shape} from "plotly.js";
// import './index.css';

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

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
                    //console.log("got json:", json);
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
        this.onRegion = this.onRegion.bind(this);
    }

    onRegion(e: any) {
        console.log("event:", e.target.value);
    }

    render() {
        const state = this.state;
        const data: (null | ({ Month: Date, Count: number, Region: 'JC'|'NYC' }[])) = state['data'];
        const region = state['region'];
        console.log("render; region:", region);
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
                //console.log("Month:",month)
                agg.set(key, count);
            } else {
                agg.set(key, cur + count)
            }
        })
        const months: string[] = Array.from(agg.keys());
        const counts: number[] = Array.from(agg.values());
        // const months = data.map((r: any) => r['Month'])
        // const counts = data.map((r: any) => r['Count'])
        //console.log(months, counts)

        function vline(year: number): Partial<Shape> {
            const x: string = `${year-1}-12-20`;
            return {
                layer: 'below',
                type: 'line',
                x0: x,
                y0: 0,
                x1: x,
                yref: 'paper',
                y1: 1,//00000,
                line: {
                    color: '#555555',
                    // width: 1.5,
                    // dash: 'dot'
                }
            }
        }

        const allYears: Array<number> = months.map((m) => new Date(m)).filter((d) => d.getMonth() == 0).map((d) => d.getFullYear());
        const years: Array<number> = [...new Set(allYears)];
        const vlines: Array<Partial<Shape>> = years.map(vline);

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
                        title: 'Citibike Rides By Month',
                        yaxis: {
                            gridcolor: '#DDDDDD',
                        },
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        font: {
                            size: 18,
                        },
                        shapes: vlines,
                    }}
                />
                <div id="region-controls" onChange={(e: any) => this.setState({ region: e.target.value })}>
                    <input type="radio" name="region" value="All"></input><label>All</label>
                    <input type="radio" name="region" value="NYC"></input><label>NYC</label>
                    <input type="radio" name="region" value="JC"></input><label>JC</label>
                </div>
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
