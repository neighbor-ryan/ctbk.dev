import { createDbWorker } from "sql.js-httpvfs";
import React from 'react';
import ReactDOM from 'react-dom'
import { Component, createElement } from "react";
import Plot from 'react-plotly.js';
import $ from 'jquery';
import {Shape} from "plotly.js";
import * as Plotly from "plotly.js";
import {Checklist} from "./checklist";
import {Radios} from "./radios";
// import './index.css';

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

type Region = 'All' | 'NYC' | 'JC'
type UserType = 'All' | 'Subscriber' | 'Customer'
type YAxis = 'Rides' | 'Ride minutes'
type Row = {
    Month: Date
    Count: number
    Duration: number
    Region: Region
    'User Type': UserType
}
type State = {
    data: null | Row[]
    region: Region
    userType: UserType
    rollingAvgs: number[]
    yAxis: YAxis
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
            const url = "/assets/ymrgtb_cd_201306:202111.sqlite";
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
        this.state = {
            data: null,
            region: 'All',
            json: null,
            userType: 'All',
            yAxis: 'Rides',
            rollingAvgs: [12],
        }
    }

    render() {
        const state = this.state;
        const { data, region, userType, yAxis, rollingAvgs } = state;
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
        console.log("Region", region, "User Type", userType, "Y-Axis", yAxis, "First row:")
        console.log(data[0])
        let agg = new Map<string, number>()
        data.forEach((r) => {
            if (!(region == 'All' || region == r['Region'])) {
                return;
            }
            if (!(userType == 'All' || userType == r['User Type'])) {
                return;
            }
            const month: Date = r['Month'];
            const key: string = month.toString();
            const cur = agg.get(key);
            const count = (yAxis == 'Rides') ? r['Count'] : r['Duration'];
            if (cur === undefined) {
                //console.log("Month:",month)
                agg.set(key, count);
            } else {
                agg.set(key, cur + count)
            }
        })

        function rollingAvg(vs: number[], n: number): (number|null)[] {
            let sum: number = 0
            let avgs: (number|null)[] = []
            for (let i = 0; i < vs.length; i++) {
                sum += vs[i]
                if (i >= n) {
                    sum -= vs[i-n]
                    const avg = sum / n
                    avgs.push(avg)
                } else {
                    avgs.push(null)
                }

            }
            return avgs
        }

        const months: string[] = Array.from(agg.keys());
        const counts: number[] = Array.from(agg.values());
        const rollingSeries = rollingAvgs.map((n) => rollingAvg(counts, n))

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

        const allYears: Array<number> =
            months
                .map((m) => new Date(m))
                .filter((d) => d.getMonth() == 0)
                .map((d) => d.getFullYear());
        const years: Array<number> = [...new Set(allYears)];
        const vlines: Array<Partial<Shape>> = years.map(vline);

        const bars: Plotly.Data = {
            name: yAxis,
            x: months,
            y: counts,
            type: 'bar',
            marker: {
                color: '#88aaff',
            },
        }

        const rollingTraces: Plotly.Data[] = rollingSeries.map(
            (y) => {
                return {
                    name: '12mo avg',
                    x: months,
                    y: y,
                    type: 'scatter',
                    marker: {
                        color: '#000',
                    }
                }
            }
        )

        const traces: Plotly.Data[] = Array.prototype.concat([ bars ], rollingTraces)

        return (
            <div id="plot">
                <Plot
                    data={traces}
                    useResizeHandler
                    layout={{
                        autosize: true,
                        showlegend: false,
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
                <div className="no-gutters row">
                    <Radios<Region> label="Region" options={["All", "NYC", "JC"]} cb={(region) => this.setState({ region })} choice="All" />
                    <Radios<UserType> label="User Type" options={["All", "Subscriber", "Customer"]} cb={(userType) => this.setState({ userType })} choice="All" />
                    <Radios<YAxis> label="Y Axis" options={["Rides", "Ride minutes"]} cb={(yAxis) => this.setState({ yAxis })} choice="Rides" />
                    <Checklist
                        label="Rolling Avg"
                        data={[{ name: "12mo", data: 12, checked: true }]}
                        cb={(rollingAvgs) => this.setState({ rollingAvgs })}
                    ></Checklist>
                </div>
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
