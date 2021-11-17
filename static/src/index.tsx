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
        }
    }

    render() {
        const state = this.state;
        const { data, region, userType, yAxis } = state;
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

        const radios = (label: string, choices: string[], selected: string, key: string) => {
            let onChange = function(this: App, e: any) {
                let obj: any = {};
                obj[key] = e.target.value;
                this.setState(obj);
            }
            onChange = onChange.bind(this)

            const labels = choices.map((choice) => {
                const key = label + "-" + choice
                return <label key={key}>
                    <input
                        type="radio"
                        name={key}
                        value={choice}
                        checked={choice == selected}
                        onChange={e => {}}
                    ></input>
                    {choice}
                </label>
            })
            return <div className="control col">
                <div className="control-header">{label}:</div>
                <div id={label} onChange={onChange}>{labels}</div>
            </div>
        }

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
                    useResizeHandler
                    layout={{
                        autosize: true,
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
                    {radios("Region", ["All", "NYC", "JC"], region, "region")}
                    {radios("User Type", ["All", "Subscriber", "Customer"], userType, "userType")}
                    {radios("Y-Axis", ["Rides", "Ride minutes"], yAxis, "yAxis")}
                </div>
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
