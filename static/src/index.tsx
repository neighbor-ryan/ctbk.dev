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

const { entries, assign, values, keys, fromEntries } = Object
const concat = Array.prototype.concat
const Arr = Array.from


const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

type Region = 'All' | 'NYC' | 'JC'
type UserType = 'All' | 'Subscriber' | 'Customer'
type Gender = 'Male' | 'Female' | 'Other / Unspecified'
const Int2Gender: { [k: number]: Gender } = { 0: 'Other / Unspecified', 1: 'Male', 2: 'Female' }
type RideableType = 'Docked' | 'Electric' | 'Unknown'
const NormalizeRideableType: { [k: string]: RideableType } = {
    'docked_bike': 'Docked',
    'classic_bike': 'Docked',
    'electric_bike': 'Electric',
    'unknown': 'Unknown',
    'motivate_dockless_bike': 'Unknown',
}
type StackBy = 'None' | 'User Type' | 'Gender 🚧' | 'Rideable Type 🚧'
type YAxis = 'Rides' | 'Ride minutes'
type Row = {
    Month: Date
    Count: number
    Duration: number
    Region: Region
    'User Type': UserType
    Gender: number
    'Rideable Type': string
}
type State = {
    data: null | Row[]
    region: Region
    userType: UserType
    genders: Gender[]
    stackBy: StackBy
    rideableTypes: RideableType[]
    rollingAvgs: number[]
    yAxis: YAxis
    json: null | any
}

// const jsonMode = true;
const jsonMode = false;

function order<T>(u: { [k: string]: T }) {
    return keys(u).sort().reduce(
        (o: { [k: string]: T }, k: string) => {
            o[k] = u[k];
            return o;
        },
        {}
    );
}

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
            genders: [ 'Male', 'Female', 'Other / Unspecified', ],
            rideableTypes: [ 'Docked', 'Electric', 'Unknown', ],
            stackBy: 'None',
            json: null,
            userType: 'All',
            yAxis: 'Rides',
            rollingAvgs: [12],
        }
    }

    render() {
        const state = this.state;
        const { data, region, userType, genders, rideableTypes, stackBy, yAxis, rollingAvgs } = state;
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

        const filtered =
            data
                .map((r) => {
                    // Normalize gender, rideable type
                    const { Gender, 'Rideable Type': rideableType, ...rest } = r
                    return { Gender: Int2Gender[Gender], 'Rideable Type': NormalizeRideableType[rideableType], ...rest }
                })
                .filter((r) => {
                    // Apply filters
                    if (!(region == 'All' || region == r.Region)) {
                        return false
                    }
                    if (!(userType == 'All' || userType == r['User Type'])) {
                        return false
                    }
                    if (genders.indexOf(r.Gender) == -1) {
                        return false
                    }
                    if (rideableTypes.indexOf(r['Rideable Type']) == -1) {
                        console.warn("Dropping", r['Count'], "rides from with unrecognized rideable type", r['Rideable Type'], r)
                        return false
                    }
                    return true
                })

        type StackType = 'None' | 'User Type' | 'Gender' | 'Rideable Type'
        const stackTypeDict: { [k: string]: StackType} = {
            'None': 'None',
            'User Type': 'User Type',
            'Gender 🚧': 'Gender',
            'Rideable Type 🚧': 'Rideable Type'
        }
        const stackType: StackType = stackTypeDict[stackBy]
        const stackKeyDict = {
            'None': [''],
            'User Type': ['Customer', 'Subscriber'],
            'Gender': ['Other / Unspecified', 'Male', 'Female'],
            'Rideable Type': ['Docked', 'Electric', 'Unknown'],
        }
        const stackKeys = stackKeyDict[stackType]

        let monthsData: { [month: string]: { [stackVal: string]: number }} = {}
        let stacksData: { [stackVal: string]: { [month: string]: number }} = {};
        filtered.forEach((r) => {
            const date: Date = r['Month'];
            const month: string = date.toString();
            const stackVal: Gender | UserType | RideableType | '' = stackType == 'None' ? '' : r[stackType]
            const count = (yAxis == 'Rides') ? r['Count'] : r['Duration'];

            if (!(month in monthsData)) {
                monthsData[month] = {}
            }
            let cur = monthsData[month]
            if (!(stackVal in cur)) {
                cur[stackVal] = 0
            }
            cur[stackVal] += count

            if (!(stackVal in stacksData)) {
               stacksData[stackVal] = {}
            }
            cur = stacksData[stackVal]
            if (!(month in cur)) {
                cur[month] = 0
            }
            cur[month] += count
        })

        monthsData = order(monthsData)
        stacksData = fromEntries(stackKeys.map((stackVal) => [ stackVal, order(stacksData[stackVal]) ]))

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

        const colorSetFades: { [k: number]: number[] } = {
            1: [ 1 ],
            2: [      .75, 1, ],
            3: [ .65, .80, 1, ],
        }

        function pad(num: number, size: number){
            return ('0' + num.toString(16)).substr(-size);
        }
        function darken(c: string, f = 0.5): string {
            return '#' + [
                c.substr(1, 2),
                c.substr(3, 2),
                c.substr(5, 2)
            ]
                .map((s) =>
                    pad(
                        Math.round(
                            parseInt(s, 16) * f
                        ),
                        2
                    )
                )
                .join('')
        }

        let barData: { month: string, stackBy?: string, value: number }[] = []
        for (const [month, obj] of entries(monthsData)) {
            for (const stackKey of stackKeys) {
                const value = obj[stackKey] || 0
                let datum: { month: string, stackBy?: string, value: number } = { month, value }
                if (stackKey != '') {
                    datum['stackBy'] = stackKey
                }
                barData.push(datum)
            }
        }

        const fades = colorSetFades[stackKeys.length]
        const baseColor = '#88aaff'

        const months: string[] = Arr(keys(monthsData))
        const totals: number[] = values(monthsData).map((stackData) => values(stackData).reduce((a, b) => a + b), 0)
        const barTraces: Plotly.Data[] = entries(stacksData).map(([stackVal, values], idx) => {
            const x = months
            const y = months.map((month) => values[month] || 0)
            const name = stackVal || '???'
            const fade = fades[idx]
            const traceColor = darken(baseColor, fade)
            console.log("trace", name, "color", traceColor)
            return {
                x, y, name,
                type: 'bar',
                marker: {
                    color: traceColor,
                },
            }
        })

        const rollingSeries0: ((number | null)[])[][] =
            values(stacksData)
                .map((months) => {
                    console.log("stacks:", months)
                    let vals = values(months)
                    if (stackType == 'Gender') {
                        vals = entries(months).filter(([ month, _ ]) => new Date(month) < new Date('2021-02-01')).map(([ _, vals ]) => vals)
                    }
                    return rollingAvgs.map(
                        (n) =>
                            rollingAvg(vals, n)
                    )
                })

        console.log("rollingSeries0", rollingSeries0)
        let rollingSeries: ((number | null)[])[] = []
        rollingSeries = rollingSeries.concat(...rollingSeries0)

        const rollingTotals = rollingAvgs.map((n) => rollingAvg(totals, n))
        rollingSeries = rollingSeries.concat(rollingTotals)

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

        const stackRollDicts = {
            'None': {
               '': '0',
            },
            'User Type': {
                'Customer': 'D',
                'Subscriber': '7',
            },
            'Gender': {
                'Male': '6',
                'Female': 'D',
                'Other / Unspecified': 'E',
            },
            'Rideable Type': {
                'Docked': 'E',
                'Electric': 'D',
                'Unknown': '6',
            },
        }
        let stackRollDict: { [k: string]: string } = stackRollDicts[stackType]
        stackRollDict['Total'] = '0'
        const rollingTraces: Plotly.Data[] = rollingSeries.map(
            (y, idx) => {
                const stackVal = stackKeys[idx] || 'Total'
                const char = stackRollDict[stackVal]
                const traceColor = '#' + char + char + char + char + char + char
                console.log("rolling color:", idx, stackVal, char, traceColor)
                return {
                    name: `12mo avg (${stackVal})`,
                    x: months,
                    y: y,
                    type: 'scatter',
                    marker: {
                        color: traceColor,
                    },
                    line: {
                        width: 4,
                    },
                }
            }
        )

        const traces: Plotly.Data[] = barTraces.concat(rollingTraces)

        return (
            <div id="plot">
                <Plot
                    data={traces}
                    useResizeHandler
                    layout={{
                        autosize: true,
                        barmode: 'stack',
                        showlegend: stackType != 'None',
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
                    <Radios<StackBy>
                        label="Stack by" options={["None", "User Type", "Gender 🚧", { data: "Rideable Type 🚧", disabled: true }]} cb={(stackBy) => this.setState({ stackBy })} choice="None"
                    ></Radios>
                    <Checklist<Gender>
                        label="Gender 🚧"
                        data={[
                            { name: 'Male', data: 'Male', checked: true },
                            { name: 'Female', data: 'Female', checked: true },
                            { name: 'Other / Unspecified', data: 'Other / Unspecified', checked: true },
                        ]}
                        cb={(genders) => this.setState({ genders })}
                    ></Checklist>
                    <Checklist<RideableType>
                        label="Rideable Type 🚧"
                        data={[
                            { name: 'Docked', data: 'Docked', checked: true, disabled: true },
                            { name: 'Electric', data: 'Electric', checked: true, disabled: true },
                            { name: 'Unknown', data: 'Unknown', checked: true, disabled: true },
                        ]}
                        cb={(rideableTypes) => this.setState({ rideableTypes })}
                    ></Checklist>
                </div>
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
