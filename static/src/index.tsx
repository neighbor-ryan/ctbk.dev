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
import {Checkbox} from "./checkbox";

const { entries, values, keys, fromEntries } = Object
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

type StackBy = 'None' | 'User Type' | 'Gender' | 'Rideable Type'

type YAxis = 'Rides' | 'Ride minutes'
const yAxisLabelDict = {
    'Rides': { yAxis: 'Total Rides', title: 'Citibike Rides per Month', hoverLabel: 'Rides' },
    'Ride minutes': { yAxis: 'Total Ride Minutes', title: 'Citibike Ride Minutes per Month', hoverLabel: 'Minutes', },
}

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
    stackRelative: boolean
    rideableTypes: RideableType[]
    rollingAvgs: number[]
    yAxis: YAxis
    json: null | any
    showLegend: boolean | null
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

function sum(arr: number[]) {
    return arr.reduce((a, b) => a + b, 0)
}

function sumValues(o: { [k: string]: number }) {
    return sum(values(o))
}

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

// For stacked graphs with 1, 2, or 3 stacked features, darken the base color by these ratios
const colorSetFades: { [k: number]: number[] } = {
    1: [           1, ],
    2: [      .75, 1, ],
    3: [ .65, .80, 1, ],
}

// Hexadecimal character color for rolling average traces in stacked graphs
const stackRollColorDicts = {
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

// Hex-color utilities
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
            stackRelative: false,
            json: null,
            userType: 'All',
            yAxis: 'Rides',
            rollingAvgs: [12],
            showLegend: null,
        }
    }

    render() {
        const state = this.state;
        const { data, region, userType, genders, rideableTypes, stackBy, stackRelative, yAxis, rollingAvgs, showLegend } = state;
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

        let yAxisLabel = yAxisLabelDict[yAxis].yAxis
        let yHoverLabel = yAxisLabelDict[yAxis].hoverLabel
        let title = yAxisLabelDict[yAxis].title
        if (stackRelative) {
            yAxisLabel += ' (%)'
            yHoverLabel += ' (%)'
            title += ` (%, by ${stackBy})`
        }

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

        const stackKeyDict = {
            'None': [''],
            'User Type': ['Customer', 'Subscriber'],
            'Gender': ['Other / Unspecified', 'Male', 'Female'],
            'Rideable Type': ['Docked', 'Electric', 'Unknown'],
        }
        const stackKeys = stackKeyDict[stackBy]
        const showlegend = showLegend == null ? (stackBy != 'None') : showLegend

        // Build multi-index in both orders:
        // - month -> stackVal -> count
        // - stackVal -> month -> count
        let monthsData: { [month: string]: { [stackVal: string]: number }} = {}
        let stacksData: { [stackVal: string]: { [month: string]: number }} = {};
        filtered.forEach((r) => {
            const date: Date = r['Month'];
            const month: string = date.toString();
            const stackVal: Gender | UserType | RideableType | '' = stackBy == 'None' ? '' : r[stackBy]
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

        // Sort months within each index
        monthsData = order(monthsData)
        stacksData = fromEntries(stackKeys.map((stackVal) => [ stackVal, order(stacksData[stackVal]) ]))
        const months: string[] = Arr(keys(monthsData))

        if (stackRelative) {
            const monthTotals = fromEntries(
                entries(monthsData)
                    .map(
                        ([month, stackVals]) => [ month, sumValues(stackVals) ]
                    )
            )
            monthsData = fromEntries(
                entries(monthsData)
                    .map(
                        ([month, stackVals]) => [
                            month,
                            fromEntries(
                                entries(stackVals)
                                    .map(
                                        ([ stackVal, count ]) => [ stackVal, count / monthTotals[month] * 100]
                                    )
                            )
                        ]
                    )
            )
            stacksData = fromEntries(
                entries(stacksData)
                    .map(
                        ([stackVal, values]) => [
                            stackVal,
                            fromEntries(
                                entries(values)
                                    .map(
                                        ([ month, count ]) => [ month, count / monthTotals[month] * 100 ]
                                    )
                            )
                        ]
                    )
            )
        }

        const fades = colorSetFades[stackKeys.length]
        const baseColor = '#88aaff'

        const barTraces: Plotly.Data[] =
            entries(stacksData)
                .map(([stackVal, values], idx) => {
                    const x = months
                    const y = months.map((month) => values[month] || 0)
                    const name = stackVal || yHoverLabel
                    const fade = fades[idx]
                    const color = darken(baseColor, fade)
                    console.log("trace", name, "color", color)
                    return {
                        x, y, name,
                        type: 'bar',
                        marker: { color, },
                    }
                })

        let rollingSeries: ((number | null)[])[] = []
        rollingSeries = rollingSeries.concat(
            ...values(stacksData)
                .map((months) => {
                    //console.log("stacks:", months)
                    let vals = values(months)
                    if (stackBy == 'Gender') {
                        // Gender data became 100% "Other / Unspecified" from February 2021; don't bother with per-entry
                        // rolling averages from that point onward
                        const cutoff = new Date('2021-02-01')
                        vals =
                            entries(months)
                                .filter(([ month, _ ]) => new Date(month) < cutoff)
                                .map(([ _, vals ]) => vals)
                    }
                    return rollingAvgs.map(
                        (n) =>
                            rollingAvg(vals, n)
                    )
                })
        )

        if (stackBy != 'None' && !stackRelative) {
            const totals: number[] = values(monthsData).map((stackData) => sumValues(stackData))
            const rollingTotals = rollingAvgs.map((n) => rollingAvg(totals, n))
            rollingSeries = rollingSeries.concat(rollingTotals)
        }

        const allYears: Array<number> =
            months
                .map((m) => new Date(m))
                .filter((d) => d.getMonth() == 0)
                .map((d) => d.getFullYear());
        const years: Array<number> = [...new Set(allYears)];
        const vlines: Array<Partial<Shape>> = years.map(vline);

        let stackRollColorDict: { [k: string]: string } = stackRollColorDicts[stackBy]
        stackRollColorDict['Total'] = '0'  // Black
        const rollingTraces: Plotly.Data[] = rollingSeries.map(
            (y, idx) => {
                const stackVal = stackKeys[idx] || 'Total'
                const name = stackVal == 'Total' ? '12mo avg' : `${stackVal} (12mo)`
                const char = stackRollColorDict[stackVal]
                const color = '#' + char + char + char + char + char + char
                console.log("rolling color:", idx, stackVal, char, color)
                return {
                    name,
                    x: months,
                    y,
                    type: 'scatter',
                    marker: { color, },
                    line: { width: 4, },
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
                        titlefont: { size: 18 },
                        autosize: true,
                        barmode: 'stack',
                        showlegend,
                        title,
                        xaxis: {
                            title: 'Month',
                            tickfont: { size: 14 },
                            titlefont: { size: 14 },
                        },
                        yaxis: {
                            gridcolor: '#DDDDDD',
                            title: yAxisLabel,
                            tickfont: { size: 14 },
                            titlefont: { size: 14 },
                        },
                        paper_bgcolor: 'rgba(0,0,0,0)',
                        plot_bgcolor: 'rgba(0,0,0,0)',
                        shapes: vlines,
                    }}
                />
                <div className="no-gutters row">
                    <Radios label="Region" options={["All", "NYC", "JC"]} cb={(region) => this.setState({ region })} choice="All" />
                    <Radios label="User Type" options={["All", "Subscriber", "Customer"]} cb={(userType) => this.setState({ userType })} choice="All" />
                    <Radios label="Y Axis" options={["Rides", "Ride minutes"]} cb={(yAxis) => this.setState({ yAxis })} choice="Rides" />
                    <Checklist
                        label="Rolling Avg"
                        data={[{ name: "12mo", data: 12, checked: true }]}
                        cb={(rollingAvgs) => this.setState({ rollingAvgs })}
                        extra={
                            <Checkbox
                                id="showlegend"
                                label="Legend"
                                checked={showlegend}
                                cb={
                                    (checked) => {
                                        console.log("legend checkbox:", checked)
                                        this.setState({ showLegend: checked })
                                    }
                                }
                            />
                        }
                    />
                    <Radios
                        label="Stack by"
                        options={[
                            "None",
                            "User Type",
                            { label: "Gender 🚧", data: "Gender", },
                            { label: "Rideable Type 🚧", data: "Rideable Type", disabled: true }
                        ]}
                        cb={(stackBy) => this.setState({ stackBy })}
                        choice="None"
                        extra={
                            <Checkbox
                                id="stack-relative"
                                label="Percentages"
                                checked={stackRelative}
                                cb={
                                    (checked) => {
                                        console.log("percentage checked:", checked)
                                        this.setState({ stackRelative: checked })
                                    }
                                }
                            />
                        }
                    />
                    <Checklist<Gender>
                        label="Gender 🚧"
                        data={[
                            { name: 'Male', data: 'Male', checked: true },
                            { name: 'Female', data: 'Female', checked: true },
                            { name: 'Other / Unspecified', data: 'Other / Unspecified', checked: true },
                        ]}
                        cb={(genders) => this.setState({ genders })}
                    />
                    <Checklist<RideableType>
                        label="Rideable Type 🚧"
                        data={[
                            { name: 'Docked', data: 'Docked', checked: true, disabled: true },
                            { name: 'Electric', data: 'Electric', checked: true, disabled: true },
                            { name: 'Unknown', data: 'Unknown', checked: true, disabled: true },
                        ]}
                        cb={(rideableTypes) => this.setState({ rideableTypes })}
                    />
                </div>
            </div>
        );
    }
}

$(document).ready(function () {
    ReactDOM.render(<App />, document.getElementById('root'));
});
