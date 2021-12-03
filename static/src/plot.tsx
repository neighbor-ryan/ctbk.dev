import React, {useEffect, useState} from 'react';
import Plot from 'react-plotly.js';
import * as Plotly from "plotly.js";
import {Shape} from "plotly.js";
import {Checklist} from "./checklist";
import {Radios} from "./radios";
import {Checkbox} from "./checkbox";
import {Worker} from "./worker";
import {QueryParamConfig, useQueryParam} from 'use-query-params';
import createPersistedState from 'use-persisted-state';
import moment from 'moment';
import {
    boolParam,
    HandleUnexpectedValue,
    returnDefaultOrError,
    useEnumMultiParam,
    useEnumQueryParam
} from "./search-params";
import _ from "lodash";

const { entries, values, keys, fromEntries } = Object
const Arr = Array.from

const useShowLegend = createPersistedState("showLegend")

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

type Region = 'All' | 'NYC' | 'JC'
const Regions: Region[] = [ 'All', 'NYC', 'JC', ]
type UserType = 'All' | 'Subscriber' | 'Customer'
const UserTypes: UserType[] = [ 'All', 'Subscriber', 'Customer', ]

type Gender = 'Male' | 'Female' | 'Other / Unspecified'
const Int2Gender: { [k: number]: Gender } = { 0: 'Other / Unspecified', 1: 'Male', 2: 'Female' }

type RideableType = 'Docked' | 'Electric' | 'Unknown'
const RideableTypes: RideableType[] = ['Docked' , 'Electric' , 'Unknown']
const RideableTypeChars: [ RideableType, string ][] = [['Docked','d'] , ['Electric','e'] , ['Unknown','u']]
const NormalizeRideableType: { [k: string]: RideableType } = {
    'docked_bike': 'Docked',
    'classic_bike': 'Docked',
    'electric_bike': 'Electric',
    'unknown': 'Unknown',
    'motivate_dockless_bike': 'Unknown',
}

type StackBy = 'None' | 'User Type' | 'Gender' | 'Rideable Type'
const StackBys: [StackBy, string][] = [ ['None', 'n'], ['Gender','g'], ['User Type','u'], ['Rideable Type','r'], ]

type DateRange = 'All' | '1y' | '2y' | '3y' | '4y' | '5y' | { start?: Date, end?: Date }

type YAxis = 'Rides' | 'Ride minutes'
const YAxes: [YAxis, string][] = [ ['Rides', 'r'], ['Ride minutes', 'm'], ]
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
    dateRange: DateRange
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

function mapEntries<T, V>(o: { [k: string]: T }, fn: (k: string, t: T) => [ string, V ]) {
    return fromEntries(entries(o).map(([ k, t ]) => fn(k, t)))
}

function mapValues<T, V>(o: { [k: string]: T }, fn: (k: string, t: T) => V) {
    return mapEntries<T, V>(o, ( k, t) => [ k, fn(k, t) ])
}

function filterEntries<T>(o: { [k: string]: T }, fn: (k: string, t: T) => boolean) {
    return fromEntries(entries(o).filter(([ k, t ]) => fn(k, t)))
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

const Char2Gender: { [k: string]: Gender } = { 'm': 'Male', 'f': 'Female', 'u': 'Other / Unspecified', }
const Gender2Char: { [k in Gender]: string } = { 'Male': 'm', 'Female': 'f', 'Other / Unspecified': 'u', }
const GenderChars: [ Gender, string ][] = [ ['Male', 'm'], ['Female', 'f'], ['Other / Unspecified', 'u'], ]
const Genders: Gender[] = ['Male' , 'Female' , 'Other / Unspecified']
// const gendersQueryState = queryState<Gender[]>({
//     defaultValue: [ 'Male', 'Female', 'Other / Unspecified', ],
//     parse: queryParam => queryParam.split('').map(ch => { return Char2Gender[ch] }),
//     render: value => value.map(gender => Gender2Char[gender]).join(''),
// })

// const Char2Rideable: { [k: string]: RideableType } = { 'd': 'Docked', 'e': 'Electric', 'u': 'Unknown' }
// const Rideable2Char: { [k in RideableType]: string } = { 'Docked': 'd', 'Electric': 'e', 'Unknown': 'u', }
// const rideablesQueryState = queryState<RideableType[]>({
//     defaultValue: [ 'Docked', 'Electric', 'Unknown', ],
//     parse: queryParam => queryParam.split('').map(ch => { return Char2Rideable[ch] }),
//     render: value => value.map(rideableType => Rideable2Char[rideableType]).join(''),
// })

const StartDate = moment('2013-06-01').toDate()
const Date2Query = (d: Date | undefined) => d ? moment(d).format('YYMM') : ''
const Query2Date = (s: string) => {
    const year = `20${s.substring(0, 2)}`
    const month = s.substring(2, 4)
    return moment(`${year}-${month}-01`).toDate()
}
// const DateRange2Dates = (dateRange: DateRange): { start: Date, end: Date, } => {
//     if (dateRange == 'All') {
//         return { start: StartDate, end: new Date }
//     }
//     else if (typeof dateRange === 'string') {
//         let start: Date = new Date
//         if (dateRange[dateRange.length - 1] != 'y') {
//             throw Error(`Unrecognized date range string: ${dateRange}`)
//         }
//         const numYears = parseInt(dateRange.substr(0, dateRange.length - 1))
//         start.setFullYear(start.getFullYear() - numYears)
//         return { start, end: new Date }
//     } else {
//         const {start, end} = dateRange
//         return { start: start || StartDate, end: end || new Date }
//     }
// }
// const dateRangeQueryState = queryState<DateRange>({
//     defaultValue: 'All',
//     parse: queryParam => {
//         if ([ 'All', '1y', '2y', '3y', '4y', '5y', ].indexOf(queryParam) != -1) {
//             return queryParam as DateRange
//         } else {
//             const [start, end] = queryParam.split('-').map(d => d ? Query2Date(d) : undefined)
//             return {start, end} as DateRange
//         }
//     },
//     render: dateRange => {
//         if (typeof dateRange === 'string') {
//             return dateRange
//         } else {
//             const {start, end} = dateRange
//             return `${Date2Query(start)}-${Date2Query(end)}`
//         }
//     },
// })

export function dateRangeParam(
    {
        defaultValue = 'All',
        handleUnexpectedValue = "Warn",
    }: {
        defaultValue?: DateRange,
        handleUnexpectedValue?: HandleUnexpectedValue,
    } = {}
): QueryParamConfig<DateRange> {
    const eq = _.isEqual
    return {
        encode(value: DateRange): string | (string | null)[] | null | undefined {
            if (eq(value, defaultValue)) return undefined
            if (typeof value === 'string') return value
            const {start, end} = value
            return `${Date2Query(start)}-${Date2Query(end)}`
        },
        decode(value: string | (string | null)[] | null | undefined): DateRange {
            if (value === undefined) return defaultValue
            if (value === null) return returnDefaultOrError(value, 'All', handleUnexpectedValue)
            if (typeof value === 'string') {
                if (['All', '1y', '2y', '3y', '4y', '5y',].indexOf(value) != -1) {
                    return value as DateRange
                } else {
                    const [start, end] = value.split('-').map(d => d ? Query2Date(d) : undefined)
                    return {start, end} as DateRange
                }
            } else {
                return returnDefaultOrError(value, 'All', handleUnexpectedValue)
            }
        },
        equals(l: DateRange, r: DateRange): boolean { return eq(l, r) }
    }
}

export function numberArrayParam(
    {
        defaultValue = [],
        handleUnexpectedValue = "Warn",
    }: {
        defaultValue?: number[],
        handleUnexpectedValue?: HandleUnexpectedValue,
    } = {}
): QueryParamConfig<number[]> {
    const eq = _.isEqual
    return {
        encode(value: number[]): string | (string | null)[] | null | undefined {
            if (eq(value, defaultValue)) return undefined
            return value.map(v => v.toString()).join(',')
        },
        decode(value: string | (string | null)[] | null | undefined): number[] {
            if (value === undefined) return defaultValue
            if (value === null) return returnDefaultOrError(value, defaultValue, handleUnexpectedValue)
            if (typeof value === 'string') {
                return value.split(',').map(parseInt)
            } else {
                const arrays: number[][] = value.map(v => this.decode(v))
                return arrays[arrays.length - 1]
            }
        },
        equals(l: number[], r: number[]): boolean { return eq(l, r) }
    }
}
export function App({ url, worker }: { url: String, worker: Worker, }) {
    const [ data, setData ] = useState<Row[] | null>(null)

    const [ region, setRegion ] = useEnumQueryParam<Region>('r', Regions)
    const [ yAxis, setYAxis ] = useEnumQueryParam<YAxis>('y', YAxes)
    const [ userType, setUserType ] = useEnumQueryParam<UserType>('u',UserTypes)
    const [ stackBy, setStackBy ] = useEnumQueryParam<StackBy>('s', StackBys)

    const [ stackRelative, setStackRelative ] = useQueryParam('rel', boolParam())

    const [ genders, setGenders ] = useEnumMultiParam<Gender>('g', { entries: GenderChars, defaultValue: Genders })
    const [ rideableTypes, setRideableTypes ] = useEnumMultiParam<RideableType>('rt', { entries: RideableTypeChars, defaultValue: RideableTypes })

    const [ dateRange, setDateRange ] = useQueryParam<DateRange>('d', dateRangeParam())
    const [ rollingAvgs, setRollingAvgs ] = useQueryParam<number[]>('rolling', numberArrayParam({ defaultValue: [12] }))

    const [ showLegend, setShowLegend ] = useShowLegend(true)

    console.log("Region", region, "User Type", userType, "Y-Axis", yAxis, "First row:")
    console.log(data && data[0])

    useEffect(
        () => {
            console.log("fetching…")
            worker.fetch<Row>({
                table: 'agg',
                // TODO: date range?
            }).then(setData)
        },
        [ worker, ],
    )

    if (!data) {
        return <div>Loading…</div>
    }

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
    let months: string[] = Arr(keys(monthsData))

    if (stackBy && stackRelative) {
        const monthTotals = mapValues(
            monthsData,
            (month, stackVals) => sumValues(stackVals)
        )
        monthsData = mapValues(
            monthsData,
        (month, stackVals) =>
            mapValues(
                stackVals,
            (stackVal, count) => count / monthTotals[month] * 100
            )
        )
        stacksData = mapValues(
            stacksData,
        (stackVal, values) =>
            mapValues(
                values,
            (month, count) => count / monthTotals[month] * 100
            )
        )
    }

    // let start: Date = new Date(StartDate)
    // let end: Date = new Date()
    // if (dateRange != 'All') {
    //     if (typeof dateRange === 'string') {
    //         if (dateRange[dateRange.length - 1] != 'y') {
    //             throw Error(`Unrecognized date range string: ${dateRange}`)
    //         }
    //         const numYears = parseInt(dateRange.substr(0, dateRange.length - 1))
    //         end.setFullYear(end.getFullYear() - numYears)
    //     } else {
    //         ({ start, end } = dateRange)
    //     }
    //     function filterMonth(month: string, _: any): boolean {
    //         const date = new Date(month)
    //         return start <= date && date < end
    //     }
    //     monthsData = filterEntries(monthsData, filterMonth)
    //     stacksData = mapValues(
    //         stacksData,
    //     (stackVal, months) => filterEntries(months, filterMonth)
    //     )
    // }

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
                return rollingAvgs.map((n) => rollingAvg(vals, n))
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
                <Radios label="Region" options={["All", "NYC", "JC"]} cb={setRegion} choice="All" />
                <Radios label="User Type" options={["All", "Subscriber", "Customer"]} cb={setUserType} choice="All" />
                <Radios label="Y Axis" options={["Rides", "Ride minutes"]} cb={setYAxis} choice="Rides" />
                <Checklist
                    label="Rolling Avg"
                    data={[{ name: "12mo", data: 12, checked: true }]}
                    cb={setRollingAvgs}
                    extra={
                        <Checkbox
                            id="showlegend"
                            label="Legend"
                            checked={showlegend}
                            cb={setShowLegend}
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
                    cb={setStackBy}
                    choice="None"
                    extra={
                        <Checkbox
                            id="stack-relative"
                            label="Percentages"
                            checked={stackRelative}
                            cb={setStackRelative}
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
                    cb={setGenders}
                />
                <Checklist<RideableType>
                    label="Rideable Type 🚧"
                    data={[
                        { name: 'Docked', data: 'Docked', checked: true, disabled: true },
                        { name: 'Electric', data: 'Electric', checked: true, disabled: true },
                        { name: 'Unknown', data: 'Unknown', checked: true, disabled: true },
                    ]}
                    cb={setRideableTypes}
                />
            </div>
        </div>
    );
}

// $(document).ready(function () {
//     ReactDOM.render(<App />, document.getElementById('root'));
// });
