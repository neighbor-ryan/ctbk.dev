import React, {useEffect, useMemo, useState} from 'react';
import ReactMarkdown from 'react-markdown'
import ReactTooltip from 'react-tooltip';
const Markdown = ReactMarkdown
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
import {Filter} from "./query";

const { entries, values, keys, fromEntries } = Object
const Arr = Array.from

const useShowLegend = createPersistedState("showLegend")

const workerUrl = new URL(
    "sql.js-httpvfs/dist/sqlite.worker.js",
    import.meta.url
);
const wasmUrl = new URL("sql.js-httpvfs/dist/sql-wasm.wasm", import.meta.url);

type Region = 'All' | 'NYC' | 'JC'
const Regions: [Region, string][] = [ ['All','all'], ['NYC','nyc'], ['JC','jc'], ]
type UserType = 'All' | 'Subscriber' | 'Customer'
const UserTypes: [UserType, string][] = [ ['All', 'a'], ['Subscriber', 's'], ['Customer','c'], ]

type Gender = 'Male' | 'Female' | 'Other / Unspecified'
const Int2Gender: { [k: number]: Gender } = { 0: 'Other / Unspecified', 1: 'Male', 2: 'Female' }
// Gender data became 100% "Other / Unspecified" from February 2021; don't bother with per-entry
// rolling averages from that point onward
const GenderRollingAvgCutoff = new Date('2021-02-01')

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

function rollingAvg<T>(vs: T[], n: number, fn?: (t: T) => number, ): [T, number | null][] {
    let avgs: [ T, number | null, ][] = []
    const f: (t: T) => number = fn ? (n => fn(n)) : (n => n as any as number)
    let sum = 0
    for (let i = 0; i < vs.length; i++) {
        sum += f(vs[i])
        if (i >= n) {
            sum -= f(vs[i-n])
            const avg = sum / n
            avgs.push([ vs[i], avg, ])
        } else {
            avgs.push([ vs[i], null, ])
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

const GenderChars: [ Gender, string ][] = [ ['Male', 'm'], ['Female', 'f'], ['Other / Unspecified', 'u'], ]
const Genders: Gender[] = ['Male' , 'Female' , 'Other / Unspecified']

const StartDate = moment('2013-06-01').toDate()
const Date2Query = (d: Date | undefined) => d ? moment(d).format('YYMM') : ''
const Query2Date = (s: string) => {
    const year = `20${s.substring(0, 2)}`
    const month = s.substring(2, 4)
    return moment(`${year}-${month}-01`).toDate()
}
const DateRange2Dates = (dateRange: DateRange, end?: Date): { start: Date, end: Date, } => {
    end = end ? end : new Date
    if (dateRange == 'All') {
        return { start: StartDate, end }
    }
    else if (typeof dateRange === 'string') {
        let start: Date = new Date(end)
        if (dateRange[dateRange.length - 1] != 'y') {
            throw Error(`Unrecognized date range string: ${dateRange}`)
        }
        const numYears = parseInt(dateRange.substr(0, dateRange.length - 1))
        start.setFullYear(start.getFullYear() - numYears)
        console.log(`subtracted ${numYears} years: ${start}, ${end}`)
        return { start, end }
    } else {
        const {start, end: e } = dateRange
        return { start: start || StartDate, end: e || end }
    }
}

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
                if (['All', '1y', '2y', '3y', '4y', '5y',].includes(value)) {
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

export function App({ url }: { url: string, }) {
    const worker = useMemo(() => new Worker({ url, }), [ url, ])
    const [ data, setData ] = useState<Row[] | null>(null)

    const [ region, setRegion ] = useEnumQueryParam<Region>('r', Regions)
    const [ yAxis, setYAxis ] = useEnumQueryParam<YAxis>('y', YAxes)
    const [ userType, setUserType ] = useEnumQueryParam<UserType>('u',UserTypes)
    const [ stackBy, setStackBy ] = useEnumQueryParam<StackBy>('s', StackBys)

    const [ stackRelative, setStackRelative ] = useQueryParam('pct', boolParam())

    const [ genders, setGenders ] = useEnumMultiParam<Gender>('g', { entries: GenderChars, defaultValue: Genders, delimiter: '' })
    const [ rideableTypes, setRideableTypes ] = useEnumMultiParam<RideableType>('rt', { entries: RideableTypeChars, defaultValue: RideableTypes })

    const [ dateRange, setDateRange ] = useQueryParam<DateRange>('d', dateRangeParam())
    const [ rollingAvgs, setRollingAvgs ] = useQueryParam<number[]>('rolling', numberArrayParam({ defaultValue: [12] }))

    const [ showLegend, setShowLegend ] = useShowLegend(true)

    console.log("Region", region, "User Type", userType, "Y-Axis", yAxis, "Date range:", dateRange, "First row:")
    console.log(data && data[0])

    useEffect(
        () => {
            console.log("fetching…")
            // This fetch seems like a good place to push-down the `dateRange` filter. However, rolling avgs are not in
            // the database, but are computed later in the outer render() flow here, and need to look backward from the
            // `start` date. Additionally, the latest data is often up to 6 weeks old (each month is typically released
            // about 2 weeks into the next month), and it's better for e.g. a "1yr" plot to show 12mos of data than 1
            // calendar year that's missing the 2 most recent months' entries.
            // The table is very small so fetching it all and slicing in memory below is fine.
            worker.fetch<Row>({
                table: 'agg',
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
    stacksData = fromEntries(stackKeys.map((stackVal) => [ stackVal, order(stacksData[stackVal] || {}) ]))
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

    // If `end` isn't set, default to 1d after the latest fetched data point (since it's generally treated as an
    // exclusive bound)
    const last = moment(_.max(data.map(r => new Date(r.Month)))).add(1, 'd').toDate()
    const { start, end } = DateRange2Dates(dateRange, last)

    // Compute a (trailing) rolling average for:
    // - [each time-window length in `rollingAvgs`] (typically just [12])], x
    // - [each stacked value (e.g. "Male", "Female", "Other / Unspecified")]
    let rollingSeries: (number | null)[][] = []
    rollingSeries = rollingSeries.concat(
        ...values(stacksData)
            .map((months) => {
                let vals: { month: Date, v: number }[] =
                    entries(months)
                        .map(
                            ([ month, v ]) => { return { month: new Date(month), v } }
                        )
                if (stackBy == 'Gender') {
                    vals = vals.filter(({ month }) => month < GenderRollingAvgCutoff)
                }
                return (
                    rollingAvgs
                        .map(n =>
                            rollingAvg(vals, n, ({ v }) => v)
                                .filter(([ { month }, avg ]) => start <= month && month < end)
                                .map(([ _, avg ]) => avg)
                        )
                )
            })
    )

    // In stacked mode, compute an extra "total" series
    if (stackBy != 'None' && !stackRelative) {
        const totals: { month: Date, total: number }[] = (
            entries(monthsData)
                .map(([ month, stackData ]) => {
                    return { month: new Date(month), total: sumValues(stackData) }
                })
        )
        const rollingTotals =
            rollingAvgs.map(n =>
                rollingAvg(totals, n, ({ total }) => total)
                    .filter(([ { month }, avg ]) => start <= month && month < end)
                    .map(([ _, avg ]) => avg)
            )
        rollingSeries = rollingSeries.concat(rollingTotals)
    }

    // Filter months

    const filterMonth = (month: string) => {
        const d = new Date(month)
        return start <= d && d < end
    }
    const filterMonthKey = <T,>() => (( month: string, t: T ) => filterMonth(month))

    months = months.filter(filterMonth)

    stacksData = mapValues(
        stacksData,
        (stackVal, months) => filterEntries(months, filterMonthKey())
    )

    monthsData = filterEntries(monthsData, filterMonthKey())

    // Vertical lines for each year boundary
    const allYears: Array<number> =
        months
            .map((m) => new Date(m))
            .filter((d) => d.getMonth() == 0)
            .map((d) => d.getFullYear());
    const years: Array<number> = [...new Set(allYears)];
    const vlines: Array<Partial<Shape>> = years.map(vline);

    // Create Plotly trace data, including colors (when stacking)
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

    // Bar data (including color fades when stacking)

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

    function icon(src: string, href: string, title: string) {
        return <a href={href} title={title}>
            <img className="icon" src={`/assets/${src}.png`} />
        </a>
    }

    const GenderLabel =
        <span>
            Gender
            <span data-tip data-for="gender-label-tooltip">
                <img className="warning icon" src="./assets/warning.png"/>
            </span>
        </span>

    return (
        <div id="plot" className="container">
            {/* Main plot: bar graph + rolling avg line(s) */}
            <Plot
                onDoubleClick={() => setDateRange('All')}
                onRelayout={e => {
                    if (!('xaxis.range[0]' in e && 'xaxis.range[1]' in e)) return
                    let [ start, end ] = [ e['xaxis.range[0]'], e['xaxis.range[1]'], ].map(s => s ? new Date(s) : undefined)
                    start = start ? moment(start).subtract(1, 'month').toDate() : start
                    const dateRange = (!start && !end) ? 'All' : { start, end, }
                    console.log("relayout:", e, start, end, dateRange,)
                    setDateRange(dateRange)
                }}
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
                        range: stackRelative ? [ 0, 100, ] : undefined,
                    },
                    paper_bgcolor: 'rgba(0,0,0,0)',
                    plot_bgcolor: 'rgba(0,0,0,0)',
                    shapes: vlines,
                    margin: { b: 30 },
                }}
            />
            {/* DateRange controls */}
            <div className="no-gutters row date-controls">
                {
                    ([ , "1y" , "2y" , "3y" , "4y" , "5y" , "All", ] as (DateRange & string)[]).map(dr =>
                        <input type="button"
                            key={dr}
                            value={dr}
                            className="date-range-button"
                            onClick={() => setDateRange( dr) }
                            disabled={dateRange ==  dr}
                        />
                    )
                }
            </div>
            {/* Other radio/checklist configs */}
            <div className="no-gutters row">
                <Radios label="Region" options={["All", "NYC", "JC"]} cb={setRegion} choice={region} />
                <Radios label="User Type" options={["All", "Subscriber", "Customer"]} cb={setUserType} choice={userType} />
                <Radios label="Y Axis" options={["Rides", "Ride minutes"]} cb={setYAxis} choice={yAxis} />
                <Checklist
                    label="Rolling Avg"
                    data={[{ name: "12mo", data: 12, checked: rollingAvgs.includes(12) }]}
                    cb={setRollingAvgs}
                >
                    <Checkbox
                        id="showlegend"
                        label="Legend"
                        checked={showlegend}
                        cb={setShowLegend}
                    />
                </Checklist>
                <Radios
                    label="Stack by"
                    options={[
                        "None",
                        "User Type",
                        {
                            label: GenderLabel,
                            data: "Gender",
                        },
                    ]}
                    cb={setStackBy}
                    choice={stackBy}
                >
                    <Checkbox
                        id="stack-relative"
                        label="Percentages"
                        checked={stackRelative}
                        cb={setStackRelative}
                    />
                </Radios>
                <Checklist<Gender>
                    label={GenderLabel}
                    data={[
                        { name: 'Male', data: 'Male', checked: genders.includes('Male') },
                        { name: 'Female', data: 'Female', checked: genders.includes('Female') },
                        { name: 'Other / Unspecified', data: 'Other / Unspecified', checked: genders.includes('Other / Unspecified') },
                    ]}
                    cb={setGenders}
                />
                <ReactTooltip id="gender-label-tooltip" className="label-tooltip">
                    <div>Gender data no longer published</div>
                    <div>(as of February 2021)</div>
                </ReactTooltip>
            </div>
            <div className="no-gutters row">
                <div className="col-md-12">
                    <Markdown>{`
## About
Use the controls above to filter/stack by region, user type, gender, or date, and toggle aggregation of rides or total ride minutes, e.g.:
- [JC only](#?r=jc)
- [Ride minute %'s, Male vs. Female](#?d=1406-2101&g=mf&pct&s=g&y=m) (Jun 2014 - January 2021, the window where 12mo rolling avgs are possible)

This plot should refresh when [new data is published by Citibike](https://www.citibikenyc.com/system-data) (typically around the 2nd week of each month, covering the previous month).

[The GitHub repo](https://github.com/neighbor-ryan/ctbk.dev) has more info as well as [planned enhancements](https://github.com/neighbor-ryan/ctbk.dev/issues).
                    `}</Markdown>
                    <h3 id="qc">🚧 Data-quality issues 🚧</h3>
                    <Markdown>{`
Several things changed in February 2021 (presumably as part of [the Lyft acquistion](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service)):
- "Gender" information no longer provided (all rides labeled "unknown" starting February 2021)
- A new "Rideable Type" field was added, containing values \`docked_bike\` and \`electric_bike\` 🎉; however, it is mostly incorrect at present, and disabled above:
  - Prior to February 2021, the field is absent (even though e-citibikes were in widespread use before then)
  - Since February 2021, only a tiny number of rides are labeled \`electric_bike\` (122 in April 2021, 148 in May, 113 in June); this is certainly not accurate!
    - One possible explanation: [electric citibikes were launched in Jersey City and Hoboken around April 2021](https://www.hobokengirl.com/hoboken-jersey-city-citi-bike-share-program/); perhaps those bikes were part of a new fleet that show up as \`electric_bike\` in the data (while extant NYC e-citibikes don't).
    - These \`electric_bike\` rides showed up in the default ("NYC") data, not the "JC" data, but it could be all in flux; February through April 2021 were also updated when the May 2021 data release happened in early June.
- The "User Type" values changed ("Subscriber" → "member", "Customer" → "casual"); I'm using the former/old values here, they seem equivalent.
                    `}</Markdown>
                    <div className="footer">
                        Code: { icon(     'gh', 'https://github.com/neighbor-ryan/ctbk.dev#readme',    'GitHub logo') }
                        Data: { icon(     's3',         'https://s3.amazonaws.com/ctbk/index.html', 'Amazon S3 logo') }
                      Author: { icon('twitter',                  'https://twitter.com/RunsAsCoded',   'Twitter logo') }
                    </div>
                </div>
            </div>
        </div>
    );
}
