import moment from 'moment';
import _ from "lodash";
import React, {useMemo, useState} from 'react';
import css from "./index.module.css"
import controlCss from "../src/controls.module.css"

import * as Plotly from "plotly.js"
import {Layout} from "plotly.js"

import {Checkbox} from "../src/checkbox";
import {Checklist} from "../src/checklist";
import {DateRange, DateRange2Dates, dateRangeParam} from "../src/date-range";
import Head from "../src/head"
import {Radios} from "../src/radios";

import {getBasePath} from "next-utils/basePath"
import {loadSync} from "next-utils/load"
import MD from "next-utils/md"
import {Arr, filterEntries, filterKeys, mapValues, order, sumValues, entries, values, keys, fromEntries, } from "next-utils/objs"
import { boolParam, enumMultiParam, enumParam, numberArrayParam, Param, ParsedParam, parseQueryParams, } from "next-utils/params";
import {
    Colors,
    Gender,
    GenderQueryStrings,
    GenderRollingAvgCutoff,
    Genders,
    Int2Gender,
    NormalizeRideableType,
    Region,
    RegionQueryStrings,
    Regions,
    RideableType,
    RideableTypeChars,
    RideableTypes,
    rollingAvg,
    Row,
    StackBy,
    StackBys,
    stackKeyDict,
    UserType,
    UserTypeQueryStrings,
    UserTypes,
    YAxes,
    YAxis,
    yAxisLabelDict,
} from "../src/data";

import dynamic from 'next/dynamic'
import Link from "next/link";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false, })
const Tooltip = dynamic(() => import("react-tooltip").then(m => m.Tooltip), { ssr: false, })
import 'react-tooltip/dist/react-tooltip.css'
import {darken} from "../src/colors";

const JSON_PATH = 'public/assets/ymrgtb_cd.json'

export async function getStaticProps(context: any) {
    const data = loadSync<Row[]>(JSON_PATH)
    return { props: { data } }
}

type Params = {
    y: Param<YAxis>
    u: Param<UserType[]>
    s: Param<StackBy>
    pct: Param<boolean>
    r: Param<Region[]>
    g: Param<Gender[]>
    rt: Param<RideableType[]>
    d: Param<DateRange>
    rolling: Param<number[]>
}

type ParsedParams = {
    y: ParsedParam<YAxis>
    u: ParsedParam<UserType[]>
    s: ParsedParam<StackBy>
    pct: ParsedParam<boolean>
    r: ParsedParam<Region[]>
    g: ParsedParam<Gender[]>
    rt: ParsedParam<RideableType[]>
    d: ParsedParam<DateRange>
    rolling: ParsedParam<number[]>
}

export default function App({ data, }: { data: Row[] }) {
    const params: Params = {
        y: enumParam('Rides', YAxes),
        u: enumMultiParam(UserTypes, UserTypeQueryStrings, ''),
        s: enumParam('None', StackBys),
        pct: boolParam,
        r: enumMultiParam(Regions, RegionQueryStrings, ''),
        g: enumMultiParam(Genders, GenderQueryStrings, ''),
        rt: enumMultiParam(RideableTypes, RideableTypeChars, ''),
        d: dateRangeParam(),
        rolling: numberArrayParam([ 12 ]),
    }

    const {
        y: [ yAxis, setYAxis ],
        u: [ userTypes, setUserTypes ],
        s: [ stackBy, setStackBy ],
        pct: [ stackRelative, setStackRelative ],
        r: [ regions, setRegions ],
        g: [ genders, setGenders ],
        rt: [ rideableTypes, setRideableTypes ],
        d: [ dateRange, setDateRange ],
        rolling: [ rollingAvgs, setRollingAvgs ],
    }: ParsedParams = parseQueryParams({ params })

    const [ showLegend, setShowLegend ] = useState(true)

    // console.log("Regions", regions, "User Type", userType, "Y-Axis", yAxis, "Date range:", dateRange, "Last row:")
    // console.log(data && data[data.length - 1])
    const stackPercents = useMemo(() => stackRelative && stackBy != 'None', [ stackBy, stackRelative ])
    const hovertemplate = stackPercents ? "%{y:.0%}" : "%{y:,.0f}"

    const { hoverLabel: yHoverLabel, title } = useMemo(() => yAxisLabelDict[yAxis], [ yAxis ])
    const subtitle = useMemo(() => {
        let parendStrings = []
        if (regions && regions.length < Regions.length) {
            parendStrings.push(`${regions.join("+")}`)
        }
        if (stackPercents) {
            parendStrings.push(`%, by ${stackBy}`)
        }
        return (parendStrings.length) ? `${parendStrings.join(", ")}` : undefined
    }, [ regions, stackPercents, stackBy, ] )

    const stackKeys = stackKeyDict[stackBy]

    const filtered = useMemo(
        () =>
            data
                .map((r) => {
                    // Normalize gender, rideable type
                    const { Gender, 'Rideable Type': rideableType, ...rest } = r
                    return { Gender: Int2Gender[Gender], 'Rideable Type': NormalizeRideableType[rideableType], ...rest }
                })
                .filter((r) => {
                    // Apply filters
                    if (regions.indexOf(r.Region) == -1) return false
                    if (userTypes.indexOf(r['User Type']) == -1) return false
                    if (genders.indexOf(r.Gender) == -1) return false
                    if (rideableTypes.indexOf(r['Rideable Type']) == -1) {
                        console.warn("Dropping", r['Count'], "rides from with unrecognized rideable type", r['Rideable Type'], r)
                        return false
                    }
                    return true
                }),
        [ data, regions, userTypes, genders, rideableTypes ]
    )

    // Build multi-index in both orders:
    // - month -> stackVal -> count
    // - stackVal -> month -> count
    let monthsData: { [month: string]: { [stackVal: string]: number }} = {}
    let stacksData: { [stackVal: string]: { [month: string]: number }} = {};
    filtered.forEach((r) => {
        const month: string = `${r.Year.toString()}-${r.Month.toString().padStart(2, '0')}`
        const stackVal: Region | Gender | UserType | RideableType | '' = stackBy == 'None' ? '' : r[stackBy]
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
    stacksData = fromEntries(
        stackKeys
            .filter(stackVal => stackVal in stacksData)
            .map(stackVal => [ stackVal, order(stacksData[stackVal]) ])
    )
    let months: string[] = Arr(keys(monthsData))

    if (stackBy && stackPercents) {
        const monthTotals = mapValues(
            monthsData,
            (month, stackVals) => sumValues(stackVals)
        )
        monthsData = mapValues(
            monthsData,
        (month, stackVals) =>
            mapValues(
                stackVals,
            (stackVal, count) => count / monthTotals[month]
            )
        )
        stacksData = mapValues(
            stacksData,
        (stackVal, values) =>
            mapValues(
                values,
            (month, count) => count / monthTotals[month]
            )
        )
    }

    // If `end` isn't set, default to 1d after the latest fetched data point (since it's generally treated as an
    // exclusive bound)
    const last = moment(_.max(data.map(r => new Date(r.Year, r.Month - 1,)))).add(1, 'd').toDate()
    const { start, end } = DateRange2Dates(dateRange, last)

    // Compute a (trailing) rolling average for:
    // - [each time-window length in `rollingAvgs`] (typically just [12])], x
    // - [each stacked value (e.g. "Male", "Female", "Unspecified")]
    let rollingSeries: { [month: string]: number }[] = []
    rollingSeries = rollingSeries.concat(
        ...values(stacksData)
            .map((months) => {
                // let vals: { month: Date, v: number }[] =
                //     entries(months)
                //         .map(
                //             ([ month, v ]) => { return { month: new Date(month), v } }
                //         )
                if (stackBy == 'Gender') {
                    months = filterKeys(months, month => new Date(month) < GenderRollingAvgCutoff)
                    // vals = vals.filter(({ month }) => month < GenderRollingAvgCutoff)
                }
                return rollingAvgs.map(n => rollingAvg(months, n))
            })
    )

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

    // Create Plotly trace data, including colors (when stacking)
    const rollingTraces: Plotly.Data[] = rollingSeries.map(
        (series, idx) => {
            const stackVal = stackKeys[idx] || 'Total'
            const name = stackVal == 'Total' ? '12mo avg' : `${stackVal} (12mo)`
            const colors: { [k: string]: string } = Colors[stackBy]
            const color = stackVal == 'Total' ? 'black' : darken(colors[stackVal], .75)
            // console.log(`rolling ${name}:`, months, y)
            return {
                name,
                x: Arr(keys(series)),
                y: Arr(values(series)),
                type: 'scatter',
                marker: { color, },
                line: { width: 4, },
                hovertemplate,
            }
        }
    )

    console.log("stacksData:", stacksData)

    // Bar data (including color fades when stacking)
    const barTraces: Plotly.Data[] =
        entries(stacksData)
            .map(([stackVal, values], idx) => {
                const x = months
                const y = months.map((month) => values[month] || NaN)
                const name = stackVal || yHoverLabel
                const colors: { [k: string]: string } = Colors[stackBy]
                const color = colors[stackVal]
                return {
                    x, y, name,
                    type: 'bar',
                    marker: { color, },
                    hovertemplate
                }
            })

    const traces: Plotly.Data[] = barTraces.concat(rollingTraces)

    const basePath = getBasePath()

    function icon(src: string, href: string, title: string) {
        return <a href={href} title={title}>
            <img className={css.icon} alt={title} src={`${basePath}/assets/${src}.png`} />
        </a>
    }

    const GenderLabel = (suffix: number | string) =>
        <span>
            Gender
            <span id={`gender-label-tooltip-${suffix}`}>
                <img className={css.warning} alt={"warning icon"} src={`${basePath}/assets/warning.png`} />
            </span>
            <Tooltip anchorId={`gender-label-tooltip-${suffix}`} className={css.tooltip} events={['click', 'hover',]}>
                <div>Gender data no longer published</div>
                <div>(as of February 2021)</div>
            </Tooltip>
        </span>

    const gridcolor = "#ddd"
    const showlegend = showLegend == null ? (stackBy != 'None') : showLegend
    const layout: Partial<Layout> = {
        autosize: true,
        barmode: 'stack',
        showlegend,
        hovermode: "x",
        legend: {
            x: 0.5,
            xanchor: 'center',
            yanchor: 'top',
            orientation: 'h',
            traceorder: "normal",
        },
        xaxis: {
            tickfont: { size: 14 },
            titlefont: { size: 14 },
            gridcolor,
        },
        yaxis: {
            automargin: true,
            gridcolor,
            tickfont: { size: 14 },
            titlefont: { size: 14 },
            tickformat: stackPercents ? ".0%" : undefined,
            range: stackPercents ? [ 0, 1.01, ] : undefined,
        },
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        margin: { t: 0, r: 0, b: 40, l: 0, },
    }

    return (
        <div id="plot" className={css.container}>
            <Head
                title={title}
                description={"Graph of Citi Bike ridership over time"}
                thumbnail={`ctbk-rides`}
            />
            <div className={css.titleContainer}>
                <h1 className={css.title}>{title}</h1>
                {subtitle && <p className={css.subtitle}>{subtitle}</p>}
            </div>
            {/* Main plot: bar graph + rolling avg line(s) */}
            <Plot
                className={css.plotly}
                onDoubleClick={() => setDateRange('All')}
                onRelayout={e => {
                    if (!('xaxis.range[0]' in e && 'xaxis.range[1]' in e)) return
                    let [ start, end ] = [ e['xaxis.range[0]'], e['xaxis.range[1]'], ].map(s => s ? new Date(s) : undefined)
                    start = start ? moment(start).subtract(1, 'month').toDate() : start
                    const dateRange = (!start && !end) ? 'All' : { start, end, }
                    // console.log("relayout:", e, start, end, dateRange,)
                    setDateRange(dateRange)
                }}
                data={traces}
                useResizeHandler
                layout={layout}
                config={{ displayModeBar: false, /*responsive: true,*/ }}
            />
            {/* DateRange controls */}
            <div className={`no-gutters row`}>
                <div className={`${css.dateControls} ${controlCss.control}`}>
                    <label className={controlCss.controlHeader}>Dates</label>
                {
                    ([ "1y", "2y", "3y", "4y", "5y", "All" ] as (DateRange & string)[])
                        .map(dr =>
                                <input
                                    type="button"
                                    key={dr}
                                    value={dr}
                                    className={css.dateRangeButton}
                                    onClick={() => setDateRange( dr) }
                                    disabled={dateRange ==  dr}
                                />
                        )
                }
                </div>
                <Checklist
                    label={"Region"}
                    data={Regions.map(region => ({ name: region, data: region, checked: regions.includes(region), }))}
                    cb={setRegions}
                />
                <Checklist
                    label={"User Type"}
                    data={UserTypes.map(userType => ({ name: userType, data: userType, checked: userTypes.includes(userType), }))}
                    cb={setUserTypes}
                />
                <Radios label="Y Axis" options={["Rides", "Ride minutes"]} cb={setYAxis} choice={yAxis} />
                <Checklist
                    label="Rolling Avg"
                    data={[{ name: "12mo", data: 12, checked: rollingAvgs.includes(12) }]}
                    cb={setRollingAvgs}
                >
                    <Checkbox
                        id={`${css.showlegend}`}
                        label="Legend"
                        checked={showlegend}
                        cb={setShowLegend}
                    />
                </Checklist>
                <Radios
                    label="Stack by"
                    options={[
                        "None",
                        "Region",
                        "User Type",
                        {
                            label: GenderLabel(1),
                            data: "Gender",
                        },
                    ]}
                    cb={setStackBy}
                    choice={stackBy}
                >
                    <Checkbox
                        id="stack-relative"
                        label="%"
                        checked={stackRelative}
                        cb={setStackRelative}
                    />
                </Radios>
                <Checklist
                    label={GenderLabel(2)}
                    data={[
                        { name: 'Male', data: 'Male', checked: genders.includes('Male') },
                        { name: 'Female', data: 'Female', checked: genders.includes('Female') },
                        { name: 'Unspecified', data: 'Unspecified', checked: genders.includes('Unspecified') },
                    ]}
                    cb={setGenders}
                />
            </div>
            <div className={`no-gutters row ${css.row}`}>
                <div className="col-md-12">
                    <h2>About</h2>
                    <p>Use the controls above to filter/stack by region, user type, gender, or date, and toggle aggregation of rides or total ride minutes, e.g.:</p>
                    <ul>
                        <li><Link href={"/?r=jh"}>JC+Hoboken</Link></li>
                        <li><Link href={"/?y=m&s=g&pct=&g=mf&d=1406-2101"}>Ride minute %'s, Male vs. Female</Link> (Jun 2014 - January 2021, the window where 12mo rolling avgs are possible)</li>
                    </ul>
                    <p>This plot should refresh when <a href={"https://www.citibikenyc.com/system-data"}>new data is published by Citibike</a> (typically around the 2nd week of each month, covering the previous month).</p>
                    <p><a href={"https://github.com/neighbor-ryan/ctbk.dev"}>The GitHub repo</a> has more info as well as <a href={"https://github.com/neighbor-ryan/ctbk.dev/issues"}>planned enhancements</a>.</p>
                    <p>Also, check out <Link href={"./stations"}>this map visualization of stations and their ridership counts in August 2022</Link>.</p>
                    <h3 id="qc">🚧 Data-quality issues 🚧</h3>
                    {MD(`
Several things changed in February 2021 (presumably as part of [the Lyft acquistion](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service)):
- "Gender" information no longer provided (all rides labeled "unknown" starting February 2021)
- A new "Rideable Type" field was added, containing values \`docked_bike\` and \`electric_bike\` 🎉; however, it is mostly incorrect at present, and disabled above:
  - Prior to February 2021, the field is absent (even though e-citibikes were in widespread use before then)
  - Since February 2021, only a tiny number of rides are labeled \`electric_bike\` (122 in April 2021, 148 in May, 113 in June); this is certainly not accurate!
    - One possible explanation: [electric citibikes were launched in Jersey City and Hoboken around April 2021](https://www.hobokengirl.com/hoboken-jersey-city-citi-bike-share-program/); perhaps those bikes were part of a new fleet that show up as \`electric_bike\` in the data (while extant NYC e-citibikes don't).
    - These \`electric_bike\` rides showed up in the default ("NYC") data, not the "JC" data, but it could be all in flux; February through April 2021 were also updated when the May 2021 data release happened in early June.
- The "User Type" values changed ("Annual" → "member", "Daily" → "casual"); I'm using the former/old values here, they seem equivalent.
                    `)}
                    <div className={css.footer}>
                        Code: { icon(     'gh', 'https://github.com/neighbor-ryan/ctbk.dev#readme',    'GitHub logo') }
                        Data: { icon(     's3',         'https://s3.amazonaws.com/ctbk/index.html', 'Amazon S3 logo') }
                      Author: { icon('twitter',                  'https://twitter.com/RunsAsCoded',   'Twitter logo') }
                    </div>
                </div>
            </div>
        </div>
    );
}
