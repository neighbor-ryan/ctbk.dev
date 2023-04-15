import {DataFrame, Series} from "danfojs";
import * as danfo from "../src/danfo"
import {clampIndex, pivot} from "../src/danfo"
import moment from 'moment';
import _ from "lodash";
import React, {ReactNode, useMemo, useState} from 'react';
import css from "./index.module.css"
import controlCss from "../src/controls.module.css"

import {Data, Layout} from "plotly.js"

import {Checkbox} from "../src/checkbox";
import {Checklist} from "../src/checklist";
import {DateRange, DateRange2Dates, dateRangeParam} from "../src/date-range";
import Head from "../src/head"
import {Radios} from "../src/radios";

import {getBasePath} from "next-utils/basePath"
import {loadSync} from "next-utils/load"
import MD from "next-utils/md"
import {concat, fromEntries, mapValues, o2a,} from "next-utils/objs"
import { boolParam, enumMultiParam, enumParam, numberArrayParam, Param, ParsedParam, parseQueryParams,
} from "next-utils/params";
import { Colors, Gender, GenderQueryStrings, GenderRollingAvgCutoff, Genders, Int2Gender, NormalizeRideableType, Region, RegionQueryStrings, Regions, RideableType, RideableTypeChars, RideableTypes, Row, StackBy, StackBys, stackKeyDict, toYM, UnknownRideableCutoff, UserType, UserTypeQueryStrings, UserTypes, YAxes, YAxis, yAxisLabelDict,
} from "../src/data";

import dynamic from 'next/dynamic'
import Link from "next/link";
import 'react-tooltip/dist/react-tooltip.css'

import {darken} from "../src/colors";
import {Figure} from "react-plotly.js";

import { useEffect } from "react";
const Plot = dynamic(() => import("react-plotly.js"), { ssr: false, })
const Tooltip = dynamic(() => import("react-tooltip").then(m => m.Tooltip), { ssr: false, })

const {pow} = Math

const JSON_PATH = 'public/assets/ymrgtb_cd.json'
import {LAST_MONTH_PATH} from "../src/paths";

export async function getStaticProps(context: any) {
    const data = loadSync<Row[]>(JSON_PATH)
    const lastMonthStr = loadSync<string>(LAST_MONTH_PATH)
    return { props: { data, lastMonthStr } }
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
    avg: Param<number[]>
    dl: Param<boolean>
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
    avg: ParsedParam<number[]>
    dl: ParsedParam<boolean>
}

type MonthVal = { m: string, v: number }
type AnnualizedPercent = {
    numYrs: number
    numMos: number
    first: { m: string, v: number }
    last: { m: string, v: number }
    ratio: number
    percent: number
}

function annualizedPercents(series: Series): AnnualizedPercent[] {
    if (!series.size) {
        return []
    }
    const filtered = series.dropNa()
    const numMos = filtered.size
    const maxRange = numMos - 1
    let delta = 12
    const percents = []
    while (true) {
        if (delta > maxRange) {
            delta = maxRange
        }
        const end = filtered.iloc([maxRange])
        const start = filtered.iloc([maxRange - delta])
        const first: MonthVal = { m: start.index[0] as string, v: start.values[0] as number }
        const last: MonthVal = { m: end.index[0] as string, v: end.values[0] as number }
        const ratio = last.v / first.v
        const numYrs = delta / 12
        const rate = pow(ratio, 1/ numYrs) - 1
        const percent = rate * 100
        percents.push({ numYrs, numMos: delta, first, last, ratio, percent })
        if (delta == maxRange) break
        delta += 12
    }
    return percents
}

function annualPercentStr({ numYrs, numMos, first, last, ratio, percent }: AnnualizedPercent) {
    function ymStr(m: string): string {
        return `${parseInt(m.substring(5))}/${m.substring(2, 4)}`
    }
    const datesStr = `${ymStr(first.m)}-${ymStr(last.m)}`
    const yrsMsg =
        (numMos % 12 == 0)
            ? `${numYrs.toFixed(0)} year${numMos == 12 ? "" : "s"} (${datesStr})`
            : `${numYrs.toFixed(1)} years (${numMos} mos, ${datesStr})`
    const [ percentStr, typeStr ] = percent >= 0 ? [ percent.toFixed(1), "increase" ] : [ (-percent).toFixed(1), "decrease" ]
    return `${yrsMsg}: ${first.v.toFixed(0)} → ${last.v.toFixed(0)} (${ratio.toFixed(1)}x), ${percentStr}% avg annual ${typeStr}`
}

const WarningLabel = ({ label, id, children }: { label: string, id: string, children: ReactNode, }) => {
    const basePath = getBasePath()
    return (
        <span>
            {label}
            <span id={id}>
                <img className={css.warning} alt={"warning icon"} src={`${basePath}/assets/warning.png`}/>
            </span>
            <Tooltip anchorId={id} className={css.tooltip}>{children}</Tooltip>
        </span>
    )
}

const GenderLabel = (suffix: number | string) =>
    <WarningLabel label={"Gender"} id={`gender-label-tooltip-${suffix}`}>
        <div>Gender data no longer published</div>
        <div>(as of February 2021)</div>
    </WarningLabel>

const BikeTypeLabel = (suffix: number | string) =>
    <WarningLabel label={"Bike Type"} id={`bike-type-label-tooltip-${suffix}`}>
        <div>E-bike data seems to be</div>
        <div>mostly missing / undercounted</div>
    </WarningLabel>

type InitializedPlot = {
    figure: Readonly<Figure>
    graphDiv: Readonly<HTMLElement>
    set: boolean
}

type PlotInitialized = { time: number, set: boolean }

export default function App({ data, lastMonthStr }: { data: Row[], lastMonthStr: string }) {
    const params: Params = {
        y: enumParam('Rides', YAxes),
        u: enumMultiParam(UserTypes, UserTypeQueryStrings, ''),
        s: enumParam('None', StackBys),
        pct: boolParam,
        r: enumMultiParam(Regions, RegionQueryStrings, ''),
        g: enumMultiParam(Genders, GenderQueryStrings, ''),
        rt: enumMultiParam(RideableTypes, RideableTypeChars, ''),
        d: dateRangeParam(),
        avg: numberArrayParam([ 12 ]),
        dl: boolParam,
    }

    const lastMonthDisplayStr = useMemo(
        () => {
            const lastMonthYear = parseInt(lastMonthStr.substring(0, 4))
            const lastMonthIdx = parseInt(lastMonthStr.substring(4, 6)) - 1
            const lastMonth = new Date(lastMonthYear, lastMonthIdx)
            return lastMonth.toLocaleDateString('en-us', { month: "short", year: "numeric" })

        },
        [ lastMonthStr ]
    )

    const {
        y: [ yAxis, setYAxis ],
        u: [ userTypes, setUserTypes ],
        s: [ stackBy, setStackBy ],
        pct: [ stackRelative, setStackRelative ],
        r: [ regions, setRegions ],
        g: [ genders, setGenders ],
        rt: [ rideableTypes, setRideableTypes ],
        d: [ dateRange, setDateRange ],
        avg: [ rollingAvgs, setRollingAvgs ],
        dl: [ downloadPlotImage, setDownloadPlotImage ],
    }: ParsedParams = parseQueryParams({ params })

    let df = useMemo(
        () => {
            let df = new DataFrame(data)
            let m = new Series(
                df
                    .loc({ columns: ['Year', 'Month'] })
                    .apply(([ y, m ]: [ number, number ]) => `${y}-${m.toString().padStart(2, "0")}`)
                    .values
            )
            df = (
                df
                    .drop({ columns: ['Year', 'Month'] })
                    .addColumn("m", m)
                    .rename({ 'Count': 'Rides' })
            )
            df = df.addColumn('Ride minutes', df.column('Duration').div(60))
            df = df.drop({ columns: [ 'Duration' ]})
            return df
        },
        [ data ]
    )

    const [ showLegend, setShowLegend ] = useState(true)

    // console.log("Regions", regions, "User Type", userType, "Y-Axis", yAxis, "Date range:", dateRange, "Last row:")
    // console.log(data && data[data.length - 1])
    const { stackKeys, stackPercents, hovertemplate } = useMemo(
        () => {
            const stackPercents = stackRelative && stackBy != 'None'
            const hovertemplate = stackPercents ? "%{y:.0%}" : "%{y:,.0f}"
            return ({
                stackKeys: stackKeyDict[stackBy] as string[],
                stackPercents,
                hovertemplate,
            })
        },
        [ stackBy, stackRelative ]
    )

    const { hoverLabel: yHoverLabel, title } = useMemo(() => yAxisLabelDict[yAxis], [ yAxis ])
    const subtitle = useMemo(() => {
        let parendStrings = []
        if (regions && regions.length < Regions.length) {
            parendStrings.push(`${regions.join("+")}`)
        }
        if (rideableTypes.length && rideableTypes.length < RideableTypes.length) {
            parendStrings.push(`${rideableTypes.join("/")} bikes`)
        }
        if (userTypes.length && userTypes.length < UserTypes.length) {
            const [userType] = userTypes
            parendStrings.push({ Annual: "Annual members", Daily: "Daily customers" }[userType])
        }
        const byName = stackBy == 'None' ? undefined : (stackBy == 'Rideable Type' ? 'Bike Type' : stackBy)
        if (stackPercents && byName) parendStrings.push(`% by ${byName}`)
        else if (stackPercents) parendStrings.push(`%`)
        else if (byName) parendStrings.push(`by ${byName}`)
        return (parendStrings.length) ? `${parendStrings.join(", ")}` : undefined
    }, [ regions, rideableTypes, userTypes, stackPercents, stackBy, ] )

    const normalized = useMemo(
        () => (
            df
                .drop({ columns: 'Gender' })
                .addColumn('Gender', df.Gender.map((g: number) => Int2Gender[g]))
                .drop({ columns: 'Rideable Type' })
                .addColumn('Rideable Type', df['Rideable Type'].map((r: RideableType) => NormalizeRideableType[r]))
        ),
        [df]
    )

    const fdf = useMemo(
        () => {
            // print("normalized", normalized)
            const filters = {
                Region: regions,
                'User Type': userTypes,
                Gender: genders,
                'Rideable Type': rideableTypes,
            }
            const [ mask0, ...masks ] = o2a(
                filters,
                (k: string, vs: string[]) => {
                    return normalized[k].apply((v: string) => vs.includes(v))
                }
            )
            const mask = masks.reduce((a, b) => a.and(b), mask0)
            // print("mask counts", mask.valueCounts())
            const filtered = normalized.loc({ rows: mask })
            // print("filtered", filtered)
            return filtered
        },
        [ normalized, regions, userTypes, genders, rideableTypes, ]
    )

    // If `end` isn't set, default to 1d after the latest fetched data point (since it's generally treated as an
    // exclusive bound)
    const { start, end } = useMemo(
        () => {
            const last = moment(_.max(data.map(r => new Date(r.Year, r.Month - 1,)))).add(1, 'M').toDate()
            const { start, end } = mapValues<Date, string>(
                DateRange2Dates(dateRange, last),
                (_, d) => toYM(d)
            )
            console.log("computed start, end", { start, end }, "last:", last)
            return { start, end }
        },
        [ data, dateRange, ]
    )

    const { grouped, pivoted } = useMemo(
        () => {
            const rename: {[k: string]: string} = {}
            rename[`${yAxis}_sum`] = yAxis
            let groupCols = stackBy == 'None' ? ['m'] : ['m', stackBy]
            let grouped: DataFrame | null = null
            if (fdf.shape[0]) {
                grouped = fdf.groupby(groupCols).col([yAxis]).sum().rename(rename)
            }
            let pivoted: DataFrame | null = null
            if (grouped) {
                if (stackBy == 'None') {
                    pivoted = grouped.setIndex({column: 'm', drop: true}).sortIndex()
                } else {
                    pivoted = pivot(grouped, 'm', stackBy, yAxis)
                    const columns = pivoted.columns
                    pivoted = pivoted.loc({columns: stackKeys.filter(k => columns.includes(k))})
                    pivoted = pivoted.sortIndex()
                }
                // print("grouped", grouped)
            }

            if (stackPercents && pivoted) {
                const sums = pivoted.sum()
                // print("percents sums", sums)
                pivoted = pivoted.div(sums, { axis: 0 })
            }

            // print("pivoted", pivoted)
            return { grouped, pivoted }
        },
        [ yAxis, stackBy, stackPercents, fdf ]
    )

    type NDF = { [p: number]: DataFrame }
    type RollingSerie = { stackVal: string, n: number, s: Series }
    const rollingSeries: RollingSerie[] = useMemo(
        () => {
            if (stackBy == 'None') {
                if (!grouped) return []
                const rename: {[k: string]: string} = {}
                rename[`${yAxis}_sum`] = yAxis
                const series = (
                    grouped
                        .groupby(['m'])
                        .col([yAxis])
                        .sum()
                        .rename(rename)
                        .setIndex({column: 'm', drop: true})
                        .sortIndex()
                        [yAxis] as Series
                )
                // print("pre-roll", series)
                return rollingAvgs.map(n => {
                    const rolls = danfo.rollingAvgs(series, n)
                    // print("rolls", rolls)
                    const clamped = clampIndex(rolls, {start, end})
                    annualizedPercents(clamped).forEach(percent => console.log(annualPercentStr(percent)))
                    return { stackVal: '', n, s: clamped }
                })
            } else {
                if (!pivoted) return []
                let clampEnd = end
                if (stackBy == 'Gender' && end > GenderRollingAvgCutoff) {
                    clampEnd = GenderRollingAvgCutoff
                }
                let avgs: NDF = fromEntries(
                    rollingAvgs.map(n => {
                        // print(`pre-roll pivot`, pivoted)
                        const rolled = danfo.rollingAvgs(pivoted, n)
                        // print(`rolled`, rolled)
                        const clamped = clampIndex(rolled, { start, end: clampEnd })
                        // print(`clamped`, clamped)
                        return [ n, clamped, ]
                    })
                )
                return concat(
                    o2a<number, DataFrame, RollingSerie[]>(
                        avgs,
                        (n, df) => {
                            // print(`rolling df`, df)
                            return df.columns.map(stackVal => {
                                let s = df[stackVal] as Series
                                if (stackBy == 'Rideable Type' && stackVal == 'Unknown' && end > UnknownRideableCutoff) {
                                    s = clampIndex(s, { end: UnknownRideableCutoff })
                                }
                                annualizedPercents(s).forEach(percent => console.log(`${stackVal}: ${annualPercentStr(percent)}`))
                                return {stackVal, n, s}
                            })
                        }
                    )
                )
            }
        },
        [ pivoted, grouped, start, end, yAxis, rollingAvgs, ]
    )

    console.log("rollingSeries:", rollingSeries, rollingSeries[0]?.s?.shape)

    const legendRanks: { [stackVal: string]: number } = useMemo(
        () => (
            stackBy == 'None'
                ? { '': 0 }
                : fromEntries(
                    stackKeys
                        .filter(stackVal => pivoted?.columns.includes(stackVal))
                        .map((stackVal, idx) => [ stackVal, -idx ])
                )
        ),
        [ stackBy, stackKeys, pivoted ]
    )

    const pivotedClamped = useMemo(
        () => {
            if (!pivoted) return null
            const pivotedClamped = clampIndex(pivoted, { start, end })
            // print("pivotedClamped", pivotedClamped)
            return pivotedClamped
        },
        [ pivoted, start, end ]
    )

    // Create Plotly trace data, including colors (when stacking)
    const traces: Data[] = useMemo(
        () => {
            const rollingTraces: Data[] = rollingSeries.map(
                ({ stackVal, n, s }) => {
                    const stackName = stackVal || 'Total'
                    const name = stackVal ? `${stackVal} (${n}mo)` : `${n}mo avg`
                    const colors: { [k: string]: string } = Colors[stackBy]
                    const color = stackVal ? darken(colors[stackName], .75) : 'black'
                    return {
                        name,
                        x: s.index,
                        y: s.values,
                        type: 'scatter',
                        marker: {color,},
                        line: {width: 4,},
                        hovertemplate,
                        legendrank: 101 + 2 * legendRanks[stackVal]
                    } as Data
                }
            )
            console.log("rollingTraces:", rollingTraces)

            // Bar data (including color fades when stacking)
            const barTraces: Data[] = pivotedClamped && pivotedClamped.columns.map(k => {
                const series = pivotedClamped[k]
                const x = series.index
                const y = series.values
                const stackVal = stackBy == 'None' ? '' : k
                const name = stackVal ? k : yHoverLabel
                const colors: { [k: string]: string } = Colors[stackBy]
                const color = colors[stackVal]
                return {
                    x, y, name,
                    type: 'bar',
                    marker: { color, },
                    hovertemplate,
                    legendrank: 100 + 2*legendRanks[stackVal],
                    //selectedpoints: selectedX ? undefined : [],
                }
            }) || []
            console.log("barTraces:", barTraces)

            return barTraces.concat(rollingTraces)
        },
        [ rollingSeries, pivotedClamped, stackBy, ]
    )

    const basePath = getBasePath()

    function icon(src: string, href: string, title: string) {
        return <a href={href} title={title} target={"_blank"}>
            <img className={css.icon} alt={title} src={`${basePath}/assets/${src}.png`} />
        </a>
    }

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
            // tickangle: -45,
            tickformat: "%b '%y",
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

    const [ initialized, setInitialized ] = useState(false)
    const clickToToggle = false
    // const width = 768
    const height = 450
    const src = 'plot-fallback.png'
    const [ initializedPlot, setInitializedPlot ] = useState<InitializedPlot | null>(null)
    const [ firstRender, setFirstRender ] = useState<Date>(new Date)
    const [ plotInitialized, setPlotInitialized ] = useState<PlotInitialized | null>(null)
    useEffect(
        () => {
            if (!plotInitialized) return
            const { time } = plotInitialized
            const delayMs = time - firstRender?.getTime()
            console.log(`Plot initialized ${delayMs / 1000}s after initial page render`)
        },
        [ plotInitialized?.set ]
    )
    useEffect(
        () => {
            console.log("plotly effect:", initializedPlot, downloadPlotImage)
            if (!initializedPlot) return
            const { figure, graphDiv, set } = initializedPlot
            if (!set) return
            if (!downloadPlotImage) return
            import('plotly.js')
                .then(m => m.default)
                .then(
                    Plotly => Plotly.downloadImage(
                        figure,
                        {
                            width: graphDiv.offsetWidth,
                            height: graphDiv.offsetHeight,
                            filename: "rides",
                            format: "png",
                        }
                    )
                )
                .then(img => console.log('downloaded img:', img))
        },
        [ initializedPlot?.set ]
    )

    return (
        <div id="plot" className={css.container}>
            <Head
                title={title}
                description={"Graph of Citi Bike ridership over time"}
                thumbnail={`ctbk-rides`}
            />
            <main className={css.main}>
                <div
                    className={css.titleContainer}
                    onClick={() => clickToToggle && setInitialized(!initialized)}
                >
                    <h1 className={css.title}>{title}</h1>
                    {subtitle && <p className={css.subtitle}>{subtitle}</p>}
                </div>
                {/* Main plot: bar graph + rolling avg line(s) */}
                <div className={css.plotWrapper}>
                    <div
                        className={`${css.fallback} ${initialized ? css.hidden : ""}`}
                        style={{ height: `${height}px`, maxHeight: `${height}px` }}
                        onClick={() => clickToToggle && setInitialized(true)}
                    >
                        <img
                            alt={title}
                            src={`${basePath}/${src}`}
                            width={"100%"}
                            height={height}
                            // layout={"fill"}
                            // fill  // TODO: Next 13 requires updating <Link> in next-utils/md
                            // layout="responsive"
                            // loading="lazy"
                        />
                    </div>
                    {
                        <Plot
                            className={css.plotly}
                            style={{ visibility: initialized ? undefined : "hidden", width: "100%", height: `${height}px` }}
                            onInitialized={async (figure: Readonly<Figure>, graphDiv: Readonly<HTMLElement>) => {
                                if (!plotInitialized) {
                                    // This can still run more than once; true once-only semantics (for storing only a
                                    // timestamp only the first time this code path is hit) comes from
                                    // `useEffect(…, [ set ])` above.
                                    setPlotInitialized({ time: Date.now(), set: true })
                                }

                                console.log("initialized:", figure, graphDiv)
                                setInitializedPlot({ figure, graphDiv, set: true })
                                clickToToggle || setInitialized(true)
                            }}
                            onDoubleClick={() => setDateRange('All')}
                            onRelayout={e => {
                                if (!('xaxis.range[0]' in e && 'xaxis.range[1]' in e)) return
                                let [start, end] = [e['xaxis.range[0]'], e['xaxis.range[1]'],].map(s => s ? new Date(s) : undefined)
                                start = start ? moment(start).subtract(1, 'month').toDate() : start
                                const dateRange = (!start && !end) ? 'All' : {start, end,}
                                // console.log("relayout:", e, start, end, dateRange,)
                                setDateRange(dateRange)
                            }}
                            data={traces}
                            useResizeHandler
                            layout={layout}
                            config={{
                                displayModeBar: false,
                                // staticPlot: true,
                                // responsive: true,
                            }}
                        />
                    }
                </div>
                {/* DateRange controls */}
                <div className={css.row}>
                    <details className={css.controls}>
                        <summary><span className={css.settingsGear}>⚙</span>️</summary>
                        <div className={`${css.dateControls} ${controlCss.control}`}>
                            <label className={controlCss.controlHeader}>Dates</label>
                            {
                                ([ "1y", "2y", "3y", "4y", "5y", "All" ] as (DateRange & string)[])
                                    .map(dr =>
                                        <input
                                            type="button"
                                            key={dr}
                                            value={dr}
                                            className={`${css.dateRangeButton} ${dateRange == dr && css.activeButton || css.inactiveButton}`}
                                            onClick={() => setDateRange(dr) }
                                        />
                                    )
                            }
                        </div>
                        <Checklist
                            label={"Region"}
                            data={Regions.map(region => ({ name: region, data: region, checked: regions.includes(region), }))}
                            cb={setRegions}
                        />
                        <Radios
                            label="Stack by"
                            options={[
                                "None",
                                "Region",
                                "User Type",
                                { label: GenderLabel(1), data: "Gender", },
                                { label: BikeTypeLabel(1), data: "Rideable Type", },
                            ]}
                            cb={setStackBy}
                            choice={stackBy}
                        />
                        <div className={controlCss.control}>
                            <Checkbox
                                label="12mo avg"
                                checked={rollingAvgs.includes(12)}
                                cb={v => setRollingAvgs(v ? [12] : [])}
                            />
                            <Checkbox
                                label="Legend"
                                checked={showlegend}
                                cb={setShowLegend}
                            />
                            <Checkbox
                                label="Stack %"
                                checked={stackRelative}
                                cb={setStackRelative}
                            />
                        </div>
                        <Radios label="Y Axis" options={["Rides", { label: "Minutes", data: "Ride minutes", }]} cb={setYAxis} choice={yAxis} />
                        <Checklist
                            label={"User Type"}
                            data={UserTypes.map(userType => ({ name: userType, data: userType, checked: userTypes.includes(userType), }))}
                            cb={setUserTypes}
                        />
                        <Checklist
                            label={GenderLabel(2)}
                            data={[
                                { name: 'Men', data: 'Men', checked: genders.includes('Men') },
                                { name: 'Women', data: 'Women', checked: genders.includes('Women') },
                                { name: 'Unknown', data: 'Unknown', checked: genders.includes('Unknown') },
                            ]}
                            cb={setGenders}
                        />
                        <Checklist
                            label={BikeTypeLabel(2)}
                            data={[
                                { name: 'Classic', data: 'Classic', checked: rideableTypes.includes('Classic') },
                                // { name: 'Docked', data: 'Docked', checked: rideableTypes.includes('Docked') },
                                { name: 'Electric', data: 'Electric', checked: rideableTypes.includes('Electric') },
                                { name: 'Unknown', data: 'Unknown', checked: rideableTypes.includes('Unknown') },
                            ]}
                            cb={setRideableTypes}
                        />
                    </details>
                </div>
                <hr/>
                <div className={`no-gutters row ${css.row}`}>
                    <div className="col-md-12">
                        <p>Expand the "⚙️" to filter or stack by region, user type, gender, bike type, or date, or toggle aggregation of rides or total ride minutes.</p>
                        <h4>Examples</h4>
                        <ul>
                            <li><Link href={"/?r=jh&s=r"}>JC + Hoboken</Link></li>
                            <li><Link href={"/?y=m&s=g&pct=&g=mf&d=1406-2102"}>Ride minute %'s, Men vs. Women</Link>, Jun 2014 – January 2021</li>
                            <li><Link href={"/?s=u&pct="}>Annual vs. daily user %'s</Link></li>
                            <li><Link href={"/?y=m&s=b&rt=ce"}>Classic / E-bike ride minutes</Link></li>
                            <li><Link href={"/"}>Default view (system-wide rides over time)</Link></li>
                        </ul>
                        <p>This plot should refresh when <a href={"https://www.citibikenyc.com/system-data"} target={"_blank"}>new data is published by Citi Bike</a> (typically around the 2nd week of each month, covering the previous month).</p>
                        <p><a href={"https://github.com/neighbor-ryan/ctbk.dev"} target={"_blank"}>The GitHub repo</a> has more info as well as <a href={"https://github.com/neighbor-ryan/ctbk.dev/issues"} target={"_blank"}>planned enhancements</a>.</p>
                        <hr/>
                        <h3 id={"map"}>Map: Stations + Common Destinations</h3>
                        <p>Check out <Link href={"./stations"}>this map visualization of stations and their ridership counts in {lastMonthDisplayStr}</Link>.</p>
                        <a href={"./stations"}>
                            <img
                                className={css.map}
                                src={"screenshots/ctbk-stations.png"}
                                alt={"Map of stations in Jersey City, sized by ridership, and showing connections to other stations"}
                                // layout={"fill"}
                                // loading={"lazy"}
                            />
                        </a>
                        <hr />
                        <h3 id="qc">🚧 Data-quality issues 🚧</h3>
                        {MD(`
Several things changed in February 2021 (presumably as part of [the Lyft acquisition](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service)):
- "Gender" information is no longer provided:
  - All rides are labeled "unknown" starting February 2021
  - [Here's an example showing the available data](?y=m&s=g&pct=&g=mf&d=1406-2102)
- A new "Rideable Type" field was added, containing values \`docked_bike\` and \`electric_bike\` 🎉; however, [it currently only shows ebike data from June 2021](?y=m&s=b&rt=ce)
- The "User Type" values changed ("Annual" → "member", "Daily" → "casual"); I'm using the former/old values here, they seem equivalent.
                    `)}
                        <div className={css.footer}>
                            Code: { icon(     'gh', 'https://github.com/neighbor-ryan/ctbk.dev#readme',    'GitHub logo') }
                            Data: { icon(     's3',         'https://s3.amazonaws.com/ctbk/index.html', 'Amazon S3 logo') }
                            Author: { icon('twitter',                  'https://twitter.com/RunsAsCoded',   'Twitter logo') }
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}
