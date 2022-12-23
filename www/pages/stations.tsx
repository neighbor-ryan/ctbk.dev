import Head from "../src/utils/head"

import Map from '../src/components/Map';

import {floatParam, Param, ParsedParam, parseQueryParams, stringParam, LL, llParam} from "next-utils/params";
import {loadSync, getSync} from "next-utils/load"
import * as ReactLeaflet from "react-leaflet";
import type L from 'leaflet';
import {Dispatch, useMemo, useState} from 'react';

import css from './stations.module.css'
import fetch from "node-fetch";
import _ from "lodash";
import {LAST_MONTH_PATH, DOMAIN, SCREENSHOTS} from "../src/utils/paths";

const DEFAULT_CENTER = { lat: 40.758, lng: -73.965, }
const DEFAULT_ZOOM = 12

const { entries, fromEntries } = Object
const { sqrt } = Math

type CountRow = {
    id: string
    count: number
}

type StationValue = {
    name: string
    lat: number
    lng: number
}

type Stations = { [id: string]: StationValue }

type Idx = string
type ID = string
type Idx2Id = { [k: Idx]: ID }
type StationCounts = { [k: ID]: number }

type YmProps = {
    ym: string
    stationCounts: StationCounts
    idx2id: Idx2Id
}

type YmState = {
    ym: string
    stationCounts: Promise<StationCounts>
    idx2id: Promise<Idx2Id>
    stationPairCounts: Promise<StationPairCounts>
}

export async function getStaticProps(context: any) {
    const ym = loadSync<string>(LAST_MONTH_PATH)

    const idx2idUrl = `https://ctbk.s3.amazonaws.com/aggregated/${ym}/idx2id.json`
    const stationCountsUrl = `https://ctbk.s3.amazonaws.com/aggregated/${ym}/s_c.json`
    const stationsUrl = `https://ctbk.s3.amazonaws.com/stations/ids.json`

    const idx2id = await getSync<Idx2Id>(idx2idUrl)
    const stationCounts: StationCounts = _.mapKeys(await getSync<StationCounts>(stationCountsUrl), (v, k) => idx2id[k])
    const defaults: YmProps = { ym, idx2id, stationCounts }

    const stations = await getSync<Stations>(stationsUrl)
    // const stations = load<Stations>(STATIONS_PATH)
    return { props: { defaults, stations, } }
}

export const MAPS = {
    openstreetmap: {
        url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        attribution: "&copy; <a href=&quot;http://osm.org/copyright&quot;>OpenStreetMap</a> contributors",
    },
    alidade_smooth_dark: {
        url: 'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}{r}.png',
        attribution: '&copy; <a href="https://stadiamaps.com/">Stadia Maps</a>, &copy; <a href="https://openmaptiles.org/">OpenMapTiles</a> &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors',
    },
}

type Params = {
    ll: Param<LL>
    z: Param<number>
    ss: Param<string | undefined>
    ym: Param<string>
}

type ParsedParams = {
    ll: ParsedParam<LL>
    z: ParsedParam<number>
    ss: ParsedParam<string | undefined>
    ym: ParsedParam<string>
}

function getMetersPerPixel(map: L.Map) {
    const centerLatLng = map.getCenter(); // get map center
    const pointC = map.latLngToContainerPoint(centerLatLng); // convert to containerpoint (pixels)
    const pointX: L.PointExpression = [pointC.x + 1, pointC.y]; // add one pixel to x
    // const pointY: L.PointExpression = [pointC.x, pointC.y + 1]; // add one pixel to y

    // convert containerpoints to latlng's
    const latLngC = map.containerPointToLatLng(pointC);
    const latLngX = map.containerPointToLatLng(pointX);
    // const latLngY = map.containerPointToLatLng(pointY);

    const distanceX = latLngC.distanceTo(latLngX); // calculate distance between c and x (latitude)
    // const distanceY = latLngC.distanceTo(latLngY); // calculate distance between c and y (longitude)

    // const zoom = map.getZoom()
    //console.log("distanceX:", distanceX, "distanceY:", distanceY, "center:", centerLatLng, "zoom:", zoom)
    return distanceX
}

function MapBody(
    {TileLayer, Marker, Circle, CircleMarker, Polyline, Pane, Tooltip, useMapEvents, useMap }: typeof ReactLeaflet,
    { setLL, setZoom, stations, stationCounts, selectedStation, setSelectedStation, stationPairCounts, url, attribution }: {
        setLL: Dispatch<LL>
        setZoom: Dispatch<number>
        stations: Stations
        stationCounts: StationCounts
        selectedStation: string | undefined
        setSelectedStation: Dispatch<string | undefined>
        stationPairCounts: StationPairCounts | null
        url: string
        attribution: string
    }
) {
    // Error: No context provided: useLeafletContext() can only be used in a descendant of <MapContainer>
    const map = useMap()
    const zoom = map.getZoom()
    const mPerPx = useMemo(() => getMetersPerPixel(map), [ map, zoom, ])
    useMapEvents({
        moveend: () => setLL(map.getCenter()),
        zoom: () => setZoom(map.getZoom()),
        click: () => { setSelectedStation(undefined) },
    })

    const lines = useMemo(
        () => {
            if (!selectedStation || !stationPairCounts) return null
            const selectedPairCounts = stationPairCounts[selectedStation]
            // console.log("selectedPairCounts:", selectedPairCounts)
            const selectedPairValues = Array.from(Object.values(selectedPairCounts))
            // console.log("selectedPairValues:", selectedPairValues)
            const maxDst = Math.max(...selectedPairValues)
            const sum = stationCounts[selectedStation]
            const src = stations[selectedStation]
            //console.log("computing lines:", selectedStation)
            return <Pane name={"lines"} className={css.lines}>{
                entries(selectedPairCounts).map(([ id, count ]) => {
                    // if (Count != maxDst) return
                    if (!(id in stations)) {
                        console.log(`id ${id} not in stations:`, stations)
                        return
                    }
                    const {name, lat, lng} = stations[id]
                    return <Polyline key={`${selectedStation}-${id}-${zoom}`} color={"red"}
                                     positions={[[src.lat, src.lng], [lat, lng],]}
                                     weight={Math.max(0.7, count / maxDst * sqrt(sum) / mPerPx)} opacity={0.7}>
                        <Tooltip sticky={true}>
                            {src.name} → {name}: {count}
                        </Tooltip>
                    </Polyline>
                })
            }
            </Pane>
        },
        [ stationPairCounts, selectedStation, stationCounts, mPerPx ]
    )

    const selectedCount: [ ID, number ] | undefined = selectedStation ? Array.from(entries(stationCounts).filter(([ id, count, ]) => id == selectedStation))[0] : undefined

    function StationCircle({ id, count, selected }: CountRow & { selected?: boolean }) {
        const { name, lat, lng } = stations[id]
        return <Circle key={id} center={{ lat, lng }} color={selected ? "yellow" : "orange"} radius={sqrt(count)}
                       eventHandlers={{
                           click: () => {
                               if (id == selectedStation) return
                               console.log("click:", name)
                               setSelectedStation(id)
                           },
                           mouseover: () => {
                               if (id == selectedStation) return
                               console.log("over:", name)
                               setSelectedStation(id)
                           },
                           mousedown: () => {
                               if (id == selectedStation) return
                               console.log("down:", name)
                               setSelectedStation(id)
                           },
                       }}
        >
            <Tooltip sticky={true}>
                {name}: {count}
            </Tooltip>
        </Circle>
    }

    return <>
        <Pane name={"selected"} className={css.selected}>{
            selectedCount && [selectedCount].map(([ id, count ]) => <StationCircle key={id} id={id} count={count} selected={true} />)
        }</Pane>
        {lines}
        <Pane name={"circles"} className={css.circles}>{
            entries(stationCounts).map(([ id, count ]) => <StationCircle key={id} id={id} count={count} />)
        }</Pane>
        <TileLayer url={url} attribution={attribution}/>
    </>
}

export function ymParam(init: string, push: boolean = true): Param<string> {
    const ymRegex = /^20(\d{4})$/
    const vRegex = /^\d{4}$/
    return {
        encode: ym => {
            if (ym == init) return undefined
            const match = ym.match(ymRegex)
            if (match) {
                return match[1]
            } else {
                console.warn(`Invalid ym param: ${ym}`)
                return undefined
            }
        },
        decode: v => {
            if (!v) return init
            const match = v.match(vRegex)
            if (match) {
                return `20${v}`
            } else {
                console.warn(`Invalid ym param value: ${v}`)
                return init
            }
        },
        push,
    }
}

type StationPairCounts = {
    [k1: string]: { [k2: string]: number }
}

export default function Home({ defaults, stations }: { defaults: YmProps, stations: Stations, }) {
    const params: Params = {
        ll: llParam({ init: DEFAULT_CENTER, places: 3, }),
        z: floatParam(DEFAULT_ZOOM, false),
        ss: stringParam(),
        ym: ymParam(defaults.ym),
    }
    const {
        ll: [ { lat, lng }, setLL ],
        z: [ zoom, setZoom, ],
        ss: [ selectedStation, setSelectedStation ],
        ym: [ ym, setYM ],
    }: ParsedParams = parseQueryParams({ params })

    const [ stationCounts, setStationCounts ] = useState(defaults.stationCounts)
    const [ stationPairCounts, setStationPairCounts ] = useState<StationPairCounts | null>(null)

    const { ymString, } = useMemo(() => {
        const year = parseInt(ym.substring(0, 4))
        const month = parseInt(ym.substring(4))
        const ymString = new Date(year, month - 1).toLocaleDateString('default', { month: 'short', year: 'numeric' })
        const dir = `https://ctbk.s3.amazonaws.com/aggregated/${ym}`
        const idx2idUrl = `${dir}/idx2id.json`
        const stationCountsUrl = `${dir}/s_c.json`
        const stationPairsUrl = `${dir}/se_c.json`

        let idx2id: Promise<Idx2Id>
        if (ym == defaults.ym) {
            setStationCounts(defaults.stationCounts)
            idx2id = Promise.resolve(defaults.idx2id)
            console.log("Setting stationCounts to default")
        } else {
            console.log(`fetching ${stationCountsUrl}`)
            idx2id = fetch(idx2idUrl)
                .then<Idx2Id>(data => data.json())
                .then(data => { console.log("got idx2id"); return data })

            fetch(stationCountsUrl)
                .then<StationCounts>(data => data.json())
                .then(stationCounts =>
                    idx2id.then(
                        idx2id => {
                            console.log("got stationCounts")
                            setStationCounts(_.mapKeys(stationCounts, (v, k) => idx2id[k]))
                        }
                    )
                )
        }
        console.log(`fetching ${stationPairsUrl}`)
        fetch(stationPairsUrl)
            .then<StationPairCounts>(data => data.json())
            .then(data =>
                idx2id.then(
                    idx2id => {
                        console.log("got stationPairCounts")
                        setStationPairCounts(
                            fromEntries(
                                entries(data)
                                    .map(([k1, v1]) => [
                                        idx2id[k1],
                                        _.mapKeys(v1, (v2, k2) => idx2id[k2])
                                    ])
                            )
                        )
                    }
                )
            )

        return { ymString, }
    }, [ ym ])

    const title = `Citi Bike rides by station, ${ymString}`

    const { url, attribution } = MAPS['alidade_smooth_dark']

    return (
        <div className={css.container}>
            <Head
                title={title}
                description={"Map of Citi Bike stations, including ridership counts and frequent destinations"}
                path={`stations`}
                thumbnail={`ctbk-stations`}
            />

            <main className={css.main}>
                <Map className={css.homeMap} center={{ lat, lng, }} zoom={zoom} zoomControl={false} zoomSnap={0.5} zoomDelta={0.5}>{
                    (RL: typeof ReactLeaflet) => <div>{
                        MapBody(RL, { setLL, setZoom, stations, stationCounts, selectedStation, setSelectedStation, stationPairCounts, url, attribution, })
                    }</div>
                }</Map>
                {title && <div className={css.title}>{title}</div>}
            </main>
        </div>
    )
}
