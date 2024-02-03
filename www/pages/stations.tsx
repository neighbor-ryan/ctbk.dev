import Head from "../src/head"
import Map from '../src/components/Map';

import {floatParam, LL, llParam, Param, ParsedParam, parseQueryParams, stringParam} from "@rdub/next-params/params";
import {getSync, loadSync} from "@rdub/base/load"
import * as ReactLeaflet from "react-leaflet";
import type L from 'leaflet';
import {Dispatch, useMemo, useState} from 'react';

import css from './stations.module.css'
import fetch from "node-fetch";
import _ from "lodash";
import {LAST_MONTH_PATH} from "../src/paths";

const DEFAULT_CENTER = { lat: 40.758, lng: -73.965, }
const DEFAULT_ZOOM = 12

const { entries, fromEntries, keys } = Object
const { sqrt } = Math

type CountRow = {
    id: string
    count: number
}

type StationValue = {
    name: string
    lat: number
    lng: number
    starts: number
}

type Idx = string
type ID = string
type Stations = { [id: ID]: StationValue }

type YmProps = {
    ym: string
    stations: Stations
}

export async function getStaticProps(context: any) {
    const ym = loadSync<string>(LAST_MONTH_PATH)
    const stationsUrl = `https://ctbk.s3.amazonaws.com/aggregated/${ym}/stations.json`
    const stations = await getSync<Stations>(stationsUrl)
    const defaults: YmProps = { ym, stations }
    return { props: { defaults } }
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
    { setLL, setZoom, stations, selectedStationId, setSelectedStationId, stationPairCounts, url, attribution }: {
        setLL: Dispatch<LL>
        setZoom: Dispatch<number>
        stations: Stations
        selectedStationId: string | undefined
        setSelectedStationId: Dispatch<string | undefined>
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
        click: () => { setSelectedStationId(undefined) },
    })

    const selectedStation = useMemo(
        () => selectedStationId ? stations[selectedStationId] : undefined,
        [ stations, selectedStationId ]
    )

    const lines = useMemo(
        () => {
            if (!selectedStation || !selectedStationId || !stationPairCounts) return null
            if (!(selectedStationId in stationPairCounts)) {
                console.log(`${selectedStationId} not found among ${keys(stationPairCounts).length} stations`)
                return null
            }
            const selectedPairCounts = stationPairCounts[selectedStationId]
            // console.log("selectedPairCounts:", selectedPairCounts)
            const selectedPairValues = Array.from(Object.values(selectedPairCounts))
            // console.log("selectedPairValues:", selectedPairValues)
            const maxDst = Math.max(...selectedPairValues)
            const src = selectedStation
            return <Pane name={"lines"} className={css.lines}>{
                entries(selectedPairCounts).map(([ id, count ]) => {
                    // if (Count != maxDst) return
                    if (!(id in stations)) {
                        console.log(`id ${id} not in stations:`, stations)
                        return
                    }
                    const {name, lat, lng} = stations[id]
                    return <Polyline key={`${selectedStationId}-${id}-${zoom}`} color={"red"}
                                     positions={[[src.lat, src.lng], [lat, lng],]}
                                     weight={Math.max(0.7, count / maxDst * sqrt(src.starts) / mPerPx)} opacity={0.7}>
                        <Tooltip sticky={true}>
                            {src.name} → {name}: {count}
                        </Tooltip>
                    </Polyline>
                })
            }
            </Pane>
        },
        [ stationPairCounts, selectedStationId, mPerPx ]
    )

    function StationCircle({ id, count, selected }: CountRow & { selected?: boolean }) {
        if (!(id in stations)) {
            console.log(`id ${id} not in stations`)
            return null
        }
        const { name, lat, lng } = stations[id]
        return <Circle key={id} center={{ lat, lng }} color={selected ? "yellow" : "orange"} radius={sqrt(count)}
                       eventHandlers={{
                           click: () => {
                               if (id == selectedStationId) return
                               console.log("click:", name)
                               setSelectedStationId(id)
                           },
                           mouseover: () => {
                               if (id == selectedStationId) return
                               console.log("over:", name)
                               setSelectedStationId(id)
                           },
                           mousedown: () => {
                               if (id == selectedStationId) return
                               console.log("down:", name)
                               setSelectedStationId(id)
                           },
                       }}
        >
            <Tooltip className={css.tooltip} sticky={true} permanent={id == selectedStationId} pane={"selected"}>
                <p>{name}: {count}</p>
            </Tooltip>
        </Circle>
    }

    return <>
        <Pane name={"selected"} className={css.selected}>{
            selectedStationId && selectedStation &&
            <StationCircle key={selectedStationId} id={selectedStationId} count={selectedStation.starts} selected={true} />
        }</Pane>
        {lines}
        <Pane name={"circles"} className={css.circles}>{
            entries(stations).map(([ id, station ]) => <StationCircle key={id} id={id} count={station.starts} />)
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

export default function Home({ defaults }: { defaults: YmProps, }) {
    const params: Params = {
        ll: llParam({ init: DEFAULT_CENTER, places: 3, }),
        z: floatParam(DEFAULT_ZOOM, false),
        ss: stringParam(),
        ym: ymParam(defaults.ym),
    }
    const {
        ll: [ { lat, lng }, setLL ],
        z: [ zoom, setZoom, ],
        ss: [ selectedStationId, setSelectedStationId ],
        ym: [ ym, setYM ],
    }: ParsedParams = parseQueryParams({ params })

    const [ stations, setStations ] = useState(defaults.stations)
    const [ stationPairCounts, setStationPairCounts ] = useState<StationPairCounts | null>(null)

    const { ymString } = useMemo(
        () => {
            const year = parseInt(ym.substring(0, 4))
            const month = parseInt(ym.substring(4))
            const ymString = new Date(year, month - 1).toLocaleDateString('default', { month: 'short', year: 'numeric' })
            const dir = `https://ctbk.s3.amazonaws.com/aggregated/${ym}`
            const stationPairsUrl = `${dir}/se_c.json`
            const stationsUrl = `${dir}/stations.json`

            let stationsPromise: Promise<Stations>
            if (ym == defaults.ym) {
                setStations(defaults.stations)
                console.log("Setting stations to default")
                stationsPromise = Promise.resolve(defaults.stations)
            } else {
                console.log(`fetching ${stationsUrl}`)
                stationsPromise = fetch(stationsUrl)
                    .then<Stations>(data => data.json())
                    .then(stations => {
                        console.log(`got ${entries(stations).length} stations`)
                        setStations(stations)
                        return stations
                    })
            }
            console.log(`fetching ${stationPairsUrl}`)
            Promise.all([
                stationsPromise,
                fetch(stationPairsUrl).then<StationPairCounts>(data => data.json())
            ]).then(([ newStations, data]) => {
                const idx2id: { [idx: Idx]: ID } = fromEntries(keys(newStations).map((id, idx) => [ idx, id ]))
                const newStationPairCounts =
                    fromEntries(
                        entries(data)
                            .map(([k1, v1]) => [
                                idx2id[k1],
                                _.mapKeys(v1, (v2, k2) => idx2id[k2])
                            ])
                    )
                console.log(`got stationPairCounts (${entries(newStationPairCounts).length} stations)`)
                setStationPairCounts(newStationPairCounts)
            })

            return { ymString }
        },
        [ ym ]
    )

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
                        MapBody(RL, { setLL, setZoom, stations, selectedStationId, setSelectedStationId, stationPairCounts, url, attribution, })
                    }</div>
                }</Map>
                {title && <div className={css.title}>{title}</div>}
            </main>
        </div>
    )
}
