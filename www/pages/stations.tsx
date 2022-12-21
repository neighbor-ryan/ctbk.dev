import Head from 'next/head';

import Map from '../src/components/Map';

import {floatParam, Param, ParsedParam, parseQueryParams, stringParam} from "../src/utils/params";
import {LL, llParam} from "../src/latlng";
import * as ReactLeaflet from "react-leaflet";
import type L from 'leaflet';
import {Dispatch, useCallback, useEffect, useMemo, useState} from 'react';
import * as fs from "fs";
import path from "path";

import css from './stations.module.css'

const DEFAULT_CENTER = { lat: 40.758, lng: -73.965, }
const DEFAULT_ZOOM = 12

const COUNTS_PATH = 'public/assets/s_c_202211.json'
const STATIONS_PATH = 'public/assets/ids.json'
const DB_URL = '/assets/se_c_202211_normd.db'

const { fromEntries } = Object
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

export async function getStaticProps(context: any) {
    const cwd = process.cwd()
    const counts = JSON.parse(fs.readFileSync(path.join(cwd, COUNTS_PATH), 'utf-8')) as CountRow[]
    const stations = JSON.parse(fs.readFileSync(path.join(cwd, STATIONS_PATH), 'utf-8')) as Stations
    return { props: { counts, stations, } }
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
}

type ParsedParams = {
    ll: ParsedParam<LL>
    z: ParsedParam<number>
    ss: ParsedParam<string | undefined>
}

function getMetersPerPixel(map: L.Map) {
    const centerLatLng = map.getCenter(); // get map center
    const pointC = map.latLngToContainerPoint(centerLatLng); // convert to containerpoint (pixels)
    const pointX: L.PointExpression = [pointC.x + 1, pointC.y]; // add one pixel to x
    const pointY: L.PointExpression = [pointC.x, pointC.y + 1]; // add one pixel to y

    // convert containerpoints to latlng's
    const latLngC = map.containerPointToLatLng(pointC);
    const latLngX = map.containerPointToLatLng(pointX);
    const latLngY = map.containerPointToLatLng(pointY);

    const distanceX = latLngC.distanceTo(latLngX); // calculate distance between c and x (latitude)
    const distanceY = latLngC.distanceTo(latLngY); // calculate distance between c and y (longitude)

    const zoom = map.getZoom()
    //console.log("distanceX:", distanceX, "distanceY:", distanceY, "center:", centerLatLng, "zoom:", zoom)
    return distanceX
}

function MapBody(
    {TileLayer, Marker, Circle, CircleMarker, Polyline, Pane, Tooltip, useMapEvents, useMap }: typeof ReactLeaflet,
    { setLL, setZoom, counts, stations, selectedStation, setSelectedStation, stationCounts, setStationCounts, url, attribution }: {
        setLL: Dispatch<LL>
        setZoom: Dispatch<number>
        counts: CountRow[]
        stations: Stations
        selectedStation: string | undefined
        setSelectedStation: Dispatch<string | undefined>
        stationCounts: StationCount[] | null
        setStationCounts: Dispatch<StationCount[] | null>
        url: string
        attribution: string
    }
) {
    // Error: No context provided: useLeafletContext() can only be used in a descendant of <MapContainer>
    const map = useMap()
    const zoom = map.getZoom()
    const mPerPx = useMemo(() => {
        let mPerPx = getMetersPerPixel(map)
        // console.log("mPerPx:", mPerPx)
        return mPerPx
    }, [ map, zoom, ])
    useMapEvents({
        moveend: () => setLL(map.getCenter()),
        zoom: () => setZoom(map.getZoom()),
        click: () => { setSelectedStation(undefined) ; setStationCounts(null) },
    })

    const maxDst = Math.max(...(stationCounts || []).map(({ Count }) => Count))
    const sum = (stationCounts || []).reduce((a, b) => a + b.Count, 0)
    // const weight = sqrt(sum) / mPerPx
    // console.log("counts:", counts)
    //console.log("maxDst:", maxDst, "sum:", sum, "counts:", counts.filter(({ id }) => id == selectedStation)[0], "selected:", selectedStation, "num counts:", (stationCounts || []).length)

    const lines = useMemo(
        () => {
            if (!selectedStation) return null
            const src = stations[selectedStation]
            //console.log("computing lines:", selectedStation)
            return <Pane name={"lines"} className={css.lines}>{
                (stationCounts || []).map(({ID, Count}) => {
                    // if (Count != maxDst) return
                    const {name, lat, lng} = stations[ID]
                    return <Polyline key={`${selectedStation}-${ID}-${zoom}`} color={"red"}
                                     positions={[[src.lat, src.lng], [lat, lng],]}
                                     weight={Math.max(0.7, Count / maxDst * sqrt(sum) / mPerPx)} opacity={0.7}>
                        <Tooltip sticky={true}>
                            {src.name} → {name}: {Count}
                        </Tooltip>
                    </Polyline>
                })
            }
            </Pane>
        },
        [ selectedStation, stationCounts, maxDst, sum, mPerPx ]
    )

    const selectedCounts = counts.filter(count => count.id == selectedStation)

    function StationCircle({ id, count, selected }: CountRow & { selected?: boolean }) {
        const { name, lat, lng } = stations[id]
        return <Circle key={id} center={{ lat, lng }} color={selected ? "yellow" : "orange"} radius={sqrt(count)}
                       eventHandlers={{
                           click: () => {
                               if (id == selectedStation) return
                               console.log("click:", name)
                               setStationCounts(null)
                               setSelectedStation(id)
                           },
                           mouseover: () => {
                               if (id == selectedStation) return
                               console.log("over:", name)
                               setStationCounts(null)
                               setSelectedStation(id)
                           },
                           mousedown: () => {
                               if (id == selectedStation) return
                               console.log("click:", name)
                               setStationCounts(null)
                               setSelectedStation(id)
                           },
                           // mouseout: () => {
                           //     console.log("out:", name)
                           //     setStationCounts(null)
                           //     setSelectedStation(null)
                           // }
                       }}
        >
            <Tooltip sticky={true}>
                {name}: {count}
            </Tooltip>
        </Circle>
    }

    return <>
        <Pane name={"selected"} className={css.selected}>{
            selectedCounts.map(({ id, count}) => <StationCircle key={id} id={id} count={count} selected={true} />)
        }</Pane>
        {lines}
        <Pane name={"circles"} className={css.circles}>{
            counts.map(({ id, count }) => <StationCircle key={id} id={id} count={count} />)
        }</Pane>
        <TileLayer url={url} attribution={attribution}/>
    </>
}

type StationCount = { ID: string, Count: number }

export default function Home({ counts, stations, }: { counts: CountRow[], stations: Stations }) {
    const params: Params = {
        ll: llParam({ init: DEFAULT_CENTER, places: 3, }),
        z: floatParam(DEFAULT_ZOOM, false),
        ss: stringParam(),
    }
    const {
        ll: [ { lat, lng }, setLL ],
        z: [ zoom, setZoom, ],
        ss: [ selectedStation, setSelectedStation ],
    }: ParsedParams = parseQueryParams({ params })

    const title = "Citi Bike rides by station, November 2022"

    const { url, attribution } = MAPS['alidade_smooth_dark']

    const isSSR = typeof window === "undefined";

    const [ stationCounts, setStationCounts ] = useState<StationCount[] | null>(null)

    const query = useMemo(
        () => `
            select ID, Count from (
                select
                    "End Station Idx",
                    Count
                from
                    counts
                        join stations on counts."Start Station Idx" = stations.idx
                where
                    stations.ID = "${selectedStation}") a join stations on a."End Station Idx"=stations.idx
        `,
        [ selectedStation ]
    )

    const worker = useMemo(
        () => {
            if (isSSR) {
                console.log("SSR, aborting effect")
                return
            }
            const WorkerModulePromise = import("../src/worker")

            // console.log("Worker promise:", WorkerModulePromise,)
            return WorkerModulePromise.then(WorkerModule => {
                const {Worker} = WorkerModule
                // console.log("WorkerModule:", WorkerModule, "Worker:", Worker)
                return new Worker({url: DB_URL,})
            })
        },
        [ DB_URL ]
    )

    useEffect(
        () => {
            console.log("fetching…")
            worker
                ?.then(w => w.fetchQuery<StationCount>(query))
                ?.then(data => {
                    console.log(`loaded ${selectedStation} counts:`, data)
                    setStationCounts(data)
                })
        },
        [ worker, query, ],
    )

    return (
        <div className={css.container}>
            <Head>
                <title>{title}</title>
                {/*<link rel="icon" href={`${basePath}/favicon.ico`} />*/}

                <meta name="twitter:card" content="summary" key="twcard" />
                <meta name="twitter:creator" content={"RunsAsCoded"} key="twhandle" />

                <meta property="og:url" content="https://ctbk.dev/stations" key="ogurl" />
                <meta property="og:type" content="website" />
                <meta property="og:image" content="https://ctbk.dev/stations-screenshot.png" key="ogimage" />
                <meta property="og:site_name" content="ctbk.dev" key="ogsitename" />
                <meta property="og:title" content="Citi Bike Station Ridership Map" key="ogtitle" />
                <meta property="og:description" content={"Map of Citi Bike stations' ridership in August 2022"} key="ogdesc" />
            </Head>

            <main className={css.main}>{
                (typeof window !== undefined) && <>
                    <Map className={css.homeMap} center={{ lat, lng, }} zoom={zoom} zoomControl={false} zoomSnap={0.5} zoomDelta={0.5}>{
                        (RL: typeof ReactLeaflet) => <div>{
                            MapBody(RL, { setLL, setZoom, counts, stations, selectedStation, setSelectedStation, stationCounts, setStationCounts, url, attribution, })
                        }</div>
                    }</Map>
                    {title && <div className={css.title}>{title}</div>}
                </>
            }</main>
        </div>
    )
}
