import Head from 'next/head';

import Map from '../src/components/Map';

import styles from '../styles/Home.module.css';

import {floatParam, Param, ParsedParam, parseQueryParams} from "../src/utils/params";
import {LL, llParam} from "../src/latlng";
import * as ReactLeaflet from "react-leaflet";
import {Dispatch, useEffect, useState} from 'react';

const DEFAULT_CENTER = { lat: 40.750, lng: -73.944, }
const DEFAULT_ZOOM = 12.5

const COUNTS_URL = 'https://ctbk.s3.amazonaws.com/aggregated/s_c_202208.json'
const STATIONS_URL = 'https://ctbk.s3.amazonaws.com/stations/ids.json'

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
    let res = await fetch(COUNTS_URL)
    const counts = await res.json() as any as CountRow[]
    res = await fetch(STATIONS_URL)
    const stations = await res.json() as any as Stations
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
}

type ParsedParams = {
    ll: ParsedParam<LL>
    z: ParsedParam<number>
}

function MapBody(
    {TileLayer, Marker, Circle, CircleMarker, Tooltip, useMapEvents, useMap, }: typeof ReactLeaflet,
    { setLL, setZoom, counts, stations, url, attribution }: {
        setLL: Dispatch<LL>
        setZoom: Dispatch<number>
        counts: CountRow[]
        stations: Stations
        url: string
        attribution: string
    }
) {
    // Error: No context provided: useLeafletContext() can only be used in a descendant of <MapContainer>
/*
    const map = useMap()
    useMapEvents({
        move: () => setLL(map.getCenter()),
        zoom: () => setZoom(map.getZoom()),
    })
*/

    return <>
        {counts.map(({ id, count }) => {
            const { name, lat, lng } = stations[id]
            return <Circle key={id} center={{ lat, lng }} color={"orange"} radius={sqrt(count)}>
                <Tooltip sticky={true}>
                    {name} ({id}): {count}
                </Tooltip>

            </Circle>
        })}
        <TileLayer url={url} attribution={attribution}/>
    </>
}

export default function Home({ counts, stations, }: { counts: CountRow[], stations: Stations }) {
    const params: Params = {
        ll: llParam(DEFAULT_CENTER, 3),
        z: floatParam(DEFAULT_ZOOM),
    }
    const {
        ll: [ { lat, lng }, setLL ],
        z: [ zoom, setZoom, ],
    }: ParsedParams = parseQueryParams({ params })

    // const [ ready, setReady ] = useState(false)
    // console.log("ready:", ready)

    const title = "Citi Bike rides by station, August 2022"

    const { url, attribution } = MAPS['alidade_smooth_dark']

    return (
        <div className={styles.container}>
            <Head>
                <title>{title}</title>
                {/*<link rel="icon" href={`${basePath}/favicon.ico`} />*/}

                <meta name="twitter:card" content="summary" key="twcard" />
                <meta name="twitter:creator" content={"RunsAsCoded"} key="twhandle" />

                <meta property="og:url" content="https://bikejc.github.io/maps" key="ogurl" />
                <meta property="og:type" content="website" />
                <meta property="og:image" content="https://bikejc.github.io/maps/1-pbls.png" key="ogimage" />
                <meta property="og:site_name" content="JC Bike Lane Map" key="ogsitename" />
                <meta property="og:title" content="JC Bike Lane + Ward Map" key="ogtitle" />
                <meta property="og:description" content={"≈10 protected bike lanes overlaid on the 6 council wards"} key="ogdesc" />
            </Head>

            <main className={styles.main}>{
                (typeof window !== undefined) && <>
                    <Map className={styles.homeMap} center={{ lat, lng, }} zoom={zoom} zoomControl={false} zoomSnap={0.5} zoomDelta={0.5} /*whenReady={() => { console.log("setReady"); setReady(true) }*/>
                        {
                            (RL: typeof ReactLeaflet) => <div>{
                                MapBody(RL, { setLL, setZoom, counts, stations, url, attribution, })
                            }</div>
                        }
                    </Map>
                    {title && <div className={styles.title}>{title}</div>}
                </>
            }</main>
        </div>
    )
}
