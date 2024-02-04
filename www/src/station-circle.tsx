import { Dispatch } from "react";
import { Circle, Tooltip } from "react-leaflet";
import css from "../pages/stations.module.css";
import { CountRow, Stations } from "./stations";
const { sqrt } = Math

export type Props = CountRow & {
    stations: Stations
    selectedStationId: string | undefined
    setSelectedStationId: Dispatch<string | undefined>
    selected?: boolean
}

export default function StationCircle(
    {
        id,
        count,
        selected,
        selectedStationId, setSelectedStationId,
        stations,
    }: Props
) {
    if (!(id in stations)) {
        console.log(`id ${id} not in stations`)
        return null
    }
    const { name, lat, lng } = stations[id]
    return <Circle
        key={id}
        center={{lat, lng}}
        color={selected ? "yellow" : "orange"}
        radius={sqrt(count)}
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
