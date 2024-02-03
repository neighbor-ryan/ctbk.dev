import {ReactElement} from 'react';
import L from 'leaflet';
import * as ReactLeaflet from 'react-leaflet';

import styles from './Map.module.css';

import {MapContainerProps} from "react-leaflet/lib/MapContainer";

type MapProps = (Omit<MapContainerProps, "children"> & { children: (RL: typeof ReactLeaflet, map: L.Map) => ReactElement<{}> })

import { useMap } from "react-leaflet"

function MapConsumer(_ref: { children: (map: L.Map) => ReactElement<{}> }) {
    let { children } = _ref;
    return children(useMap());
}

const Map = ({ children, className, ...rest }: MapProps) => {
    let mapClassName = styles.map;

    if ( className ) {
        mapClassName = `${mapClassName} ${className}`;
    }

    const MapContainer = ReactLeaflet.MapContainer
    return (
        <MapContainer className={mapClassName} {...rest}>
            <MapConsumer>{
                (map: L.Map) => children(ReactLeaflet, map)
            }</MapConsumer>
        </MapContainer>
    )
}

export default Map;
