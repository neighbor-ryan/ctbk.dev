import {ReactElement, ReactNode, useEffect} from 'react';
import L from 'leaflet';
import * as ReactLeaflet from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

import styles from './Map.module.css';

import {MapContainerProps} from "react-leaflet/lib/MapContainer";

type MapProps = (Omit<MapContainerProps, "children"> & { children: (RL: typeof ReactLeaflet, map: L.Map) => ReactElement<{}> })

const _hooks = require("react-leaflet/hooks");

function MapConsumer(_ref: { children: (map: L.Map) => ReactElement<{}> }) {
  let { children } = _ref;
  return children(_hooks.useMap());
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
