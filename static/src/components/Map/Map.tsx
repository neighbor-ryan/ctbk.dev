import {ReactNode, useEffect} from 'react';
import * as ReactLeaflet from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

import styles from './Map.module.css';

// import { MapContainer, TileLayer, useMap } from 'react-leaflet'

import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png';
import iconUrl from 'leaflet/dist/images/marker-icon.png';
import shadowUrl from 'leaflet/dist/images/marker-shadow.png';
import {MapContainerProps} from "react-leaflet/lib/MapContainer";

type MapProps = (Omit<MapContainerProps, "children"> & { children: (RL: typeof ReactLeaflet) => ReactNode })

const Map = ({ children, className, ...rest }: MapProps) => {
  let mapClassName = styles.map;

  if ( className ) {
    mapClassName = `${mapClassName} ${className}`;
  }

/*
  useEffect(() => {
    (async function init() {
      delete L.Icon.Default.prototype._getIconUrl;

      L.Icon.Default.mergeOptions({
        iconRetinaUrl: iconRetinaUrl.src,
        iconUrl: iconUrl.src,
        shadowUrl: shadowUrl.src,
      });
    })();
  }, []);
*/
  const MapContainer = ReactLeaflet.MapContainer
  return (
    <MapContainer className={mapClassName} {...rest}>
      {children(ReactLeaflet)}
    </MapContainer>
  )
}

export default Map;
