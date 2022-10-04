import {Param} from "./utils/params";

export type LL = { lat: number, lng: number }

export function llParam(init: LL, places?: number): Param<LL> {
    return {
        encode: ({ lat, lng }) =>
            (lat === init.lat && lng === init.lng)
                ? undefined
                : (
                    places
                        ? `${lat.toFixed(places)}_${lng.toFixed(places)}`
                        : `${lat}_${lng}`
                ),
        decode: v => {
            if (!v) return init
            const [ lat, lng ] = v.split("_").map(parseFloat)
            return { lat, lng }
        },
    }
}
