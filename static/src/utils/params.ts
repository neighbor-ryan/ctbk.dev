import {useRouter} from "next/router";
import {useEffect, useState} from "react";
import _ from "lodash";

export const pathnameRegex = /[^?#]+/u;

export type Param<T> = {
    encode: (t: T) => string | undefined
    decode: (v: string | undefined) => T
    // use?: (init: Set<T>) => [Set<T>, SetSet<T>]
}

export function floatParam(init: number): Param<number> {
    return {
        encode: v => v === init ? undefined : v.toString(),
        decode: v => v ? parseFloat(v) : init
    }
}

const { entries, fromEntries, keys, } = Object

export function enumMultiParam<T extends string>(
    init: T[],
    mapper: { [k in T]: string } | [ T, string ][],
    delim?: string,
): Param<T[]> {
    const delimiter: string = delim === undefined ? '_' : delim
    const t2s: { [k in T]: string } = (mapper instanceof Array) ? fromEntries(mapper) as { [k in T]: string } : mapper
    const s2t: { [key: string]: T } = fromEntries(entries(t2s).map(([ k, v, ]) => [ v, k, ]))

    function verify(values: string[]): T[] {
        return Array.from(values).filter(
            (v): v is T => {
                if (v in t2s) {
                    return true
                } else {
                    console.warn(`Invalid value: ${v} not in ${keys(t2s).join(", ")}`)
                    return false
                }
            }
        )
    }

    const encode = (values: T[]) => {
        return verify(values).map(v => t2s[v]).join(delimiter)
    }

    const encodedInit = encode(init)

    return {
        encode: values => {
            const enc = encode(values)
            if (enc === encodedInit) return undefined
            return enc
        },
        decode: s => {
            if (!s && s !== '') {
                return init
            }
            let values = s.split(delimiter).filter(v => {
                    if (v in s2t) {
                        return true
                    } else {
                        console.warn(`Unrecognized value: ${v} not in ${keys(s2t).join(",")}`)
                        return false
                    }
                }).map(v => s2t[v])
            values = verify(values)
            return values
        },
    }
}

export function enumParam<T extends string>(
    init: T,
    mapper: { [k in T]: string } | [ T, string ][]
): Param<T> {
    const t2s: { [k in T]: string } = (mapper instanceof Array) ? fromEntries(mapper) as { [k in T]: string } : mapper
    const s2t: { [key: string]: T } = fromEntries(entries(t2s).map(([ k, v, ]) => [ v, k, ]))
    return {
        encode(t: T): string | undefined {
            if (t == init) return undefined
            return t2s[t];
        },
        decode(v: string | undefined): T {
            if (v === undefined) return init
            if (!(v in s2t)) {
                console.warn(`Invalid enum: ${v} not in ${keys(s2t).join(",")}`)
                return init
            }
            return s2t[v]
        },
    }
}

export const boolParam: Param<boolean> = {
    encode(t: boolean): string | undefined {
        return t ? '' : undefined;
    },
    decode(v: string | undefined): boolean {
        return v !== undefined;
    },
}

export function numberArrayParam(
    defaultValue: number[] = [],
): Param<number[]> {
    const eq = _.isEqual
    return {
        encode(value: number[]): string | undefined {
            if (eq(value, defaultValue)) return undefined
            return value.map(v => v.toString()).join(',')
        },
        decode(value: string | undefined): number[] {
            if (value === undefined) return defaultValue
            return value.split(',').map(parseInt)
        },
    }
}

export function parseQueryParams<Params extends { [k: string]: Param<any> }, ParsedParams>({ params }: { params: Params }): ParsedParams {
    const router = useRouter()
    const path = router.asPath
    const searchStr = path.replace(pathnameRegex, '')
    const searchObj = Object.fromEntries(new URLSearchParams(searchStr).entries())
    const state = Object.fromEntries(
        Object.entries(params).map(([ k, param ]) => {
            const [ val, set ] = useState(param.decode(searchObj[k]))
            return [ k, { val, set, param } ]
        })
    )
    const stateValues = Object.values(state).map(({ val }) => val)
    const setters = Object.values(state).map(({ set }) => set)

    useEffect(
        () => {
            window.onpopstate = e => {
                const newUrl = e.state.url
                const newSearchStr = newUrl.replace(pathnameRegex, '')
                // console.log("onpopstate:", e, "newUrl:", newUrl, "newSearchStr:", newSearchStr, "oldSearchStr:", searchStr)
                const newSearchObj = Object.fromEntries(new URLSearchParams(newSearchStr).entries())
                Object.entries(params).forEach(([ k, param ]) => {
                    const val = param.decode(newSearchObj[k])
                    const { set } = state[k]
                    // console.log(`back! setting: ${k}, ${set}, ${val}`)
                    set(val)
                })
            };
        },
        setters
    );

    const match = path.match(pathnameRegex);
    const pathname = match ? match[0] : path;

    const query: {[k: string]: string} = {}
    Object.entries(state).map(([ k, { val, param, } ]) => {
        const s = param.encode(val)
        if (s !== undefined) {
            query[k] = s
        }
    })
    const search = new URLSearchParams(query).toString()
    // console.log(`path: ${path}, searchStr: ${searchStr}, searchObj: `, searchObj, `, search: ${search}, query:`, query)

    useEffect(
        () => {
            const hash = ''
            router.push(
                { pathname: router.pathname, hash, search},
                { pathname, hash, search, },
                { shallow: true, scroll: false, }
            )
        },
        [ ...stateValues, pathname, search, ]
    )

    return fromEntries(
        entries(state)
            .map(([ k, { val, set, }]) => [ k, [ val, set, ] ])
    ) as any as ParsedParams
}
