const {round} = Math

export type SizeFmt = 'bytes' | 'iec' | 'iso'
type Size = { num: number, suffix?: string }

export const renderHumanReadableSize = function(size: number, iec: boolean = true): Size {
    const orders = [ 'K', 'M', 'G', 'T', 'P', 'E', ]
    const [ suffix, base ]: [ string, number, ] = iec ? ['iB', 1024, ] : ['B', 1000]
    const [ n, o, ] = orders.reduce<[ number, string, ]>(
        ([size, curOrder], nxtOrder) => {
            if (size < base) {
                return [ size, curOrder ]
            } else {
                return [ size / base, nxtOrder]
            }
        },
        [ size, '', ],
    )
    if (!o) {
        return { num: n, suffix: 'B' }
    }
    return { num: n, suffix: `${o}${suffix}` }
}

const sizeSuffixer = {
    'iec': (sz: number): Size => renderHumanReadableSize(sz, true),
    'iso': (sz: number): Size => renderHumanReadableSize(sz, false),
    'bytes': (sz: number): Size => { return { num: sz } }
}

export const computeSize = (size: number, fmt: SizeFmt): Size => sizeSuffixer[fmt](size)

export const renderSize = (size: number, fmt: SizeFmt): string => {
    const { num: n, suffix } = sizeSuffixer[fmt](size)
    const rendered = n >= 100 ? round(n).toString() : n.toFixed(1)
    return `${rendered}${suffix || ""}`
}
