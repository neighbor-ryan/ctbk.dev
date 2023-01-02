import {DataFrame, Series} from "danfojs";

const { fromEntries } = Object

export function pivot(df: DataFrame, index: string, columns: string, values: string): DataFrame {
    const rename: {[k:string]:string} = {}
    rename[`${index}_Group`] = index
    return (
        df
            .groupby([index])
            .apply(g => (
                g
                    .drop({ columns: [index] })
                    .setIndex({ column: columns })
                    .transpose()
                    .drop({ index: [columns] })
            ))
            .rename(rename)
            .setIndex({column: index})
            .drop({ columns: [index] })
    )
}

export function rollingAvg(arr: number[], n: number): number[] {
    const [ rv, _ ] = arr.reduce(([rv, sum], v, i) => {
        const dropIdx = i - n
        if (!isNaN(arr[i])) {
            sum += arr[i]
        }
        if (dropIdx >= 0 && !isNaN(arr[dropIdx])) {
            sum -= arr[dropIdx]
            rv.push(sum / n)
        } else {
            rv.push(NaN)
        }
        return [ rv, sum ]
    }, [ [] as number[], 0 ])
    return rv
}

export function rollingAvgs(df: DataFrame, n: number): DataFrame
export function rollingAvgs(df: Series, n: number): Series
export function rollingAvgs(df: DataFrame | Series, n: number): DataFrame | Series {
    if (df instanceof DataFrame) {
        let obj = fromEntries(df.columns.map(r => [r, rollingAvg(df[r].values, n)]))
        return new DataFrame(obj, {index: df.index})
    } else {
        return new Series(rollingAvg(df.values, n), { index: df.index })
    }
}

export function clampIndex(df: DataFrame, { start, end }: { start: any, end: any }): DataFrame
export function clampIndex(df: Series, { start, end }: { start: any, end: any }): Series
export function clampIndex(df: DataFrame | Series, { start, end }: { start: any, end: any }): DataFrame | Series {
    if (df instanceof DataFrame) {
        return df.loc({ rows: df.index.map(v => start <= v && v < end )})
    } else {
        return df.loc(df.index.map(v => start <= v && v < end ))
    }
}
