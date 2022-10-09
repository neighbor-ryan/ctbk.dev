// import {QueryState} from "./search-params";
// import _ from "lodash";

export type Sort = { column: string, desc: boolean, }

// const sortRegex = /(?<column>[^!\-]+)(?<dir>[!\-]?)/g
// export const DefaultSorts = [ { column: 'size', desc: true, }]
//
// export const parseQuerySorts = function(str: string): Sort[] {
//     if (!str) return []
//     return (
//         Array.from(str.matchAll(sortRegex))
//             .map(a =>
//                 a.groups as { column: string, dir: string }
//             )
//             .map(
//                 ( { column, dir }) => {
//                     return {
//                         column,
//                         desc: dir == '-',
//                     }
//                 }
//             )
//     )
// }
//
// export const renderQuerySorts = function(sorts: Sort[] | null): string {
//     return (sorts || [])
//         .map(( { column, desc, }, idx) => {
//             // return column + (desc ? '-' : (idx + 1 < sorts.length ? '!' : ''))
//             let s = column
//             if (desc) s += '-'
//             else if (idx + 1 < (sorts?.length || 0)) s += '!'
//             return s
//         })
//         .join('')
// }
//
// export class SortsQueryState extends QueryState<Sort[]> {
//     defaultValue: Sort[] = DefaultSorts
//     parse = parseQuerySorts
//     render = renderQuerySorts
//     eq = _.isEqual
// }
//
// export const sortsQueryState = new SortsQueryState()
