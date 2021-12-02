// import {Setter} from "./utils";
// import {useEffect, useState} from "react";
// import {NavigateFunction, useLocation, useNavigate} from "react-router-dom";
// import _ from "lodash";
// import queryString from "query-string";
//
// type QueryComponentValue = string | null
// type QueryValue = QueryComponentValue | QueryComponentValue[]
// type NonNullableQueryValue = string | string[]
// type NonNullableStringQueryValue = string
// type StringQueryValue = QueryComponentValue
// type Rendered = QueryValue | NonNullableQueryValue | NonNullableStringQueryValue | StringQueryValue
//
// //export type Getter<R extends Rendered = QueryValue> = (query: string) => (k: string) => R
// export type Parse<T, R extends Rendered = QueryValue> = (queryParam: R) => T
// export type Render<T, R extends Rendered = QueryValue> = (value: T) => R
// export type Eq<T> = (l: T, r: T) => boolean
//
// // const NonNullableGetter: Getter<QueryValue> = (query: string) => {
// //     const searchParams = new URLSearchParams(query)
// //     return (k: string) => {
// //         const queryValue = searchParams.get(k)
// //         if (queryValue === null)
// //         return queryValue
// //     }
// // }
//
// export abstract class QueryState<T, R extends Rendered = QueryValue> {
//     abstract defaultValue: T
//     abstract coerce(queryValue: QueryValue): R
//     abstract parse(queryParams: R): T
//     render(value: T): R {
//         return (value as any).toString()
//     }
//     eq(l: T, r: T): boolean {
//         return l == r
//     }
// }
//
// export function nonNullable<T>(qs: QueryState<T, NullableQueryValue>): QueryState<T> {
//     const { defaultValue, parse, render, eq } = qs
//     return queryState<T, QueryValue>({
//         defaultValue,
//         parse: queryParam => parse(queryParam),
//         render: value => {
//             const rendered = render(value)
//             if (rendered === null) {
//                 throw new Error(`Null rendered query param value: ${value} -> ${rendered}`)
//             }
//             return rendered
//         },
//         eq,
//     })
// }
//
// export function nullable<T>(qs: QueryState<T>): QueryState<T, NullableQueryValue> {
//     const { defaultValue, parse, render, eq } = qs
//     return queryState<T, NullableQueryValue>({
//         defaultValue,
//         parse: queryParam => {
//             if (queryParam === null) {
//                 return null
//                 //throw new Error("null queryParam not allowed")
//             }
//             return parse(queryParam)
//         },
//         render: value => {
//             const rendered = render(value)
//             if (rendered === null) {
//                 throw new Error(`Null rendered query param value: ${value} -> ${rendered}`)
//             }
//             return rendered
//         },
//         eq,
//     })
// }
//
// export class StringQueryState extends QueryState<string> {
//     readonly defaultValue: string = ""
//     parse(queryParam: string): string { return queryParam }
// }
//
// export const stringQueryState = new StringQueryState()
//
// export function queryState<T, R extends Rendered = QueryValue >(
//     { defaultValue, parse, render, eq, }: {
//         defaultValue: T,
//         parse: Parse<T, R>,
//         render?: Render<T, R>,
//         eq?: Eq<T>,
//     }
// ): QueryState<T, R> {
//     return new class extends QueryState<T, R> {
//         defaultValue = defaultValue
//         parse(queryParams: R): T { return parse(queryParams) }
//         render(value: T): R { return render ? render(value) : super.render(value) }
//         eq(l: T, r: T): boolean { return eq ? eq(l, r) : super.eq(l, r) }
//     }
// }
//
// export const numbersQueryState = queryState<number[]>({
//     defaultValue: [],
//     parse: (queryParam: string) => queryParam.split(',').map(parseInt),
// })
//
// export function stringyQueryState<T extends string>(defaultValue: T): QueryState<T> {
//     return queryState<T>({
//         defaultValue,
//         parse: (queryParam: string) => queryParam as T,
//     })
// }
//
// export function useStringyQueryState<T extends string>(param: string, defaultValue: T) {
//     return useQueryState<T>(param, defaultValue, stringyQueryState<T>(defaultValue))
// }
//
// export const boolQueryState = (defaultValue?: boolean) => queryState<boolean, NullableQueryValue>({
//     defaultValue: defaultValue || false,
//     parse: queryParam =>
//         queryParam === ''
//             ? false
//             : queryParam === null
//                 ? true
//                 : defaultValue || false,
//     render: value => value ? null : '',
// })
//
// export const useBoolQueryState =
//     (k: string, defaultValue?: boolean) =>
//         useQueryState<boolean, NullableQueryValue>('rel', defaultValue || false, boolQueryState(defaultValue || false))
// // export class StringTypeQueryState<T extends string> extends QueryState<T> {
// //     readonly defaultValue: string = ""
// //     parse(queryParam: string): string { return queryParam }
// // }
// //
// // export const stringQueryState<T extends string> = new StringTypeQueryState()
//
// export class NumericQueryState extends QueryState<number> {
//     constructor(readonly defaultValue: number) {
//         super();
//     }
//     parse(queryParams: string): number {
//         const value = parseInt(queryParams);
//         return isNaN(value) ? this.defaultValue : value
//     }
// }
// export const numericQueryState = (defaultValue: number) => new NumericQueryState(defaultValue)
//
// // export function queryParamToState<T>(
// //     { queryKey, queryValue, state, setState, defaultValue, parse }: {
// //         queryKey: string,
// //         queryValue: NullableQueryValue,
// //         state: T,
// //         setState: Setter <T>,
// //         defaultValue: T,
// //         parse: Parse<T, R>,
// //     }
// // ) {
// //     useEffect(
// //         () => {
// //             if (state === null) {
// //                 if (queryValue) {
// //                     const parsedState = parse(queryValue)
// //                     console.log(`queryKey ${queryKey} = ${queryValue}: parsed`, parsedState)
// //                     setState(parsedState)
// //                 } else {
// //                     console.log(`queryKey ${queryKey} = ${queryValue}: setting default value`, defaultValue)
// //                     setState(defaultValue)
// //                 }
// //             }
// //         },
// //         [ queryValue ]
// //     )
// // }
//
// export const defaultReplaceChars = { '%2F': '/', '%21': '!', '%2C': ',', }
//
// export function stateToQueryParam<T, R extends Rendered = QueryValue>(
//     { queryKey, state, searchParams, navigate, defaultValue, render, eq, replaceChars, }: {
//         queryKey: string,
//         queryValue: R,
//         state: T,
//         searchParams: URLSearchParams,
//         navigate: NavigateFunction,
//         defaultValue: T,
//         render?: Render<T, R>,
//         eq?: Eq<T>,
//         replaceChars?: { [k: string]: string, },
//     }
// ) {
//     useEffect(
//         () => {
//             if (state === null) return
//             if (
//                 eq ?
//                     eq(state, defaultValue) :
//                     typeof state === 'object' ?
//                         _.isEqual(state, defaultValue) :
//                         (state == defaultValue)
//             ) {
//                 searchParams.delete(queryKey)
//             } else {
//                 const queryValue = render ? render(state) : (state as any).toString()
//                 searchParams.set(queryKey, queryValue)
//             }
//             const queryString = getQueryString({ searchParams, replaceChars })
//             console.log(`queryKey ${queryKey} new string`, queryString)
//             navigate(
//                 {
//                     pathname: "",
//                     search: queryString,
//                 },
//                 { replace: true, },
//             )
//         },
//         [ state ]
//     )
// }
//
// export function getQueryString(
//     { searchParams, replaceChars }: {
//         searchParams?: URLSearchParams,
//         replaceChars?: { [k: string]: string },
//     }
// ) {
//     if (!searchParams) {
//         const { search: query } = useLocation();
//         searchParams = new URLSearchParams(query)
//     }
//     let queryString = searchParams.toString()
//     replaceChars = replaceChars || defaultReplaceChars
//     Object.entries(replaceChars).forEach(([ k, v ]) => {
//         queryString = queryString.replaceAll(k, v)
//     })
//     return queryString
// }
//
// export function fromQueryString({ query }: { query?: string, }): { [k: string]: string } {
//     if (query === undefined) {
//         query = useLocation().search
//     }
//     const searchParams = new URLSearchParams(query)
//     return Object.fromEntries(searchParams.entries())
// }
//
// export function toQueryString(o: { [k: string]: string }): string {
//     let searchParams = new URLSearchParams()
//     Object.entries(o).forEach(([ k, v ]) => {
//         searchParams.set(k, v)
//     })
//     const queryString = getQueryString({searchParams})
//     return queryString
// }
//
// export function assignQueryString(
//     { query, o }: {
//         query?: string,
//         o: { [k: string]: string }
//     }
// ): string {
//     return toQueryString(
//         Object.assign(
//             {},
//             fromQueryString({ query }),
//             o,
//         )
//     )
// }
//
// export function nullableQueryState<T>(
//     queryKey: string,
//     queryState: QueryState<T>,
//     initialValue?: T,
// ): [ T | null, Setter<T | null>, ] {
//     return useQueryState<T | null>(
//         queryKey,
//         initialValue === undefined
//             ? null
//             : initialValue,
//         queryState,
//     )
// }
//
// export function useQueryState<T>(
//     queryKey: string,
//     defaultValue: T,
//     queryState: QueryState<T>,
// ): [ T, Setter<T>, ] {
//     const { parse, render, eq, } = queryState
//     const { search: query } = useLocation();
//     const searchParams = queryString.parse(query)
//     // const searchParams = new URLSearchParams(query)
//     const queryValue: string | string[] | null | undefined = searchParams[queryKey]
//
//     const initialValue = queryValue === undefined ? defaultValue : parse(queryValue)
//     const [ state, setState ] = useState<T>(initialValue)
//
//     useEffect(
//         () => {
//             if (state === null) {
//                 if (queryValue) {
//                     const parsedState = parse(queryValue)
//                     console.log(`queryKey ${queryKey} = ${queryValue}: parsed`, parsedState)
//                     setState(parsedState)
//                 } else {
//                     console.log(`queryKey ${queryKey} = ${queryValue}: setting default value`, defaultValue)
//                     setState(defaultValue)
//                 }
//             }
//         },
//         [ queryValue ]
//     )
//     // queryParamToState<T, R>({
//     //     queryKey,
//     //     queryValue,
//     //     state,
//     //     setState,
//     //     defaultValue,
//     //     parse,
//     // })
//
//     let navigate = useNavigate()
//     stateToQueryParam<T, R>({
//         queryKey,
//         queryValue,
//         state,
//         defaultValue,
//         render,
//         searchParams,
//         navigate,
//         eq,
//     })
//     return [ state, setState, ]
// }
