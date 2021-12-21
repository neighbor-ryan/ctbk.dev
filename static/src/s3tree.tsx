import React, {useEffect, useMemo, useState,} from "react";
import moment from 'moment'
import {Object,} from "aws-sdk/clients/s3";
import {Link, Location, useLocation} from "react-router-dom";
import _, {entries} from "lodash";
import {useQueryParam} from "use-query-params";
import {QueryParamConfig} from "serialize-query-params/lib/types";
import {Dir, File, Row, S3Fetcher} from "./s3fetcher";
import {render} from "react-dom";
import {renderSize} from "./size";

const { ceil, floor, max, min } = Math

export const defaultReplaceChars = { '%2F': '/', '%21': '!', }

const stringParam: QueryParamConfig<string> = {
    encode: (value: string) => value,
    decode: (value: string | (string | null)[] | null | undefined) => {
        if (typeof value === 'string') {
            return value
        }
        if (!value) return ''
        if (!value.length) return ''
        return value[value.length - 1] || ''
    }
}

function rstrip(s: string, suffix: string): string {
    if (s.substring(s.length - suffix.length, s.length) == suffix) {
        return rstrip(s.substring(0, s.length - suffix.length), suffix)
    } else {
        return s
    }
}

function stripPrefix(prefix: string[], k: string) {
    const pcs = k.split('/')
    if (!_.isEqual(prefix, pcs.slice(0, prefix.length))) {
        throw new Error(`Key ${k} doesn't start with prefix ${prefix.join("/")}`)
    }
    return pcs.slice(prefix.length).join('/')
}

function buildQueryString(o: { [k: string]: string }, location: Location, searchParams?: URLSearchParams,) {
    let sp: URLSearchParams
    if (!searchParams) {
        const { search: query } = location;
        sp = new URLSearchParams(query)
    } else {
        sp = searchParams
    }
    entries(o).forEach(([ k, v ]) => {
        sp.set(k, v)
    })
    let queryString = sp.toString()
    const replaceChars = defaultReplaceChars
    //replaceChars = replaceChars || defaultReplaceChars
    Object.entries(replaceChars).forEach(([ k, v ]) => {
        queryString = queryString.replaceAll(k, v)
    })
    return queryString
}

function DirRow({ Prefix }: Dir, { bucket, location }: { bucket: string, location: Location }) {
    const prefix = Prefix ? rstrip(Prefix, '/') : ''
    const pieces = prefix.split('/')
    const name = pieces[pieces.length - 1]
    const fetcher = new S3Fetcher({bucket, key: prefix})
    // const end = fetcher.cache?.end
    const totalSize = fetcher.cache?.totalSize
    const mtime = fetcher.cache?.LastModified
    return <tr key={Prefix}>
        <td key="name">
            <Link to={{search: buildQueryString({ p: `${bucket}/${prefix}`}, location) }}>{name}</Link>
        </td>
        <td key="size">{totalSize ? renderSize(totalSize, 'iec') : ''}</td>
        <td key="mtime">{mtime ? moment(mtime).format('YYYY-MM-DD') : ''}</td>
    </tr>
}

function FileRow({ Key, LastModified, Size, }: File, { prefix }: { prefix: string[] }) {
    return <tr key={Key}>
        <td key="name">{Key ? stripPrefix(prefix, Key) : ""}</td>
        <td key="size">{renderSize(Size, 'iec')}</td>
        <td key="mtime">{moment(LastModified).format('YYYY-MM-DD')}</td>
    </tr>
}

function TableRow(row: Row, extra: { bucket: string, location: Location, prefix: string[], }) {
    return (
        (row as Dir).Prefix !== undefined
            ? DirRow(row as Dir, extra)
            : FileRow(row as File, extra)
    )
}

export function S3Tree({}) {
    const location = useLocation()

    const [ path, setPath ] = useQueryParam('p', stringParam)
    const [ bucket, ...rest ] = useMemo(() => path.split('/'), [ path ] )
    const key = rest.join('/')

    // const [ page, setPage ] = useState<ListObjectsV2Output | null>(null)
    // if (page && (page?.Name != bucket || ((page?.Prefix || key) && page?.Prefix?.replace(/\/$/, '') != key?.replace(/\/$/, '')))) {
    //     console.warn(`Page doesn't match bucket or key? ${bucket} ${key}`, page)
    //     setPage(null)
    //     return <div>hmm…</div>
    // }

    const [ pageIdx, setPageIdx ] = useState(0)
    const [ pageSize, setPageSize ] = useState(100)
    const [ rows, setRows ] = useState<Row[] | null>(null)
    const mismatchedRows = (rows || []).filter(
        row => {
            const Prefix = (row as Dir).Prefix
            const Key = Prefix ? Prefix : (row as File).Key
            return Key.substring(0, key.length) != key
        }
    )
    if (mismatchedRows.length) {
        console.warn(`${mismatchedRows.length} rows don't match bucket/key ${bucket}/${key}:`, mismatchedRows)
        setRows(null)
        return <div>hmm…</div>  // TODO
    }
    //if (rows.map(row => ))
    const [ s3PageSize, setS3PageSize ] = useState(1000)
    const [ total, setTotal ] = useState<number | null>(null)
    const numPages = useMemo(
        () => total === null ? null : ceil(total / pageSize),
    [ total, pageIdx, pageSize, ]
    )

    console.log(`Initializing, bucket ${bucket} key ${key}, page idx ${pageIdx} size ${pageSize} num ${numPages} total ${total}, location.state ${location.state}`)
    const [region, setRegion] = useState('us-east-1')
    const fetcher = useMemo(() => {
        console.log(`new fetcher for bucket ${bucket} (key ${key}), current rows:`, rows)
        return new S3Fetcher({ bucket, region, key, pageSize: s3PageSize, endCb: setTotal })
    }, [ bucket, region, key ])

    if (total === null && fetcher.cache?.end !== undefined) {
        setTotal(fetcher.cache?.end)
    }

    const start = pageSize * pageIdx
    const end = start + pageSize

    const [ totalSize, setTotalSize ] = useState<number | null>(null)

    useEffect(
        () => {
            fetcher.get(start, end).then(setRows)
        },
        [ fetcher, pageIdx, pageSize, bucket, key, ]
    )

    useEffect(
        () => { fetcher.computeMetadata().then(( { totalSize }) => setTotalSize(totalSize)) },
        [ fetcher, bucket, key, ]
    )

    if (!rows) {
        return <div>Fetching {bucket}, page {pageIdx}…</div>
    }

    console.log("Rows:", rows)

    const normedKey = key ? rstrip(key, '/') : key
    const keyPieces = normedKey ? normedKey.split('/') : []

    const ancestors =
        ([] as string[])
            .concat(keyPieces)
            .reduce<{ path: string, name: string }[]>(
                (prvs, nxt) => {
                    const parent = prvs[prvs.length - 1].path
                    return prvs.concat([{ path: `${parent}/${nxt}`, name: nxt }])
                },
                [ { path: bucket, name: bucket }, ],
            )

    const cache = fetcher.cache
    const numChildren = cache?.end
    const mtime = cache?.LastModified

    return (
        <div className="container">
            <div className="row">
                <ul className="breadcrumb">
                    {
                        ancestors.map(({ path, name }) => {
                            return <li key={path}>
                                <Link to={{ search: buildQueryString({ p: rstrip(path, '/')}, location) }}>{name}</Link>
                            </li>
                        })
                    }
                </ul>
                <span>{numChildren} children</span>,&nbsp;
                <span>total size {totalSize ? renderSize(totalSize, 'iec') : ''}</span>,&nbsp;
                <span>last modified {moment(mtime).format('YYYY-MM-DD')}</span>
                <button className="clear-cache" onClick={() => fetcher.clearCache()}>Clear cache</button>
            </div>
            <div className="row">
                <table className="files-list">
                    <thead>
                    <tr>
                        <th key="name">Name</th>
                        <th key="size">Size</th>
                        <th key="mtime">Modified</th>
                    </tr>
                    </thead>
                    <tbody>{
                        // rows.map(({ Prefix }) => {
                        //     const prefix = Prefix ? rstrip(Prefix, '/') : ''
                        //     const pieces = prefix.split('/')
                        //     const name = pieces[pieces.length - 1]
                        //     return <tr key={Prefix}>
                        //         <td key="name"><Link to={{search: buildQueryString({ p: `${bucket}/${prefix}`}) }}>{name}</Link></td>
                        //         <td key="size"/>
                        //         <td key="mtime"/>
                        //     </tr>
                        // })
                    }{
                        rows.map(row =>
                            TableRow(row, { bucket, location, prefix: keyPieces, })
                        )
                    }
                    </tbody>
                </table>
            </div>
            <div className="pagination">
                <button onClick={() => setPageIdx(0)} disabled={pageIdx == 0}>{'<<'}</button>{' '}
                <button onClick={() => setPageIdx(pageIdx - 1)} disabled={pageIdx == 0}>{'<'}</button>{' '}
                <button onClick={() => setPageIdx(pageIdx + 1)} disabled={numPages === null || pageIdx + 1 == numPages}>{'>'}</button>{' '}
                <button onClick={() => setPageIdx((numPages || 0) - 1)} disabled={numPages === null || pageIdx + 1 == numPages}>{'>>'}</button>{' '}
                <span className="page-number">
                    Page{' '}
                    <span>{pageIdx + 1} of {numPages === null ? '?' : numPages}</span>{' '}
                </span>
                <span className="goto-page">| Go to page:{' '}</span>
                <input
                    type="number"
                    defaultValue={pageIdx + 1}
                    onChange={e => setPageIdx(e.target.value ? Number(e.target.value) - 1 : 0)}
                    style={{ width: '100px' }}
                />
                {' '}
                <select
                    value={pageSize}
                    onChange={e => setPageSize(Number(e.target.value))}
                >
                    {[10, 20, 50, 100].map(pageSize => (
                        <option key={pageSize} value={pageSize}>
                            Show {pageSize}
                        </option>
                    ))}
                </select>
            </div>
        </div>
    )
}
