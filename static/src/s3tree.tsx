import React, {useEffect, useMemo, useState,} from "react";
import AWS from 'aws-sdk';
import moment, {Duration, DurationInputArg2, Moment} from 'moment'
import {ListObjectsV2Output, ListObjectsV2Request, Object} from "aws-sdk/clients/s3";
import {Link, useLocation} from "react-router-dom";
import _, {entries} from "lodash";
import {useQueryParam} from "use-query-params";
import {QueryParamConfig} from "serialize-query-params/lib/types";

const { ceil, floor, max, min } = Math

type Row = Object

type Cache = {
    pages: ListObjectsV2Output[]
    timestamp: Moment
}

class S3Bucket {
    pageSize: number = 1000
    pagePromises: Promise<ListObjectsV2Output>[] = []
    end: number | undefined = undefined
    endCb?: (end: number) => void
    s3?: AWS.S3
    cache?: Cache
    cacheKey: string
    ttl?: Duration

    constructor(
        readonly bucket: string,
        readonly region: string,
        readonly key?: string,
        readonly IdentityPoolId?: string,
        ttl?: string,
    ) {
        AWS.config.region = region;
        if (IdentityPoolId) {
            AWS.config.credentials = new AWS.CognitoIdentityCredentials({ IdentityPoolId, });
        }
        this.s3 = new AWS.S3({});
        const cacheKeyObj = key ? { bucket, key } : { bucket }
        this.cacheKey = JSON.stringify(cacheKeyObj)
        const cacheStr = localStorage.getItem(this.cacheKey)
        console.log(`Cache:`, cacheKeyObj, `${this.cacheKey} (key: ${key})`)
        if (cacheStr) {
            const { pages, timestamp } = JSON.parse(cacheStr)
            this.cache = { pages, timestamp: moment(timestamp), }
        }
        if (ttl) {
            const groups = ttl.match(/(?<n>\d+)(?<unit>.*)/)?.groups
            if (!groups) {
                throw Error(`Unrecognized ttl: ${ttl}`)
            }
            const n = parseInt(groups.n)
            const unit: DurationInputArg2 = groups.unit as DurationInputArg2
            this.ttl = moment.duration(n, unit)
        } else {
            this.ttl = moment.duration(1, 'd')
        }
    }

    get(start: number, end: number): Promise<Row[]> {
        const { pageSize, } = this
        const startPage = floor(start / pageSize)
        const endPage = ceil(end / pageSize)
        const pageIdxs: number[] = Array.from(Array(endPage - startPage).keys()).map(i => startPage + i)
        const pages: Promise<ListObjectsV2Output>[] = pageIdxs.map(this.getPage)
        const slicedPages: Promise<Row[]>[] =
            pages.map(
                (pagePromise, idx) => pagePromise.then(
                    page => {
                        const pageIdx = startPage + idx
                        const pageStart = pageIdx * pageSize
                        const startIdx = max(start - pageStart, 0)
                        const endIdx = min(end - pageStart, pageSize)
                        if (!page.Contents) {
                            throw Error(`Page ${pageIdx} has no Contents: ${page}`)
                        }
                        return page.Contents.slice(startIdx, endIdx) as Row[]
                    }
                )
            )
        return (
            Promise.all(slicedPages)
                .then(
                    (values: Row[][]) =>
                        ([] as Row[]).concat(...values)
                )
        )
    }

    saveCache() {
        if (!this.cache) {
            localStorage.removeItem(this.cacheKey)
        } else {
            localStorage.setItem(this.cacheKey, JSON.stringify(this.cache))
        }
    }

    getPage(pageIdx: number): Promise<ListObjectsV2Output> {
        const { pagePromises, cache, bucket, key, } = this
        console.log(`Fetcher ${bucket} (${key}):`, cache)
        if (cache) {
            const { pages, timestamp } = cache
            const { ttl } = this
            const now = moment()
            if (timestamp.add(ttl) < now) {
                console.log(`Cache purge (${timestamp} + ${ttl} < ${now}):`, this.cache)
                this.cache = undefined
                this.saveCache()
            } else {
                if (pageIdx in pages) {
                    console.log(`Cache hit: ${pageIdx} (timestamp ${this.cache?.timestamp})`)
                    return Promise.resolve(pages[pageIdx])
                }
                console.log(`Cache miss: ${pageIdx} (timestamp ${this.cache?.timestamp})`)
            }
        }
        if (pageIdx < pagePromises.length) {
            return pagePromises[pageIdx]
        }
        return this.nextPage().then(() => this.getPage(pageIdx))
    }

    nextPage(): Promise<ListObjectsV2Output> {
        const { bucket, key, s3, pageSize, IdentityPoolId, } = this
        const Prefix = key ? (key[key.length - 1] == '/' ? key : (key + '/')) : key
        if (!s3) {
            throw Error("S3 client not initialized")
        }
        let continuing: Promise<string | undefined>
        const numPages = this.pagePromises.length
        const pageIdx = numPages
        if (numPages) {
            continuing =
                this.pagePromises[numPages - 1]
                    .then(last => {
                        if (last.IsTruncated || !last.NextContinuationToken) {
                            throw new Error(
                                `Asked for next page (idx ${numPages}) but page ${numPages - 1} is truncated ` +
                                `(${last.IsTruncated}, ${last.NextContinuationToken}, ${last.Contents?.length} items)`
                            )
                        }
                        return last.NextContinuationToken
                    })
        } else {
            continuing = Promise.resolve(undefined)
        }

        const page = continuing.then(ContinuationToken => {
            const params: ListObjectsV2Request = {
                Bucket: bucket,
                Prefix,
                MaxKeys: pageSize,
                Delimiter: '/',
                ContinuationToken,
            };
            console.log(`Fetching page idx ${numPages}`)
            const timestamp = moment()
            const pagePromise: Promise<ListObjectsV2Output> =
                IdentityPoolId ?
                    s3.listObjectsV2(params).promise() :
                    s3.makeUnauthenticatedRequest('listObjectsV2', params).promise()
            return pagePromise.then(page => {
                const truncated = page.IsTruncated
                const numItems = page.Contents?.length
                console.log(
                    `Got page idx ${numPages} (${numItems} items, truncated ${truncated}, continuation ${page.NextContinuationToken})`
                )
                if (!this.cache) {
                    let pages = []
                    pages[pageIdx] = page
                    this.cache = { pages, timestamp, }
                    console.log("Fresh cache:", this.cache)
                    this.saveCache()
                } else {
                    this.cache.pages[pageIdx] = page
                    console.log(`Cache page idx ${pageIdx}:`, page)
                    if (timestamp < this.cache.timestamp) {
                        console.log(`Cache page idx ${pageIdx}: timestamp ${this.cache.timestamp} → ${timestamp}`)
                        this.cache.timestamp = timestamp
                    }
                    this.saveCache()
                }
                if (!truncated) {
                    this.end = numPages * pageSize + (numItems || 0)
                    if (this.endCb) {
                        this.endCb(this.end)
                    }
                }
                return page
            })
        })
        this.pagePromises.push(page)
        return page
    }
}

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

export function S3Tree({}) {
    const location = useLocation()

    function buildQueryString(o: { [k: string]: string }, searchParams?: URLSearchParams,) {
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

    const [ path, setPath ] = useQueryParam('p', stringParam)
    const [ bucket, ...rest ] = useMemo(() => path.split('/'), [ path ] )
    const key = rest.join('/')

    const [ page, setPage ] = useState<ListObjectsV2Output | null>(null)
    if (page && (page?.Name != bucket || ((page?.Prefix || key) && page?.Prefix?.replace(/\/$/, '') != key?.replace(/\/$/, '')))) {
        console.log(`Page doesn't match bucket or key? ${bucket} ${key}`, page)
        setPage(null)
        return <div>hmm…</div>
    }

    console.log(`Initializing, bucket ${bucket} key ${key}, page size ${page?.Contents?.length}, location.state ${location.state}`)
    const region = 'us-east-1'
    const fetcher = useMemo(() => {
        console.log(`new fetcher for bucket ${bucket} (key ${key}), page`, page)
        return new S3Bucket(bucket, region, key,)
    }, [ bucket, key ])

    const [ pageIdx, setPageIdx ] = useState(0)

    useEffect(
        () => {
            fetcher.getPage(pageIdx).then(setPage)
        },
        [ fetcher, pageIdx, bucket, key, ]
    )

    if (!page) {
        return <div>Fetching {bucket}, page {pageIdx}…</div>
    }

    console.log("Rows:", page)

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

    const parentPieces = [ bucket ].concat(key ? key.split('/') : [])
    const parent = parentPieces.join('/')

    function stripPrefix(k: string) {
        const pcs = k.split('/')
        if (!_.isEqual(keyPieces, pcs.slice(0, keyPieces.length))) {
            throw new Error(`Key ${k} doesn't start with prefix ${keyPieces.join("/")}`)
        }
        return pcs.slice(keyPieces.length).join('/')
    }

    return (
        <div className="container">
            <div className="row">
                <ul className="breadcrumb">
                    {
                        ancestors.map(({ path, name }) => {
                            return <li key={path}>
                                <Link to={{ search: buildQueryString({ p: rstrip(path, '/')}) }}>{name}</Link>
                            </li>
                        })
                    }
                </ul>
            </div>
            <div className="row">
                <table className="files">
                    <thead>
                    <tr>
                        <th key="name">Name</th>
                        <th key="size">Size</th>
                        <th key="mtime">Modified</th>
                    </tr>
                    </thead>
                    <tbody>{
                        (page?.CommonPrefixes || []).map(({ Prefix }) => {
                            const prefix = Prefix ? rstrip(Prefix, '/') : ''
                            const pieces = prefix.split('/')
                            const name = pieces[pieces.length - 1]
                            return <tr key={Prefix}>
                                <td key="name"><Link to={{search: buildQueryString({ p: `${bucket}/${prefix}`}) }}>{name}</Link></td>
                                <td key="size"/>
                                <td key="mtime"/>
                            </tr>
                        })
                    }{
                        (page?.Contents || []).map(({ Key, Size, LastModified, }) =>
                            <tr key={Key}>
                                <td key="name">{Key ? stripPrefix(Key) : ""}</td>
                                <td key="size">{Size}</td>
                                <td key="mtime">{moment(LastModified).format('YYYY-MM-DD')}</td>
                            </tr>
                        )
                    }
                    </tbody>
                </table>
            </div>
        </div>
    )
}
