import React, {useEffect, useMemo, useState,} from "react";
import AWS from 'aws-sdk';
import moment from 'moment'
import {ListObjectsV2Output, ListObjectsV2Request, Object} from "aws-sdk/clients/s3";
import {Link, useLocation, useParams} from "react-router-dom";
import _ from "lodash";

const { ceil, floor, max, min } = Math

type Row = Object

class S3Bucket {
    pageSize: number = 1000
    pagePromises: Promise<ListObjectsV2Output>[] = []
    end: number | undefined = undefined
    endCb?: (end: number) => void
    s3?: AWS.S3

    constructor(
        readonly bucket: string,
        readonly region: string,
        readonly IdentityPoolId: string,
        readonly key?: string,
    ) {
        AWS.config.region = region;
        AWS.config.credentials = new AWS.CognitoIdentityCredentials({ IdentityPoolId, });

        this.s3 = new AWS.S3({});
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

    getPage(pageIdx: number): Promise<ListObjectsV2Output> {
        const { pagePromises, } = this
        if (pageIdx < pagePromises.length) {
            return pagePromises[pageIdx]
        }
        return this.nextPage().then(() => this.getPage(pageIdx))
    }

    nextPage(): Promise<ListObjectsV2Output> {
        const { bucket, key, s3, pageSize, } = this
        const Prefix = key ? (key[key.length - 1] == '/' ? key : (key + '/')) : key
        if (!s3) {
            throw Error("S3 client not initialized")
        }
        let continuing: Promise<string | undefined>
        const numPages = this.pagePromises.length
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
            //debugger
            return s3.listObjectsV2(params).promise().then(page => {
                const truncated = page.IsTruncated
                const numItems = page.Contents?.length
                console.log(
                    `Got page idx ${numPages} (${numItems} items, truncated ${truncated}, continuation ${page.NextContinuationToken})`
                )
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

export function S3Tree({}) {
    const location = useLocation()
    const { bucket: bucketParam, key, } = useParams()
    const bucket = bucketParam || 'ctbk'

    const [ page, setPage ] = useState<ListObjectsV2Output | null>(null)
    if (page && (page?.Name != bucket || ((page?.Prefix || key) && page?.Prefix?.replace(/\/$/, '') != key?.replace(/\/$/, '')))) {
        console.log(`Page doesn't match bucket or key? ${bucket} ${key}`, page)
        setPage(null)
        return <div>hmm…</div>
    }
    // if (location.state && page) {
    //     console.log("found location.state and page, nulling")
    //     setPage(null)
    //     console.log("hmm?")
    // }

    console.log(`Initializing, bucket ${bucket} key ${key}, page size ${page?.Contents?.length}, location.state ${location.state}`)
    const region = 'us-east-1'
    const IdentityPoolId = Array.from('969d5e153efb-f898-70c4-f48f-2f6c1079:1-tsae-su').reverse().join('')
    const fetcher = useMemo(() => {
        // setPage(null)
        console.log(`new fetcher for bucket ${bucket} (key ${key}), page`, page)
        return new S3Bucket(bucket, region, IdentityPoolId, key)
    }, [ bucket, key ])

    const [ pageIdx, setPageIdx ] = useState(0)

    useEffect(
        () => {
            // console.log("nulling page")
            // setPage(null)
            fetcher.getPage(pageIdx).then(setPage)
        },
        [ fetcher, pageIdx, bucket, key, ]
    )

    if (!page) {
        return <div>Fetching {bucket}, page {pageIdx}…</div>
    }

    // const contents = (page?.Contents || []) //as Row[]
    console.log("Rows:", page)

    // const columns = [
    //     { Header: ''}
    // ]

    const ancestors =
        ([] as string[])
            .concat(key ? key.split('/') : [])
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
        const prefix = key ? key.split('/') : []
        if (!_.isEqual(prefix, pcs.slice(0, prefix.length))) {
            throw new Error(`Key ${k} doesn't start with prefix ${prefix.join("/")}`)
        }
        return pcs.slice(prefix.length).join('/')
    }

    return (
        <div className="container">
            <div className="row">
                <ul className="breadcrumb">
                    {
                        ancestors.map(({ path, name }) =>
                            <li key={path}>
                                <Link to={`/s3/${path}`}>{name}</Link>
                            </li>
                        )
                    }
                </ul>
            </div>
            <div className="row">
                <table className="files">
                    <thead>
                    <tr>
                        <th key="parent">Parent</th>
                        <th key="key">Key</th>
                        <th key="size">Size</th>
                        <th key="mtime">Modified</th>
                    </tr>
                    </thead>
                    <tbody>{
                        (page?.CommonPrefixes || []).map(({ Prefix }) =>
                            <tr key={Prefix}>
                                <td key="parent">{parent}</td>
                                <td key="key"><Link to={`/s3/${bucket}/${Prefix}`}>{Prefix}</Link></td>
                                <td key="size"/>
                                <td key="mtime"/>
                            </tr>
                        )
                    }{
                        (page?.Contents || []).map(({ Key, Size, LastModified, }) =>
                            <tr key={Key}>
                                <td key="parent">{parent}</td>
                                <td key="key">{Key ? stripPrefix(Key) : ""}</td>
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
