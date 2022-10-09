import React, {useEffect, useMemo, useState,} from "react";
import AWS from 'aws-sdk';
import moment, {Moment} from 'moment'
import fetch from "node-fetch";
import {ListObjectsV2Output, ListObjectsV2Request, Object} from "aws-sdk/clients/s3";
import {useParams} from "react-router-dom";

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
        const { bucket, s3, pageSize, } = this
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
                MaxKeys: pageSize,
                Delimiter: '/',
                ContinuationToken,
            };
            console.log(`Fetching page idx ${numPages}`)
            debugger
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

export default function S3Tree({}) {
    const { bucket: bucketParam, key, } = useParams()
    const bucket = bucketParam || 'ctbk'
    console.log(`Initializing, bucket ${bucket} key ${key}`)
    const region = 'us-east-1'
    const IdentityPoolId = 'us-east-1:9701c6f2-f84f-4c07-898f-bfe351e5d969'
    const fetcher = useMemo(() => {
        console.log(`new fetcher for bucket ${bucket}`)
        return new S3Bucket(bucket, region, IdentityPoolId)
    }, [ bucket ])

    const [ pageIdx, setPageIdx ] = useState(0)
    const [ page, setPage ] = useState<ListObjectsV2Output | null>(null)

    useEffect(
        () => { fetcher.getPage(0).then(setPage) },
        [ pageIdx, bucket, ]
    )

    if (!page) {
        return <div>Fetching {bucket}, page {pageIdx}…</div>
    }

    console.log("Rows:", page)
    return (
        <table>
            <thead>
            <tr>
                <th key="bucket">Bucket</th>
                <th key="key">Key</th>
                <th key="size">Size</th>
                <th key="mtime">Modified</th>
            </tr>
            </thead>
            <tbody>{
                (page?.CommonPrefixes || []).map(({ Prefix }) =>
                    <tr key={Prefix}>
                        <td key="bucket">{bucket}</td>
                        <td key="key">{Prefix}</td>
                        <td key="size"/>
                        <td key="mtime"/>
                    </tr>
                )
            }{
                (page?.Contents || []).map(({ Key, Size, LastModified, }) =>
                    <tr key={Key}>
                        <td key="bucket">{bucket}</td>
                        <td key="key">{Key}</td>
                        <td key="size">{Size}</td>
                        <td key="mtime">{moment(LastModified).format('YYYY-MM-DD')}</td>
                    </tr>
                )
            }
            </tbody>
        </table>
    )
}
