import {CommonPrefix, LastModified, ListObjectsV2Output, ListObjectsV2Request, Object, ObjectKey, Size, } from "aws-sdk/clients/s3";
import moment, {Duration, DurationInputArg2, Moment} from "moment";
import AWS from "aws-sdk";

const { ceil, floor, max, min } = Math

export type File = {
    Key: ObjectKey,
    LastModified: LastModified,
    Size: Size,
}

export type Dir = {
    Prefix: string
    LastModified?: LastModified
    Size?: Size
}

export type Page = {
    dirs: Dir[]
    files: File[]
}
export function Page(page: ListObjectsV2Output): Page {
    const dirs = (page?.CommonPrefixes || []).map(Dir)
    const files = (page?.Contents || []).map(File)
    return { dirs, files }
}

export type Row = File | Dir

export type Cache = {
    pages: ListObjectsV2Output[]
    timestamp: Moment
    end?: number
    totalSize?: number
    LastModified?: LastModified
}

export function File({ Key, LastModified, Size, }: Object): File {
    if (Key === undefined || LastModified === undefined || Size === undefined) {
        throw Error(`Object missing required field(s): Key ${Key}, LastModified ${LastModified}, Size ${Size}`)
    }
    return { Key, LastModified, Size, }
}

export function Dir({ Prefix }: CommonPrefix): Dir {
    if (Prefix === undefined) {
        throw Error(`CommonPrefix missing Prefix: ${Prefix}`)
    }
    return { Prefix }
}

type Metadata = { totalSize: number, LastModified?: LastModified, }

export class S3Fetcher {
    bucket: string
    key?: string
    pageSize: number
    pagePromises: Promise<ListObjectsV2Output>[] = []
    // end: number | undefined = undefined
    endCb?: (end: number) => void
    totalSize?: number
    s3?: AWS.S3
    IdentityPoolId?: string
    cache?: Cache
    cacheKey: string
    ttl?: Duration

    constructor(
        {
            bucket,
            region,
            key,
            IdentityPoolId,
            ttl,
            pageSize,
            endCb,
        }: {
            bucket: string,
            region?: string,
            key?: string,
            IdentityPoolId?: string,
            ttl?: string,
            pageSize?: number,
            endCb?: (end: number) => void,
        }
    ) {
        this.bucket = bucket
        this.key = key
        this.pageSize = pageSize || 1000
        this.IdentityPoolId = IdentityPoolId
        this.endCb = endCb

        if (region) {
            AWS.config.region = region;
        }
        if (IdentityPoolId) {
            AWS.config.credentials = new AWS.CognitoIdentityCredentials({ IdentityPoolId, });
        }
        this.s3 = new AWS.S3({});
        const cacheKeyObj = key ? { bucket, key } : { bucket }
        this.cacheKey = JSON.stringify(cacheKeyObj)
        const cacheStr = localStorage.getItem(this.cacheKey)
        console.log(`Cache:`, cacheKeyObj, `${this.cacheKey} (key: ${key})`)
        if (cacheStr) {
            const { pages, timestamp, end, totalSize, LastModified, } = JSON.parse(cacheStr)
            this.cache = { pages, timestamp: moment(timestamp), end, totalSize, LastModified, }
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
        const pages: Promise<ListObjectsV2Output>[] = pageIdxs.map(idx => this.getPage(idx))
        const slicedPages: Promise<Row[]>[] =
            pages.map(
                (pagePromise, idx) => pagePromise.then(
                    page => {
                        const pageIdx = startPage + idx
                        const pageStart = pageIdx * pageSize
                        const startIdx = max(start - pageStart, 0)
                        const endIdx = min(end - pageStart, pageSize)
                        const { dirs, files } = Page(page)
                        const rows = (dirs as Row[]).concat(files)
                        return rows.slice(startIdx, endIdx)
                    }
                )
            )
        return Promise.all(slicedPages).then(values => ([] as Row[]).concat(...values))
    }

    saveCache() {
        if (!this.cache) {
            localStorage.removeItem(this.cacheKey)
        } else {
            localStorage.setItem(this.cacheKey, JSON.stringify(this.cache))
        }
    }

    clearCache() {
        if (this.cache) {
            this.cache = undefined
            this.saveCache()
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

    reduce<T>(
        dirFn: (dir: Dir) => Promise<T>,
        fileFn: (file: File) => Promise<T>,
        fn: (cur: T, nxt: T) => T,
        init: T,
        cb?: (t: T) => void,
        pageIdx: number = 0,
    ): Promise<T> {
        return this.getPage(pageIdx).then(page => {
            const { dirs, files } = Page(page)

            const dirResults = Promise.all(dirs.map(dirFn)).then(results => results.reduce(fn, init))
            const fileResults = Promise.all(files.map(fileFn)).then(results => results.reduce(fn, init))
            const restPromise = page.IsTruncated ? this.reduce(dirFn, fileFn, fn, init, cb, pageIdx + 1) : Promise.resolve(undefined)
            const result = Promise.all([
                dirResults,
                fileResults,
                restPromise,
            ])
                .then(([ dirs, files, rest, ]) => {
                    const cur = fn(dirs, files)
                    return rest === undefined ? cur : fn(cur, rest)
                })
            return (
                cb ?
                    result.then(total => {
                        cb(total)
                        return total
                    }) :
                    result
            )
        })
    }

    computeMetadata(): Promise<Metadata> {
        const { bucket, key } = this
        const cached = { totalSize: this.cache?.totalSize, LastModified: this.cache?.LastModified }
        if (cached.totalSize !== undefined && cached.LastModified !== undefined) {
            console.log(`computeMetadata: ${bucket}/${key} cache hit`)
            return Promise.resolve(cached as Metadata)
        }
        console.log(`computeMetadata: ${bucket}/${key}; computing`)
        return this.reduce<Metadata>(
            dir => new S3Fetcher({ bucket, key: dir.Prefix }).computeMetadata(),
            ({Size, LastModified,}) => Promise.resolve({ totalSize: Size, LastModified, }),
            ({ totalSize: ls, LastModified: lm }, { totalSize: rs, LastModified: rm }) => {
                return {
                    totalSize: ls + rs,
                    LastModified: lm === undefined ? rm : rm === undefined ? lm : lm > rm ? lm : rm,
                }
            },
            { totalSize: 0, },
            ({ totalSize, LastModified }) => {
                if (this.cache) {
                    console.log(`Setting metadata for ${bucket}/${key}: totalSize ${totalSize}, mtime ${LastModified}`)
                    this.cache.totalSize = totalSize
                    this.cache.LastModified = LastModified
                    this.saveCache()
                } else {
                    console.warn(`No cache for ${bucket}/${key}, dropping metadata: totalSize ${totalSize}, mtime ${LastModified}`)
                }
            }
        )
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
                    this.cache.end = numPages * pageSize + (numItems || 0)
                    if (this.endCb) {
                        this.endCb(this.cache.end)
                    }
                }
                return page
            })
        })
        this.pagePromises.push(page)
        return page
    }
}
