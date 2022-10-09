import { Sort } from "./sorts";

export type Filter = { column: string, value: string, prefix?: boolean, suffix?: boolean, op?: string }

export function build(
    {
        table,
        limit,
        offset,
        count,
        sorts,
        filters,
    }: {
        table: string,
        limit?: number,
        offset?: number,
        count?: string,
        sorts?: Sort[],
        filters?: Filter[],
    }
) {
    let query =
        count ?
            `SELECT count(*) AS ${count} FROM ${table}` :
            `SELECT * FROM ${table}`;
    if (filters?.length) {
        query += ' WHERE'
        let first = true
        for (let { column, value, prefix, suffix, op = 'LIKE' } of filters) {
            // prefix/suffix only relevant to "LIKE" ops, and default to `true` (`value` is a prefix and a suffix, i.e.
            // an exact match; `prefix: false` inserts a wildcard "%" in front, `suffix: false` in back)
            prefix = prefix === undefined ? true : prefix
            suffix = suffix === undefined ? true : suffix
            if (!first) query += ' AND'
            first = false
            const like = (prefix ? '' : '%') + value + (suffix ? '' : '%')
            // TODO: escape values; normally not doing so would be a serious vulnerability, but this is a static site
            //  reading a read-only SQLite database, so believed to be benign
            query += ` ${column} ${op} "${like}"`
        }
    }
    if (sorts?.length) {
        query += ` ORDER BY`
        let first = true
        for (const sort of sorts) {
            if (!first) query += ','
            first = false
            query += ` ${sort.column}`
            query += sort.desc ? " DESC" : " ASC"
        }
    }
    if (limit !== undefined) query += ` LIMIT ${limit}`
    if (offset !== undefined) query += ` OFFSET ${offset}`
    return query
}
