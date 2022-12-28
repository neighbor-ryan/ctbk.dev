import React, {Fragment} from 'react';
import {getBasePath} from "next-utils/basePath"
import A from "next-utils/a";
import MD from "next-utils/md"
import {Nav} from "next-utils/nav";
import {Socials} from "next-utils/socials"
import {GitHub, url} from "../src/socials"
import Link from "next/link";
import { loadSync } from "next-utils/load"
import Head from "../src/head"
import {build, Plot, PlotsDict, PlotSpec} from "next-utils/plot"
import PlotsLoad from "next-utils/plot-load"
import css from "./index.module.css"
import {entries} from "lodash";

type AnnualizedPcts = { [k in 'All' | 'NJ']: { [y: string]: number } }

type Data = { annualizedPcts: AnnualizedPcts }

function AnnualizedPcts(o: { [y: string]: number }, yrAgos?: number[]) {
    console.log("AnnualizedPcts:", o, yrAgos)
    const yrsAgoStr =
        entries(o)
            .filter(([ k ]) => yrAgos?.includes(parseInt(k)))
            .map(([ yrsAgo, pct ]) =>
                `${yrsAgo}yr${yrsAgo == "1" ? "" : "s"}: ${pct}%`)
            .join(", ")
    return `Annualized growth rates: ${yrsAgoStr}`
}

const genderDataDisclaimer = <div className={css.plotChildren}>{
    MD(`Gender data stopped being reported in February 2021, (presumably as part of [the Lyft acquisition](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service))`)
}</div>

const bikeTypesDisclaimer = <div className={`${css.plotChildren} ${css.mid}`}>{
    MD(`
Historic "rideable type" data is pretty messy:
- "Docked bike" and "classic bike" seem to refer to the same bike type.
- Electric bikes were in widespread use for years before they start showing up in the data here (ca. March 2022).

I'm not sure how real the apparent decline in e-bike use/% is, over 2022.
`)
}</div>

const plotSpecs: PlotSpec<Data>[] = [
    {
        id: "all", name: "month_counts", title: "Citi Bike Rides per Month",
        children: ({ annualizedPcts }) => <div className={css.plotChildren}>{MD(AnnualizedPcts(annualizedPcts.All, [1, 2, 4, 8]))}</div>
    }, {
        id: "nj", name: "month_counts_nj", title: "Citi Bike Rides per Month (JC+HOB)",
        children: ({ annualizedPcts }) => <div className={css.plotChildren}>{MD(AnnualizedPcts(annualizedPcts.NJ, [1, 2, 4, 6]))}</div>
    },
    { id: "genders", name: "rides_by_gender", title: "Citi Bike Rides by Gender", children: genderDataDisclaimer, },
    { id: "gender_pcts", name: "rides_by_gender_pct", title: "Citi Bike Rides by Gender (%)", children: genderDataDisclaimer, },
    { id: "user_types", name: "rides_by_user_type", title: "Citi Bike Rides by User Type", },
    { id: "user_type_pcts", name: "rides_by_user_type_pct", title: "Citi Bike Rides by User Type (%)", },
    { id: "bike_types", name: "rides_by_bike_type", title: "Citi Bike Rides by Bike Type", children: bikeTypesDisclaimer, },
    { id: "bike_type_pcts", name: "rides_by_bike_type_pct", title: "Citi Bike Rides by Bike Type (%)", children: bikeTypesDisclaimer, },
]

export async function getStaticProps(context: any) {
    const plotsDict: PlotsDict = PlotsLoad(plotSpecs)
    const annualizedPcts = loadSync<AnnualizedPcts>('public/data/annualized_pcts.json')
    return { props: { plotsDict, annualizedPcts } }
}

export default function App({ plotsDict, annualizedPcts, }: { plotsDict: PlotsDict } & Data) {

    const basePath = getBasePath() || ""

    function icon(src: string, href: string, title: string) {
        return <a href={href} title={title}>
            <img className={css.icon} alt={title} src={`${basePath}/assets/${src}.png`} />
        </a>
    }

    const plots = build(plotSpecs, plotsDict, { annualizedPcts, })

    const title = "Citi Bike Dashboard"
    return (
        <div className={css.container}>
            <Head
                title={title}
                description={"Graphs of Citi Bike ridership over time"}
                thumbnail={`ctbk-rides`}
            />

            <main className={css.main}>
                <h1 className={css.title}>{title}</h1>
                {MD(`
[Citi Bike publishes system data](https://www.citibikenyc.com/system-data) to [s3://tripdata](https://tripdata.s3.amazonaws.com/index.html) every month.

I've cleaned, parsed, and aggregated it, and published it at [s3://ctbk](https://ctbk.s3.amazonaws.com/index.html).

See below for some visualizations and analysis; code is [on GitHub](${GitHub.href}).`
                )}
                {plots.map(plot => <div className={css.plotSection} key={plot.id}>
                    <Plot key={plot.id} {...plot} />
                    <hr/>
                </div>)}
            </main>
{/*            <div className="no-gutters row">
                <div className="col-md-12">
                    <h2>About</h2>
                    <p>Use the controls above to filter/stack by region, user type, gender, or date, and toggle aggregation of rides or total ride minutes, e.g.:</p>
                    <ul>
                        <li><Link href={"/?r=jh"}>JC+Hoboken</Link></li>
                        <li><Link href={"/?y=m&s=g&pct=&g=mf&d=1406-2101"}>Ride minute %'s, Male vs. Female</Link> (Jun 2014 - January 2021, the window where 12mo rolling avgs are possible)</li>
                    </ul>
                    <p>This plot should refresh when <a href={"https://www.citibikenyc.com/system-data"}>new data is published by Citibike</a> (typically around the 2nd week of each month, covering the previous month).</p>
                    <p><a href={"https://github.com/neighbor-ryan/ctbk.dev"}>The GitHub repo</a> has more info as well as <a href={"https://github.com/neighbor-ryan/ctbk.dev/issues"}>planned enhancements</a>.</p>
                    <p>Also, check out <Link href={"./stations"}>this map visualization of stations and their ridership counts in August 2022</Link>.</p>
                    <h3 id="qc">🚧 Data-quality issues 🚧</h3>
                    {MD(`
Several things changed in February 2021 (presumably as part of [the Lyft acquistion](https://www.lyft.com/blog/posts/lyft-becomes-americas-largest-bikeshare-service)):
- "Gender" information no longer provided (all rides labeled "unknown" starting February 2021)
- A new "Rideable Type" field was added, containing values \`docked_bike\` and \`electric_bike\` 🎉; however, it is mostly incorrect at present, and disabled above:
  - Prior to February 2021, the field is absent (even though e-citibikes were in widespread use before then)
  - Since February 2021, only a tiny number of rides are labeled \`electric_bike\` (122 in April 2021, 148 in May, 113 in June); this is certainly not accurate!
    - One possible explanation: [electric citibikes were launched in Jersey City and Hoboken around April 2021](https://www.hobokengirl.com/hoboken-jersey-city-citi-bike-share-program/); perhaps those bikes were part of a new fleet that show up as \`electric_bike\` in the data (while extant NYC e-citibikes don't).
    - These \`electric_bike\` rides showed up in the default ("NYC") data, not the "JC" data, but it could be all in flux; February through April 2021 were also updated when the May 2021 data release happened in early June.
- The "User Type" values changed ("Subscriber" → "member", "Customer" → "casual"); I'm using the former/old values here, they seem equivalent.
                    `)}
                    <div className={css.footer}>
                        Code: { icon(     'gh', 'https://github.com/neighbor-ryan/ctbk.dev#readme',    'GitHub logo') }
                        Data: { icon(     's3',         'https://ctbk.s3.amazonaws.com/index.html', 'Amazon S3 logo') }
                      Author: { icon('twitter',                  'https://twitter.com/RunsAsCoded',   'Twitter logo') }
                    </div>
                </div>
            </div>*/}
        </div>
    );
}
