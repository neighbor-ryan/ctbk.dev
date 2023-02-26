# [ctbk.dev](https://ctbk.dev/): Citi Bike Dashboard

[![Screenshot of dashboard; per-month ride counts going back to June 2013, with a 12mo rolling avg showing mostly steady growth](www/public/screenshots/ctbk-rides.png)][ctbk.dev]

### Station/Ridership Map
[![Map of Citi Bike stations, Hoboken NJ Transit Terminal selected, showing destinations for rides beginning there](www/public/screenshots/ctbk-stations.png)][ctbk.dev/stations]
[ctbk.dev/stations]

### JC & Hoboken Only
[![Screenshot of dashboard; per-month ride counts for Jersey City and Hoboken only, going back to June 2013, with a 12mo rolling avg showing mostly steady growth](www/public/screenshots/ctbk-nj.png)][ctbk nj plot]
[ctbk.dev/?r=jh][ctbk nj plot]

### Ride Minute %'s by Gender
[![](www/public/screenshots/ctbk-ride-minutes-by-gender.png)][ctbk gender pct plot]
[ctbk.dev?d=1406-2101&g=mf&pct&s=g&y=m][ctbk gender pct plot]; Jun 2014 - January 2021, the window where 12mo rolling avgs are possible

### Ride %'s by User Type
[![](www/public/screenshots/ctbk-rides-by-user.png)][ctbk user type pct plot]
[ctbk.dev/?s=u&pct=][ctbk user type pct plot]

### Total Classic / E-bike Ride Minutes
[![](www/public/screenshots/ctbk-ebike-minutes.png)][ctbk ebike minutes plot]
[ctbk.dev?y=m&s=b&rt=ce][ctbk ebike minutes plot]

Other notes:
- [Auto-updates with new data each month](#auto-update)
- [Powered by cleaned, public data (derived from the official Citibike data)](#cleaned-data)
- Interactive! Filter/Stack by:
  - user type (annual "subscriber" vs. daily "customer")
  - gender (male, female, other/unspecified; historical data up to Feb 2021 only)
  - region (NYC and/or JC)
  - date range (at monthly granularity, back to system launch in June 2013)
- URL syncs with plot controls, for ease of linking to specific views, e.g.:

## Cleaned, public data <a id="cleaned-data"></a>
I fixed some rough edges in [Citibike's published data][citibike system data] and published the results to [the `ctbk` Amazon S3 bucket][`s3://ctbk`].

Some issues that [`s3://ctbk`] mitigates:
- Convert original CSV's to [Parquet]
  - compressed, column-type-aware format saves some headache
  - works around issues like inconsistent `\n` vs. `\r\n` line endings
- Unzip+Normalize `.zip`s containing CSV's (which are sometimes inconsistently-named or contain erroneous extra files like `.DS_Store` macOS metadata)
- Harmonize column names across months (e.g. `User Type` vs. `usertype`)

## Automatic Updating <a id="auto-update"></a>
On each of the first few days of each month, [a GitHub Action in this repo][github actions]:
- checks [`s3://tripdata`] for a new month of official data
- if found, the new data gets cleaned, converted to [`.parquet`][Parquet], and uploaded to [`s3://ctbk`].

Additionally, aggregated statistics are updated, which the [ctbk.dev] app reads, meaning it should stay up to date as new data is published.

I sometimes break this in the process of adding features and improving things, but it has mostly run automatically each month since ≈2020.

## Prior Art
[Many][ckran-20210305] [great][toddschneider-20160113] [analyses][jc-analysis-2017] [of][jc-analysis-2018] [Citibike][datastudio-analysis] [data][cl2871-analysis] [have][tableau #citibike] [been][coursera citibike viz course] [done][juanjocarin analysis] over the years. However, I've generally found them lacking in 2 ways:
- They were run once and published, and are now stale. I find them via Google, years later, and want to see the same analysis on the latest data.
- They show a few specific, static plots, but I want to ask slightly different questions of the data, or slice it another way.

My hope is that this dashboard will solve both of these issues, by:
- always staying up to date with the latest published data
- providing 5-10 orthogonal, common-sense toggles that let you easily answer a large number of basic system-level questions

## Implementation
- [ctbk/](./ctbk) contains a Python library that derives various datasets from Citi Bike's public data at [`s3://tripdata`]
- [www/](./www) contains the static web app served at [ctbk.dev].

## Feedback / Contributing
Feel free to [file an issue here][github new issue] with any comments, bug reports, or feedback!

[ckran-20210305]: https://towardsdatascience.com/exploring-the-effects-of-the-pandemic-on-nyc-bike-share-usage-ab79f67ac2df
[toddschneider-20160113]: https://toddwschneider.com/posts/a-tale-of-twenty-two-million-citi-bikes-analyzing-the-nyc-bike-share-system/
[jc-analysis-2017]: https://www.bikejc.org/resources/citibikejc-2017
[jc-analysis-2018]: https://www.bikejc.org/citi-bike-usage-jersey-city-2018
[datastudio-analysis]: https://datastudio.google.com/u/0/reporting/a6fc910f-b100-4ac5-a72b-2fa35880f149/page/SKniB
[cl2871-analysis]: https://github.com/cl2871/citibike
[tableau #citibike]: https://public.tableau.com/en-gb/search/all/%23CitiBike
[coursera citibike viz course]: https://www.coursera.org/projects/visualizing-citibike-trips-tableau
[juanjocarin analysis]: http://juanjocarin.github.io/Citibike-viz/

[citibike system data]: https://www.citibikenyc.com/system-data
[Parquet]: https://parquet.apache.org/

[`s3://tripdata`]: https://s3.amazonaws.com/tripdata/index.html
[`s3://ctbk`]: https://ctbk.s3.amazonaws.com/index.html

[github actions]: https://github.com/neighbor-ryan/ctbk.dev/actions
[github issues]: https://github.com/neighbor-ryan/ctbk.dev/issues
[github new issue]: https://github.com/neighbor-ryan/ctbk.dev/issues/new

[ctbk.dev]: https://ctbk.dev/
[ctbk gender pct plot]: https://ctbk.dev/?y=m&s=g&pct=&g=mf&d=1406-2101
[ctbk.dev/stations]: https://ctbk.dev/stations?ll=40.733_-74.036&z=14&ss=HB102
[ctbk nj plot]: https://ctbk.dev/?r=jh
[ctbk user type pct plot]: https://ctbk.dev/?s=u&pct=
[ctbk ebike minutes plot]: https://ctbk.dev?y=m&s=b&rt=ce
