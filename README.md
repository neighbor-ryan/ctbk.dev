# [ctbk.dev](https://ctbk.dev/)
Citi Bike Dashboard

- [ctbk/](ctbk) contains a Python library and CLI (`ctbk`) that derives various datasets from Citi Bike's public data at [`s3://tripdata`]
- [s3://ctbk] contains cleaned, public data output by [`ctbk`]
- [www/](www) contains the static web app served at [ctbk.dev].
- GitHub Actions in [.githug/workflows](.github/workflows):
  - poll for new Citi Bike data at the start of each month
  - compute new derived data when found, and
  - build and publish the [ctbk.dev] website

## Screenshots <a id="screenshots"></a>
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

## Implementation
- [ctbk/](./ctbk) contains a Python library and CLI (`ctbk`) that derives various datasets from Citi Bike's public data at [`s3://tripdata`]
- [s3://ctbk] contains cleaned, public data output by `ctbk`
- [www/](./www) contains the static web app served at [ctbk.dev].
- GitHub Actions in [.githug/workflows](.github/workflows):
  - poll for new Citi Bike data at the start of each month
  - compute new derived data when found, and
  - build and publish the [ctbk.dev] website

## Prior Art
[Many][ckran-20210305] [great][toddschneider-20160113] [analyses][jc-analysis-2017] [of][jc-analysis-2018] [Citi Bbike][datastudio-analysis] [data][cl2871-analysis] [have][tableau #citibike] [been][coursera citibike viz course] [done][juanjocarin analysis] over the years!

My hope is that this dashboard will:
- stay up to date automatically
- support enough exploratory data analysis and visualization to answer most q's a layperson might have about system-wide stats

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
[s3://ctbk]: https://ctbk.s3.amazonaws.com/index.html

[github actions]: https://github.com/neighbor-ryan/ctbk.dev/actions
[github issues]: https://github.com/neighbor-ryan/ctbk.dev/issues
[github new issue]: https://github.com/neighbor-ryan/ctbk.dev/issues/new

[`ctbk`]: ctbk
[ctbk.dev]: https://ctbk.dev/
[ctbk gender pct plot]: https://ctbk.dev/?y=m&s=g&pct=&g=mf&d=1406-2101
[ctbk.dev/stations]: https://ctbk.dev/stations?ll=40.733_-74.036&z=14&ss=HB102
[ctbk nj plot]: https://ctbk.dev/?r=jh
[ctbk user type pct plot]: https://ctbk.dev/?s=u&pct=
[ctbk ebike minutes plot]: https://ctbk.dev?y=m&s=b&rt=ce
