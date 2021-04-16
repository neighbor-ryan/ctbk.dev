# Yet Another Citibike Dashboard
[www.ctbk.dev](https://www.ctbk.dev/):

[![Screenshot of dashboard; per-month ride counts going back 8 years, broken out by gender](https://user-images.githubusercontent.com/465045/115073461-5ca59200-9ec6-11eb-9d06-570fd2e0cc17.png)](https://www.ctbk.dev/)

- [auto-updates with new data each month](#auto-update)
- [powered by cleaned+improved public data](#cleaned-data)
- Interactive! Filter by:
  - user type (annual "subscriber" vs. daily "customer")
  - gender (male, female, or other/unspecified)
  - region (NYC or JC)
  - date range (at monthly granularity, back to system launch in June 2013)
- Stack/Group by:
  - gender
  - user type

## Cleaned, public data <a id="cleaned-data"></a>
I fixed some rough edges in [Citibike's published data][citibike system data] and published the results to [the `ctbk` Amazon S3 bucket][`s3://ctbk`].

Some issues that [`s3://ctbk`] mitigates:
- Original CSV's are converted to [Parquet] (a compressed format that is also aware of columns' types, which can save some headache; also works around other issues like inconsistent `\n` vs. `\r\n` line endings)
- Original CSV's come in `.zip` files that typically only contain one `.csv` file. However, that CSV is often inconsistently named, and sometimes other files are also mistakenly included in the `.zip` (e.g. `.DS_Store` macOS metadata from the machine where the `.zip` was created)
- Column-name inconsistencies across months (e.g. `User Type` vs. `usertype`) are harmonized

## Automatic Updating <a id="auto-update"></a>
Every day, [a GitHub Action runs in this repo](https://github.com/neighbor-ryan/citibike/actions) and checks `s3://tripdata` for a new month's worth of official data. If new data is found, it is cleaned, converted to `.parquet`, and uploaded to `s3://ctbk`.

Additionally, some aggregated/summary statistics are updated, which the dashboard at https://www.ctbk.dev/ reads, meaning it should stay up to date as new data is published.

At the time of this writing, this process has run successfully once, [adding March 2021 data to the dashboard on April 8, 2021][202103 GHA]:
```
…
Found s3://ctbk/202101-citibike-tripdata.parquet; skipping
Found s3://ctbk/202102-citibike-tripdata.parquet; skipping
Wrote s3://ctbk/202103-citibike-tripdata.parquet
Found s3://ctbk/JC-201509-citibike-tripdata.parquet; skipping
Found s3://ctbk/JC-201510-citibike-tripdata.parquet; skipping
…
Found s3://ctbk/JC-202101-citibike-tripdata.parquet; skipping
Found s3://ctbk/JC-202102-citibike-tripdata.parquet; skipping
Wrote s3://ctbk/JC-202103-citibike-tripdata.parquet
…
Found s3://ctbk/JC-202101-citibike-tripdata.parquet; skipping
Found s3://ctbk/JC-202102-citibike-tripdata.parquet; skipping
JC-202103-citibike-tripdata.csv.zip: zip names: ['JC-202103-citibike-tripdata.csv', '__MACOSX/._JC-202103-citibike-tripdata.csv']
…
Found s3://ctbk/202101-citibike-tripdata.parquet; skipping
Found s3://ctbk/202102-citibike-tripdata.parquet; skipping
202103-citibike-tripdata.csv.zip: zip names: ['202103-citibike-tripdata.csv', '__MACOSX/._202103-citibike-tripdata.csv']
```

## Prior Art
[Many][ckran-20210305] [great][toddschneider-20160113] [analyses][jc-analysis-2017] [of][jc-analysis-2018] [Citibike][datastudio-analysis] [data][cl2871-analysis] [have][tableau #citibike] [been][coursera citibike viz course] [done][juanjocarin analysis] over the years. However, I've generally found them lacking in 2 ways:
- They're typically stale. They were run once and published; I find them via Google years later, and want to see the same analysis on the latest data.
- They show a few specific, static plots, but I want to ask slightly different questions of the data.

My hope is that this dashboard will solve both of these issues, by:
- ≈always staying up to date with the latest published data
- providing 5-10 orthogonal, common-sense toggles that let you easily answer any of a large number of high-level questions about the system-level data

## Feedback / Contributing
Feel free to [file an issue here](https://github.com/neighbor-ryan/citibike/issues) with any comments, bug reports, or feedback!

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
[citibike s3 index]: https://s3.amazonaws.com/tripdata/index.html
[`s3://ctbk`]: https://s3.amazonaws.com/ctbk/index.html
[Parquet]: https://parquet.apache.org/
[202103 GHA]: https://github.com/neighbor-ryan/citibike/runs/2304544335?check_suite_focus=true#step:6:104
