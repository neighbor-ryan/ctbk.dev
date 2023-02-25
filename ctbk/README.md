# `ctbk` python library
CLI for generating [ctbk.dev] datasets (derived from Citi Bike public data in [`s3://tripdata`]).

- [Data flow](#data-flow)
- [Usage](#usage)

## Data flow <a id="data-flow"></a>

```mermaid
flowchart LR;
z["TripdataZips\ns3://tripdata"]
c["TripdataCsvs\ns3://ctbk/csvs"]
n["NormalizedMonths\ns3://ctbk/normalized/YYYYMM.parquet"]
agg_sc["AggregatedMonths(YYYYMM, 's', 'c')\ns3://ctbk/aggregated/s_c_YYYYMM.parquet"]
agg_sec["AggregatedMonths(YYYYMM, 'se', 'c')\ns3://ctbk/aggregated/se_c_YYYYMM.parquet"]
agg_ymrgtb["AggregatedMonths(YYYYMM, 'ymrgtb', 'cd')\ns3://ctbk/aggregated/ymrgtb_cd_YYYYMM.parquet"]
smh_in["StationMetaHists(YYYYMM, 'in')\ns3://ctbk/stations/meta_hists/in_YYYYMM.parquet"]
smh_il["StationMetaHists(YYYYMM, 'il')\ns3://ctbk/stations/meta_hists/il_YYYYMM.parquet"]
sm["StationModes\ns3://ctbk/aggregated/YYYYMM/stations.json"]
spj["StationPairJsons\ns3://ctbk/aggregated/YYYYMM/se_c.json"]

z --> c --> n 
n --> agg_sc
n --> agg_sec
n --> agg_ymrgtb
n --> smh_in
n --> smh_il
smh_in --> sm
smh_il --> sm
agg_sc --> sm
sm --> spj
agg_sec --> spj
```

### `TripdataZips` (a.k.a. `zip`s): public Citi Bike `.csv.zip` files
- Released as NYC and JC `.csv.zip` files at s3://tripdata
- See [s3://tripdata](https://tripdata.s3.amazonaws.com/index.html)

### `TripdataCsvs` (a.k.a. `csv`s): unzipped and gzipped CSVs
- Writes `<root>/ctbk/csvs/YYYYMM.csv`
- See also: [s3://ctbk/csvs](https://ctbk.s3.amazonaws.com/index.html#/csvs)

### `NormalizedMonths` (a.k.a. `norm`s): normalize `csv`s
- Merge regions (NYC, JC) for the same month, harmonize columns drop duplicate data, etc.
- Writes `<root>/ctbk/normalized/YYYYMM.parquet`
- See also: [s3://ctbk/normalized](https://ctbk.s3.amazonaws.com/index.html#/normalized)

### `AggregatedMonths` (a.k.a. `agg`s): compute histograms over each month's rides:
- Group by any of several \"aggregation keys\" ({year, month, day, hour, user type, bike
  type, start and end station, …})
- Produce any \"sum keys\" ({ride counts, duration in seconds})
- Writes `<root>/ctbk/aggregated/KEYS_YYYYMM.parquet`
- See also: [s3://ctbk/aggregated/*.parquet](https://ctbk.s3.amazonaws.com/index.html#/aggregated?p=8)

### `StationMetaHists` (a.k.a. `smh`s): compute station {id,name,lat/lng} histograms:
- Similar to `agg`s, but counts station {id,name,lat/lng} tuples that appear as each
  ride's start and end stations (whereas `agg`'s rows are 1:1 with rides)
- "agg_keys" can include id (i), name (n), and lat/lng (l); there are no "sum_keys"
  (only counting is supported)
- Writes `<root>/ctbk/stations/meta_hists/YYYYMM.parquet`
- See also: [s3://ctbk/stations/meta_hists](https://ctbk.s3.amazonaws.com/index.html#/stations/meta_hists)

### `StationModes` (a.k.a. `sm`s): canonical {id,name,lat/lng} info for each station:
- Computed from `StationMetaHist`s:
    - `name` is chosen as the "mode" (most commonly listed name for that station ID)
    - `lat/lng` is taken to be the mean of the lat/lngs reported for each ride's start
      and end station
- Writes `<root>/ctbk/aggregated/YYYYMM/stations.json`
- See also: [s3://ctbk/aggregated/YYYYMM/stations.json](https://ctbk.s3.amazonaws.com/index.html#/aggregated)

### `StationPairJsons` (a.k.a. `spj`s): counts of rides between each pair of stations:
- JSON formatted as `{ <start idx>: { <end idx>: <count> } }`
- `idx`s are based on order of appearance in `StationModes` / `stations.json` above
  (which is also sorted by station ID)
- Values are read from `AggregatedMonths(<ym>, 'se', 'c')`:
    - group by station start ("s") and end ("e"),
    - sum ride counts ("c")
- Writes `<root>/ctbk/aggregated/YYYYMM/se_c.json`
- See also: [s3://ctbk/stations/YYYYMM/se_c.json](https://ctbk.s3.amazonaws.com/index.html#/aggregated)

## Usage <a id="usage"></a>

<details><summary><code>ctbk</code></summary>

```
Usage: ctbk [OPTIONS] COMMAND [ARGS]...

  CLI for generating ctbk.dev datasets (derived from Citi Bike public data in `s3://`).
  ## Data flow
  ### `TripdataZips` (a.k.a. `zip`s): Public Citi Bike `.csv.zip` files
  - Released as NYC and JC `.csv.zip` files at s3://tripdata
  - See https://tripdata.s3.amazonaws.com/index.html
  ### `TripdataCsvs` (a.k.a. `csv`s): unzipped and gzipped CSVs
  - Writes `<root>/ctbk/csvs/YYYYMM.csv`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/csvs
  ### `NormalizedMonths` (a.k.a. `norm`s): normalize `csv`s
  - Merge regions (NYC, JC) for the same month, harmonize columns drop duplicate data, etc.
  - Writes `<root>/ctbk/normalized/YYYYMM.parquet`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/normalized
  ### `AggregatedMonths` (a.k.a. `agg`s): compute histograms over each month's rides:
  - Group by any of several "aggregation keys" ({year, month, day, hour, user type, bike
    type, start and end station, …})
  - Produce any "sum keys" ({ride counts, duration in seconds})
  - Writes `<root>/ctbk/aggregated/KEYS_YYYYMM.parquet`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/aggregated?p=8
  ### `StationMetaHists` (a.k.a. `smh`s): compute station {id,name,lat/lng} histograms:
  - Similar to `agg`s, but counts station {id,name,lat/lng} tuples that appear as each
    ride's start and end stations (whereas `agg`'s rows are 1:1 with rides)
  - "agg_keys" can include id (i), name (n), and lat/lng (l); there are no "sum_keys"
    (only counting is supported)
  - Writes `<root>/ctbk/stations/meta_hists/YYYYMM/KEYS.parquet`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/stations/meta_hists
  ### `StationModes` (a.k.a. `sm`s): canonical {id,name,lat/lng} info for each station:
  - Computed from `StationMetaHist`s:
    - `name` is chosen as the "mode" (most commonly listed name for that station ID)
    - `lat/lng` is taken to be the mean of the lat/lngs reported for each ride's start
      and end station
  - Writes `<root>/ctbk/aggregated/YYYYMM/stations.json`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/aggregated
  ### `StationPairJsons` (a.k.a. `spj`s): counts of rides between each pair of stations:
  - JSON formatted as `{ <start idx>: { <end idx>: <count> } }`
  - `idx`s are based on order of appearance in `StationModes` / `stations.json` above
    (which is also sorted by station ID)
  - Values are read from `AggregatedMonths(YYYYMM, 'se', 'c')`:
    - group by station start ("s") and end ("e"),
    - sum ride counts ("c")
  - Writes `<root>/ctbk/aggregated/YYYYMM/se_c.json`
  - See also: https://ctbk.s3.amazonaws.com/index.html#/aggregated

Options:
  -r, --read TEXT   Set "read" behavior for `HasRoot` subclasses, `<alias>=<value>` to set specific classes by
                    alias, just `<value>` to set a global default. `<value>`s are `memory`, `disk`, and their
                    aliases, indicating whether to return disk-round-tripped versions of newly-computed
                    datasets.
  -t, --root TEXT   Path- or URL-prefixes for `HasRoot` subclasses to write to and read from. `<alias>=<value>`
                    to set specific classes by alias, just `<value>` to set a global default. `<value>`s are
                    `memory`, `disk`, and their aliases, indicating whether to return disk-round-tripped
                    versions of newly-computed datasets.
  -w, --write TEXT  Set "write" behavior for `HasRoot` subclasses, `<alias>=<value>` to set specific classes by
                    alias, just `<value>` to set a global default. `<value>`s are `never`, `ifabsent`, `always`,
                    and their aliases, indicating how to handle each dataset type already existing on disk
                    (under its `root`) vs. not.
  --s3              Alias for `--root s3:/`, pointing all classes' "root" dirs at S3
  --help            Show this message and exit.

Commands:
  zip                 Read .csv.zip files from s3://tripdata
  csv                 Extract CSVs from "tripdata" .zip files.
  normalized          Normalize "tripdata" CSVs (combine regions for each...
  aggregated          Aggregate normalized ride entries by various...
  station-meta-hist   Aggregate station name, lat/lng info from ride...
  station-modes-json  Compute canonical station names, lat/lngs from...
  station-pairs-json  Write station-pair ride_counts keyed by...
  sampled-zip         Generate test data by downsampling tripdata...

```
</details>


<details><summary><code>ctbk zip</code></summary>

```
Usage: ctbk zip [OPTIONS] COMMAND [ARGS]...

  Read .csv.zip files from s3://tripdata

Options:
  -d, --dates TEXT
  -r, --region [NYC|JC]
  --help                 Show this message and exit.

Commands:
  urls  Print URLs for selected datasets

```
</details>


<details><summary><code>ctbk csv</code></summary>

```
Usage: ctbk csv [OPTIONS] COMMAND [ARGS]...

  Extract CSVs from "tripdata" .zip files. Writes to <root>/ctbk/csvs.

Options:
  -d, --dates TEXT
  -r, --region [NYC|JC]
  --help                 Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk normalized</code></summary>

```
Usage: ctbk normalized [OPTIONS] COMMAND [ARGS]...

  Normalize "tripdata" CSVs (combine regions for each month, harmonize column
  names, etc. Writes to <root>/ctbk/normalized/YYYYMM.parquet.

Options:
  -d, --dates TEXT
  --help            Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk aggregated</code></summary>

```
Usage: ctbk aggregated [OPTIONS] COMMAND [ARGS]...

  Aggregate normalized ride entries by various columns, summing ride counts or
  durations. Writes to <root>/ctbk/aggregated/KEYS_YYYYMM.parquet.

Options:
  -d, --dates TEXT
  --help            Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk station-meta-hist</code></summary>

```
Usage: ctbk station-meta-hist [OPTIONS] COMMAND [ARGS]...

  Aggregate station name, lat/lng info from ride start and end fields. Writes
  to <root>/ctbk/stations/meta_hists/KEYS_YYYYMM.parquet.

Options:
  -d, --dates TEXT
  --help            Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk station-modes-json</code></summary>

```
Usage: ctbk station-modes-json [OPTIONS] COMMAND [ARGS]...

  Compute canonical station names, lat/lngs from StationMetaHists. Writes to
  <root>/ctbk/aggregated/YYYYMM/stations.json.

Options:
  -d, --dates TEXT
  --help            Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk station-pairs-json</code></summary>

```
Usage: ctbk station-pairs-json [OPTIONS] COMMAND [ARGS]...

  Write station-pair ride_counts keyed by StationModes' JSON indices. Writes
  to <root>/ctbk/aggregated/YYYYMM/se_c.json.

Options:
  -d, --dates TEXT
  --help            Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>


<details><summary><code>ctbk sampled-zip</code></summary>

```
Usage: ctbk sampled-zip [OPTIONS] COMMAND [ARGS]...

  Generate test data by downsampling tripdata .csv.zip files. Writes to
  <root>/ctbk/sampled/tripdata.

Options:
  -d, --dates TEXT
  -r, --region [NYC|JC]
  --help                 Show this message and exit.

Commands:
  urls    Print URLs for selected datasets
  create  Create selected datasets
  dag     Save and `open` a graph visualization of the datasets to be...

```
</details>



[`s3://tripdata`]: https://tripdata.s3.amazonaws.com/index.html
[ctbk.dev]: https://ctbk.dev
