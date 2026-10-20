"""Bounded-memory city routing and precomputed whole-network street volumes."""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from gzip import compress, decompress
from hashlib import file_digest, md5
from json import dumps, loads
from math import isclose
from os import environ
from pathlib import Path
from re import fullmatch
from resource import RUSAGE_CHILDREN, RUSAGE_SELF, getrusage
from sqlite3 import connect
from subprocess import Popen, run
from time import monotonic
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from botocore.exceptions import ClientError
from click import Path as ClickPath, UsageError, option
from utz import err

from .flow_routes import encode_segments, flow_routes, segment_topology, wait
from .flow_routes_nj import OSRM_IMAGE, checkpoint_context, route_record
from .gbfs_cli import _r2_client
from .ride_animation import decode_day

PREFIX = 'routes-city/v1'


def city_population(population: dict, limit: int = 0) -> dict[str, list[dict]]:
    """Keep the census vocabulary; sample evenly across sorted directed pairs."""
    if population['version'] != 1 or limit < 0:
        raise ValueError('unsupported population version or negative pair limit')
    stations = population['stations']
    if len({s['id'] for s in stations}) != len(stations):
        raise ValueError('duplicate population station IDs')
    pairs = population['pairs']
    if len({(a, b) for a, b, _ in pairs}) != len(pairs):
        raise ValueError('duplicate population pair')
    if any(a == b or min(a, b) < 0 or max(a, b) >= len(stations) or count <= 0 for a, b, count in pairs):
        raise ValueError('invalid population pair')
    if limit and limit < len(pairs):
        pairs = sorted(pairs, key=lambda p: (stations[p[0]]['id'], stations[p[1]]['id']))
        pairs = [pairs[i * len(pairs) // limit] for i in range(limit)]
    origins = defaultdict(list)
    for a, b, count in pairs:
        origins[stations[a]['id']].append({'source': stations[a], 'destination': stations[b], 'count': count})
    return {sid: sorted(pairs, key=lambda p: p['destination']['id']) for sid, pairs in sorted(origins.items())}


def add_volumes(pair: dict, count: int, volumes: Counter[int]) -> bool:
    """Assign expected alternatives, preserving both directed traversals of a block."""
    if not pair['routes']:
        return False
    for route in pair['routes']:
        for code in route['segments']:
            if not code:
                raise ValueError('zero is not a signed segment ID')
            volumes[code] += count * route['weight']
    return True


def object_blob(data: dict | list) -> bytes:
    return compress(dumps(data, sort_keys=True, separators=(',', ':')).encode() + b'\n', mtime=0)


class RouteStore:
    def __init__(self, prefix: str, rw: bool = True):
        if not prefix or prefix.startswith('/') or '..' in Path(prefix).parts:
            raise UsageError('prefix must be a relative bucket path')
        self.prefix = prefix.rstrip('/')
        self.r2, self.bucket = _r2_client(rw=rw)

    def get(self, key: str) -> bytes:
        return self.r2.get_object(Bucket=self.bucket, Key=f'{self.prefix}/{key}')['Body'].read()

    def optional(self, key: str) -> bytes | None:
        try:
            return self.get(key)
        except ClientError as error:
            if error.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise

    def put(self, key: str, body: bytes, mutable: bool = False) -> None:
        self.r2.put_object(Bucket=self.bucket, Key=f'{self.prefix}/{key}', Body=body, ContentType='application/json' if mutable else 'application/gzip', CacheControl='public, max-age=60' if mutable else 'public, max-age=31536000, immutable')

    def publish(self, data: dict) -> dict:
        blob = object_blob(data)
        key = f'objects/{md5(blob).hexdigest()}.json.gz'
        self.put(key, blob)
        return {'key': key, 'bytes': len(blob)}

    def packed(self, meta: dict) -> dict:
        blob = self.get(meta['key'])
        if len(blob) != meta['bytes'] or md5(blob).hexdigest() != Path(meta['key']).name.split('.')[0]:
            raise ValueError('route object hash or size disagrees with index')
        return loads(decompress(blob))


@flow_routes.command('city')
@option('-i', '--input-prefix', default='routes-census/v1', show_default=True)
@option('-k', '--prefix', default=PREFIX, show_default=True)
@option('-n', '--limit', default=0, show_default=True, help='Evenly sampled census pairs to benchmark; zero builds the full census.')
@option('-o', '--output', type=ClickPath(path_type=Path), default=Path('tmp/routes-city'))
@option('-p', '--snapshot', default='20261012', show_default=True)
def city(
    input_prefix: str,
    prefix: str,
    limit: int,
    output: Path,
    snapshot: str,
) -> None:
    import osmium
    if not fullmatch(r'\d{8}', snapshot) or limit < 0:
        raise UsageError('use YYYYMMDD snapshot and a nonnegative pair limit')
    if limit and prefix == PREFIX:
        raise UsageError('benchmark builds require a separate --prefix')
    started = monotonic()
    store, census = RouteStore(prefix), RouteStore(input_prefix, rw=False)
    census_index = loads(census.get('index.json'))
    population = census.packed(census_index)
    origins = city_population(population, limit)
    if not origins:
        raise ValueError('empty route population')
    stations = population['stations']
    counts = {(p['source']['id'], p['destination']['id']): p['count'] for pairs in origins.values() for p in pairs}
    population_md5 = md5(object_blob({'stations': stations, 'pairs': sorted((a, b, count) for (a, b), count in counts.items())})).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    pbf = output / 'NewYork.osm.pbf'
    osm_store = RouteStore('routes-nj/v1', rw=False)
    osm_key = f'inputs/{snapshot}/NewYork.osm.pbf'
    # The existing NJ snapshot is immutable; never download a changing extract here.
    osm_store.r2.download_file(osm_store.bucket, f'{osm_store.prefix}/{osm_key}', str(pbf))
    with pbf.open('rb') as source:
        osm_md5 = file_digest(source, 'md5').hexdigest()
    context = md5(dumps([checkpoint_context(stations, osm_md5), population_md5], sort_keys=True).encode()).hexdigest()
    graph_path = pbf.with_suffix('').with_suffix('.osrm')
    threads = environ.get('ROUTE_THREADS', '4')
    for executable, args in [('osrm-extract', ['-p', '/opt/bicycle.lua', str(pbf)]), ('osrm-partition', [str(graph_path)]), ('osrm-customize', [str(graph_path)])]:
        run([executable, '--threads', threads, *args], check=True)
    preprocessing_seconds = monotonic() - started
    router = Popen(['osrm-routed', '--algorithm', 'mld', '--threads', threads, '--port', '51991', str(graph_path)])
    cache = output / context
    cache.mkdir(exist_ok=True)
    routed_pairs = resumed = 0
    routing_started = monotonic()
    try:
        wait.callback('http://127.0.0.1:51991')
        for number, (sid, pairs) in enumerate(origins.items()):
            checkpoint = f'checkpoints/{context}/{sid}.json.gz'
            cached = store.optional(checkpoint)
            if cached is not None:
                records = loads(decompress(cached))
                if [(p['source'], p['destination']) for p in records] != [(sid, p['destination']['id']) for p in pairs]:
                    raise ValueError('checkpoint coverage disagrees with population context')
                resumed += 1
            else:
                records = []
                for pair in pairs:
                    source, destination = pair['source'], pair['destination']
                    coords = f'{source["lng"]},{source["lat"]};{destination["lng"]},{destination["lat"]}'
                    url = 'http://127.0.0.1:51991/route/v1/bike/' + coords + '?' + urlencode({'alternatives': 3, 'annotations': 'nodes', 'geometries': 'geojson', 'overview': 'false'})
                    try:
                        with urlopen(url, timeout=60) as response:
                            result = loads(response.read())
                    except HTTPError as error:
                        result = loads(error.read())
                        if result.get('code') not in ('NoRoute', 'NoSegment'):
                            raise
                    records.append(route_record(source, destination, result))
                cached = object_blob(records)
                store.put(checkpoint, cached)
            (cache / f'{sid}.json.gz').write_bytes(cached)
            routed_pairs += len(records)
            err(f'City origin {number + 1}/{len(origins)}: {sid}; {routed_pairs} pairs; reused {resumed}; elapsed {monotonic() - routing_started:.1f}s')
    finally:
        router.terminate()
        router.wait(timeout=30)
    routing_seconds = monotonic() - routing_started

    def records_for(sid: str) -> list[dict]:
        return loads(decompress((cache / f'{sid}.json.gz').read_bytes()))

    # Two passes over per-origin checkpoints bound memory by topology + one origin.
    paths = (route['nodes'] for sid in origins for pair in records_for(sid) for route in pair['routes'])
    chains, edges = segment_topology(paths)
    needed = {node for chain in chains for node in chain}
    points = {}
    class Nodes(osmium.SimpleHandler):
        def node(self, node: osmium.osm.Node) -> None:
            if node.id in needed:
                points[node.id] = [node.location.lon, node.location.lat]
    Nodes().apply_file(str(pbf))
    if set(points) != needed:
        raise ValueError('routing nodes absent from the OSM snapshot')
    graph_meta = store.publish({'version': 1, 'segments': [{'path': [points[n] for n in chain]} for chain in chains]})
    encoded_origins = {}
    destination_ids = set()
    destination_db = connect(output / 'destination-index.sqlite')
    destination_db.execute('DROP TABLE IF EXISTS pairs')
    destination_db.execute('CREATE TABLE pairs (destination TEXT, source TEXT, record TEXT, PRIMARY KEY(destination, source))')
    fallbacks, alternatives, routed, routed_rides, census_volumes = Counter(), 0, 0, 0, Counter()
    for sid in origins:
        records = records_for(sid)
        for pair in records:
            for route in pair['routes']:
                route['segments'] = encode_segments(route.pop('nodes'), edges)
                alternatives += 1
            count = counts[pair['source'], pair['destination']]
            if add_volumes(pair, count, census_volumes):
                routed += 1
                routed_rides += count
            else:
                fallbacks[pair['fallback']] += 1
            destination_ids.add(pair['destination'])
            destination_db.execute('INSERT INTO pairs VALUES (?, ?, ?)', (pair['destination'], sid, dumps(pair, sort_keys=True, separators=(',', ':'))))
        encoded_origins[sid] = store.publish({'version': 1, 'pairs': records})
        destination_db.commit()
    encoded_destinations = {}
    for sid in sorted(destination_ids):
        records = [loads(row[0]) for row in destination_db.execute('SELECT record FROM pairs WHERE destination = ? ORDER BY source', (sid,))]
        encoded_destinations[sid] = store.publish({'version': 1, 'pairs': records})
    destination_db.close()
    system = store.publish({'version': 1, 'context': context, 'graph': graph_meta, 'volumes': sorted(census_volumes.items()), 'dateRange': population['dateRange'], 'selectedRides': sum(counts.values()), 'routedRides': routed_rides})
    summary = {
        'version': 1, 'scope': 'observed directed census pairs', 'population': census_index['key'], 'populationPrefix': input_prefix,
        'dateRange': population['dateRange'], 'minimum': population['minimum'], 'limit': limit,
        'stations': stations, 'pairs': len(counts), 'routedPairs': routed,
        'fallbacks': dict(sorted(fallbacks.items())), 'segments': len(chains),
        'vertices': sum(map(len, chains)), 'alternatives': alternatives,
        'graph': graph_meta, 'origins': encoded_origins, 'destinations': encoded_destinations,
        'system': system, 'context': context, 'osmMd5': osm_md5, 'engine': OSRM_IMAGE,
        'selectedRides': sum(counts.values()), 'routedRides': routed_rides, 'nonRoundTripRides': census_index['nonRoundTripRides'],
        'bytes': graph_meta['bytes'] + sum(m['bytes'] for m in encoded_origins.values()) + sum(m['bytes'] for m in encoded_destinations.values()) + system['bytes'],
        'timings': {'preprocessingSeconds': preprocessing_seconds, 'routingSeconds': routing_seconds, 'totalSeconds': monotonic() - started, 'resumedOrigins': resumed},
        'resources': {'pythonPeakRssKiB': getrusage(RUSAGE_SELF).ru_maxrss, 'childPeakRssKiB': getrusage(RUSAGE_CHILDREN).ru_maxrss},
    }
    (output / 'summary.json').write_text(dumps(summary, indent=2, sort_keys=True) + '\n')
    store.put('index.json', dumps(summary, sort_keys=True).encode() + b'\n', mutable=True)
    print(dumps({k: v for k, v in summary.items() if k not in ('stations', 'origins', 'destinations')}, indent=2, sort_keys=True))


def validate_catalog_pair(pair: dict, segments: list[dict]) -> None:
    routes = pair['routes']
    if bool(routes) == bool(pair['fallback']):
        raise ValueError('pair must have routes or an explicit fallback')
    if routes and not isclose(sum(r['weight'] for r in routes), 1, abs_tol=1e-12):
        raise ValueError('route weights do not sum to one')
    for route in routes:
        if route['weight'] <= 0 or route['distance'] <= 0 or not route['segments']:
            raise ValueError('invalid route weight, distance or segment list')
        previous = None
        for code in route['segments']:
            if not isinstance(code, int) or not code or abs(code) > len(segments):
                raise ValueError('invalid signed segment ID')
            points = segments[abs(code) - 1]['path']
            start, end = (points[0], points[-1]) if code > 0 else (points[-1], points[0])
            if previous is not None and previous != start:
                raise ValueError('route segment chain is discontinuous')
            previous = end


@flow_routes.command('check-city')
@option('-k', '--prefix', default=PREFIX, show_default=True)
@option('-o', '--output', type=ClickPath(path_type=Path), default=Path('tmp/routes-city-audit.json'))
def check_city(prefix: str, output: Path) -> None:
    store = RouteStore(prefix, rw=False)
    index = loads(store.get('index.json'))
    if index['version'] != 1:
        raise ValueError('unsupported city catalog')
    census = RouteStore(index['populationPrefix'], rw=False)
    raw = census.get(index['population'])
    if md5(raw).hexdigest() != Path(index['population']).name.split('.')[0]:
        raise ValueError('population hash disagrees with city catalog')
    population = city_population(loads(decompress(raw)), index['limit'])
    segments = store.packed(index['graph'])['segments']
    if len(segments) != index['segments']:
        raise ValueError('graph disagrees with catalog index')
    expected = {(p['source']['id'], p['destination']['id']): p['count'] for pairs in population.values() for p in pairs}
    fingerprints = {}
    counts = Counter(pairs=0, routedPairs=0, routedRides=0, alternatives=0)
    fallbacks, volumes = Counter(), Counter()
    if set(index['origins']) != set(population):
        raise ValueError('origin catalog disagrees with census population')
    for sid, meta in index['origins'].items():
        catalog = store.packed(meta)
        keys = [(p['source'], p['destination']) for p in catalog['pairs']]
        wanted = [(sid, p['destination']['id']) for p in population[sid]]
        if catalog['version'] != 1 or keys != wanted:
            raise ValueError('origin coverage disagrees with census population')
        for pair, key in zip(catalog['pairs'], keys):
            validate_catalog_pair(pair, segments)
            fingerprints[key] = md5(object_blob(pair)).hexdigest()
            counts['pairs'] += 1
            counts['alternatives'] += len(pair['routes'])
            if add_volumes(pair, expected[key], volumes):
                counts['routedPairs'] += 1
                counts['routedRides'] += expected[key]
            else:
                fallbacks[pair['fallback']] += 1
    if set(index['destinations']) != {b for a, b in expected}:
        raise ValueError('destination catalog disagrees with census population')
    for sid, meta in index['destinations'].items():
        catalog = store.packed(meta)
        for pair in catalog['pairs']:
            key = pair['source'], pair['destination']
            if pair['destination'] != sid or fingerprints.pop(key, None) != md5(object_blob(pair)).hexdigest():
                raise ValueError('origin and destination catalogs disagree')
    if fingerprints:
        raise ValueError('destination catalog is incomplete')
    actual = dict(counts) | {'fallbacks': dict(sorted(fallbacks.items()))}
    if actual != {k: index[k] for k in actual}:
        raise ValueError('catalog accounting disagrees with index')
    system = store.packed(index['system'])
    if system['context'] != index['context'] or system['graph'] != index['graph'] or system['volumes'] != [list(p) for p in sorted(volumes.items())]:
        raise ValueError('census flow volumes disagree with route assignment')
    report = actual | {'context': index['context'], 'segments': len(segments), 'bytes': index['bytes'], 'timings': index['timings'], 'resources': index['resources']}
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(dumps(report, indent=2, sort_keys=True) + '\n')
    print(dumps(report, indent=2, sort_keys=True))


@flow_routes.command('system')
@option('-d', '--date', 'ym', required=True, help='YYYYMM published animation month.')
@option('-i', '--input-prefix', default='rides-anim-candidate/v1', show_default=True)
@option('-k', '--prefix', default='flows/v1', show_default=True)
@option('-r', '--routes-prefix', default=PREFIX, show_default=True)
def system(
    ym: str,
    input_prefix: str,
    prefix: str,
    routes_prefix: str,
) -> None:
    from calendar import monthrange
    if not fullmatch(r'\d{6}', ym):
        raise UsageError('use YYYYMM')
    store, routes, animation = RouteStore(prefix), RouteStore(routes_prefix, rw=False), RouteStore(input_prefix, rw=False)
    index, ride_index = loads(routes.get('index.json')), loads(animation.get('index.json'))
    days = [f'{ym[:4]}-{ym[4:]}-{day:02d}' for day in range(1, monthrange(int(ym[:4]), int(ym[4:]))[1] + 1)]
    station_keys = {ride_index['days'][day]['stations'] for day in days}
    if len(station_keys) != 1:
        raise ValueError('system month requires one animation vocabulary')
    station_key, = station_keys
    stations = loads(animation.get(station_key))
    counts, payloads = Counter(), []
    for day in days:
        meta = ride_index['days'][day]
        blob = animation.get(meta['key'])
        if len(blob) != meta['bytes'] or md5(blob).hexdigest() != meta['md5']:
            raise ValueError('animation payload hash or size disagrees with index')
        nstations, carry, rides = decode_day(decompress(blob))
        if nstations != len(stations) or carry != meta['carry'] or len(rides) - carry != meta['rides']:
            raise ValueError('animation payload accounting disagrees with index')
        counts.update((stations[r.source]['id'], stations[r.destination]['id']) for r in rides[carry:])
        payloads.append(meta['md5'])
    volumes, routed, fallback = Counter(), 0, 0
    by_origin = defaultdict(dict)
    round_trips = 0
    for (a, b), count in counts.items():
        if a == b:
            round_trips += count
        else:
            by_origin[a][b] = count
    for sid, destinations in sorted(by_origin.items()):
        meta = index['origins'].get(sid)
        pairs = {(p['destination']): p for p in routes.packed(meta)['pairs']} if meta else {}
        for destination, count in destinations.items():
            pair = pairs.get(destination)
            if pair and add_volumes(pair, count, volumes):
                routed += count
            else:
                fallback += count
    encoded = sum(counts.values())
    if encoded != ride_index['months'][ym]['encodedRides'] or encoded != round_trips + routed + fallback:
        raise ValueError('system-flow accounting disagrees with published month')
    data = {'version': 1, 'month': ym, 'context': index['context'], 'graph': index['graph'], 'routesPrefix': routes_prefix, 'volumes': sorted(volumes.items()), 'sourceRides': ride_index['months'][ym]['sourceRides'], 'encodedRides': encoded, 'routedRides': routed, 'fallbackRides': fallback, 'roundTrips': round_trips, 'provisional': not ride_index['months'][ym]['final'], 'animationObjects': payloads}
    meta = store.publish(data)
    previous = store.optional('index.json')
    catalog = loads(previous) if previous else {'version': 1, 'months': {}}
    catalog['months'][ym] = meta
    store.put('index.json', dumps(catalog, sort_keys=True).encode() + b'\n', mutable=True)
    print(dumps({k: v for k, v in data.items() if k not in ('volumes', 'animationObjects')} | meta, indent=2, sort_keys=True))


@flow_routes.command('submit-city')
@option('-k', '--prefix', default=PREFIX, show_default=True)
@option('-m', '--memory', default=16384, type=int, show_default=True)
@option('-n', '--limit', default=10000, type=int, show_default=True)
@option('-N', '--dry-run', is_flag=True)
@option('-s', '--image', required=True)
def submit_city(
    prefix: str,
    memory: int,
    limit: int,
    dry_run: bool,
    image: str,
) -> None:
    import boto3
    if not fullmatch(r'688066488567\.dkr\.ecr\.us-east-1\.amazonaws\.com/ctbk-reproc:routes-[a-f0-9]{8,40}', image):
        raise UsageError('use a pinned HCCS routing image')
    if memory not in (8192, 16384, 30720) or limit < 0:
        raise UsageError('4-vCPU jobs support 8192, 16384 or 30720 MiB here; limit must be nonnegative')
    if limit and prefix == PREFIX:
        raise UsageError('benchmark builds require a separate --prefix')
    command = ['ctbk', 'flow-routes', 'city', '-k', prefix, '-n', str(limit)]
    plan = {'queue': 'ctbk-reproc', 'image': image, 'architecture': 'X86_64', 'vcpus': 4, 'memory': memory, 'command': command}
    if dry_run:
        print(dumps(plan, indent=2))
        return
    if boto3.client('sts').get_caller_identity()['Account'] != '688066488567':
        raise UsageError('submit requires the HCCS AWS account')
    batch = boto3.client('batch', region_name='us-east-1')
    latest = max(batch.describe_job_definitions(jobDefinitionName='ctbk-reproc', status='ACTIVE')['jobDefinitions'], key=lambda d: d['revision'])
    props = deepcopy(latest['containerProperties'])
    props.update(image=image, command=command, runtimePlatform={'cpuArchitecture': 'X86_64', 'operatingSystemFamily': 'LINUX'}, resourceRequirements=[{'type': 'VCPU', 'value': '4'}, {'type': 'MEMORY', 'value': str(memory)}])
    names = {'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY'}
    props['secrets'] = [s for s in props.get('secrets', []) if s['name'] in names]
    if {s['name'] for s in props['secrets']} != names:
        raise UsageError('base definition must bind the R2 pair from Secrets Manager')
    props['environment'] = [{'name': 'CLOUDFLARE_ACCOUNT_ID', 'value': '2363642879f18d37d52dca114059937e'}]
    definition = batch.register_job_definition(jobDefinitionName='ctbk-reproc-routes-city', type='container', containerProperties=props, platformCapabilities=['FARGATE'], timeout={'attemptDurationSeconds': 21600}, retryStrategy={'attempts': 3, 'evaluateOnExit': [{'onStatusReason': 'Your Spot Task*', 'action': 'RETRY'}, {'onStatusReason': 'Host EC2*', 'action': 'RETRY'}, {'onReason': '*', 'action': 'EXIT'}]})
    job = batch.submit_job(jobName='routes-city-benchmark' if limit else 'routes-city', jobQueue=plan['queue'], jobDefinition=definition['jobDefinitionArn'])
    print(dumps({'jobId': job['jobId'], 'jobDefinition': definition['jobDefinitionArn'], **plan}, indent=2))
