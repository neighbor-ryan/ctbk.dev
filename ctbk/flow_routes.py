"""Offline bicycle-route sample and shared OSM segment identities."""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable
from gzip import compress, decompress
from hashlib import file_digest, md5
from itertools import groupby
from json import dumps, loads
from pathlib import Path
from time import monotonic, sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen

from click import Path as ClickPath, UsageError, option
from utz import err

from .cli.base import ctbk
from .gbfs_cli import _r2_client
from .ride_animation import decode_day


def route_weights(lengths: list[float]) -> list[float]:
    if not lengths or min(lengths) <= 0:
        raise ValueError('route lengths must be positive')
    shortest = min(lengths)
    weights = [(shortest / length) ** 4 for length in lengths]
    total = sum(weights)
    return [weight / total for weight in weights]


def segment_topology(paths: Iterable[list[int]]) -> tuple[list[tuple[int, ...]], dict[tuple[int, int], int]]:
    """Contract a streamed union; retain topology, not every route's node list."""
    neighbors = defaultdict(set)
    ends = set()
    for path in paths:
        if not path:
            continue
        ends.update([path[0], path[-1]])
        for a, b in zip(path, path[1:]):
            if a == b:
                raise ValueError('route has duplicate consecutive nodes')
            neighbors[a].add(b)
            neighbors[b].add(a)
        ends.update(path[i] for i in range(1, len(path) - 1) if path[i - 1] == path[i + 1])
    ends.update(node for node, ns in neighbors.items() if len(ns) != 2)
    visited = set()
    chains = []
    edge_key = lambda a, b: tuple(sorted((a, b)))
    for first in sorted(ends):
        for second in sorted(neighbors[first]):
            if edge_key(first, second) in visited:
                continue
            chain = [first, second]
            visited.add(edge_key(first, second))
            previous, current = first, second
            while current not in ends:
                following, = neighbors[current] - {previous}
                visited.add(edge_key(current, following))
                chain.append(following)
                previous, current = current, following
            chains.append(min(tuple(chain), tuple(reversed(chain))))
    edges = {}
    chains = sorted(chains)
    for sid, chain in enumerate(chains):
        for a, b in zip(chain, chain[1:]):
            edges[a, b] = sid + 1
            edges[b, a] = -(sid + 1)
    return chains, edges


def encode_segments(path: list[int], edges: dict[tuple[int, int], int]) -> list[int]:
    codes = []
    for a, b in zip(path, path[1:]):
        code = edges[a, b]
        if not codes or codes[-1] != code:
            codes.append(code)
    return codes


def segment_graph(
    paths: list[list[int]],
    points: dict[int, tuple[float, float]],
) -> tuple[list[dict], list[list[int]]]:
    """Contract nonbranching union chains; signed IDs carry direction."""
    chains, edges = segment_topology(paths)
    segments = [{'nodes': list(chain), 'path': [list(points[node]) for node in chain]} for chain in chains]
    return segments, [encode_segments(path, edges) for path in paths]


@ctbk.group('flow-routes')
def flow_routes() -> None:
    """Measure inferred bicycle routes with a self-hosted routing engine."""


def route_population(
    counts: Counter[tuple[int, int]],
    stations: list[dict],
    minimum: int,
) -> dict:
    pairs = [[a, b, count] for (a, b), count in sorted(counts.items()) if a != b and count >= minimum]
    total = sum(count for (a, b), count in counts.items() if a != b)
    selected = sum(p[2] for p in pairs)
    regions = Counter()
    for a, _, _ in pairs:
        sid = stations[a]['id']
        regions['JC' if sid.startswith('JC') else 'HB' if sid.startswith('HB') else 'NYC'] += 1
    return {
        'pairs': pairs, 'distinctPairs': sum(a != b for a, b in counts),
        'nonRoundTripRides': total, 'selectedRides': selected,
        'roundTrips': sum(count for (a, b), count in counts.items() if a == b),
        'regions': dict(sorted(regions.items())),
    }


@flow_routes.command('census')
@option('-d', '--date', 'date_range', required=True, help='Inclusive YYYYMM-YYYYMM animation months.')
@option('-i', '--input-prefix', default='rides-anim-candidate/v1', show_default=True)
@option('-k', '--prefix', default='routes-census/v1', show_default=True)
@option('-n', '--minimum', default=10, type=int, show_default=True)
@option('-o', '--output', type=ClickPath(path_type=Path), default=Path('tmp/routes-census'))
@option('-u', '--upload', is_flag=True)
def census(
    date_range: str,
    input_prefix: str,
    prefix: str,
    minimum: int,
    output: Path,
    upload: bool,
) -> None:
    from calendar import monthrange
    from .ride_animation_cli import animation_months
    if minimum < 1:
        raise UsageError('minimum pair count must be positive')
    if not prefix or prefix.startswith('/') or '..' in Path(prefix).parts:
        raise UsageError('prefix must be a relative bucket path')
    months = animation_months(date_range)
    r2, bucket = _r2_client(rw=upload)
    def get(key: str) -> bytes:
        return r2.get_object(Bucket=bucket, Key=key)['Body'].read()
    raw_index = get(f'{input_prefix.rstrip("/")}/index.json')
    index = loads(raw_index)
    if index['version'] != 1:
        raise ValueError('unsupported animation index version')
    dates = [f'{ym[:4]}-{ym[4:]}-{day:02d}' for ym in months for day in range(1, monthrange(int(ym[:4]), int(ym[4:]))[1] + 1)]
    metadata = [index['days'][date] for date in dates]
    tables = {meta['stations'] for meta in metadata}
    if len(tables) != 1:
        raise ValueError('route census requires a shared animation station table')
    station_key, = tables
    stations = loads(get(f'{input_prefix.rstrip("/")}/{station_key}'))
    counts = Counter()
    encoded = 0
    for date, meta in zip(dates, metadata):
        blob = get(f'{input_prefix.rstrip("/")}/{meta["key"]}')
        if len(blob) != meta['bytes'] or md5(blob).hexdigest() != meta['md5']:
            raise ValueError(f'{date}: census payload disagrees with indexed hash/size')
        nstations, carry, rides = decode_day(decompress(blob))
        if nstations != len(stations) or carry != meta['carry'] or len(rides) - carry != meta['rides']:
            raise ValueError(f'{date}: census payload disagrees with index')
        encoded += len(rides) - carry
        counts.update((ride.source, ride.destination) for ride in rides[carry:])
        if date.endswith('-01'):
            err(f'{date}: counting route population')
    if encoded != sum(index['months'][ym]['encodedRides'] for ym in months):
        raise ValueError('census ride count disagrees with month accounting')
    population = route_population(counts, stations, minimum)
    payload = {'version': 1, 'dateRange': date_range, 'minimum': minimum, 'stations': stations, 'pairs': population['pairs']}
    packed = compress(dumps(payload, separators=(',', ':'), sort_keys=True).encode() + b'\n', mtime=0)
    key = f'objects/{md5(packed).hexdigest()}.json.gz'
    summary = {k: v for k, v in population.items() if k != 'pairs'} | {
        'version': 1, 'dateRange': date_range, 'days': len(dates), 'encodedRides': encoded,
        'minimum': minimum, 'selectedPairs': len(population['pairs']), 'bytes': len(packed), 'key': key,
        'indexMd5': md5(raw_index).hexdigest(),
        'sourceRides': sum(index['months'][ym]['sourceRides'] for ym in months),
        'invalidDuration': sum(index['months'][ym]['invalidDuration'] for ym in months),
        'unmapped': sum(index['months'][ym]['unmapped'] for ym in months),
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / 'population.json.gz').write_bytes(packed)
    (output / 'summary.json').write_text(dumps(summary, indent=2, sort_keys=True) + '\n')
    if upload:
        r2.put_object(Bucket=bucket, Key=f'{prefix.rstrip("/")}/{key}', Body=packed, ContentType='application/gzip', CacheControl='public, max-age=31536000, immutable')
        r2.put_object(Bucket=bucket, Key=f'{prefix.rstrip("/")}/index.json', Body=dumps(summary, sort_keys=True).encode() + b'\n', ContentType='application/json', CacheControl='public, max-age=60')
    print(dumps(summary, indent=2, sort_keys=True))


def local_server(value: str) -> str:
    if urlparse(value).hostname not in ('localhost', '127.0.0.1', '::1'):
        raise UsageError('run the routing sample beside a self-hosted localhost engine')
    return value.rstrip('/')


@flow_routes.command('wait')
@option('-s', '--server', default='http://127.0.0.1:51991', show_default=True)
def wait(server: str) -> None:
    server = local_server(server)
    deadline = monotonic() + 60
    while monotonic() < deadline:
        try:
            with urlopen(server + '/nearest/v1/bike/-74.03,40.74', timeout=2) as response:
                result = loads(response.read())
            if result['code'] != 'Ok':
                raise ValueError(f'engine probe failed: {result}')
            return
        except URLError:
            # Connection refusal is expected while the graph is loading.
            sleep(1)
    raise TimeoutError('routing engine did not become ready in 60 seconds')


@flow_routes.command('sample')
@option('-d', '--date', 'ym', default='202506', show_default=True)
@option('-i', '--input-prefix', default='rides-anim-candidate/v1', show_default=True)
@option('-k', '--prefix', default='routes-sample/v1', show_default=True)
@option('-n', '--pairs', default=1000, type=int, show_default=True)
@option('-o', '--output', type=ClickPath(path_type=Path), default=Path('tmp/routes-sample'))
@option('-p', '--pbf', required=True, type=ClickPath(exists=True, path_type=Path))
@option('-s', '--server', default='http://127.0.0.1:51991', show_default=True)
@option('-u', '--upload', is_flag=True)
def sample(
    ym: str,
    input_prefix: str,
    prefix: str,
    pairs: int,
    output: Path,
    pbf: Path,
    server: str,
    upload: bool,
) -> None:
    import osmium
    from os import environ
    if len(ym) != 6 or not ym.isdigit() or pairs < 1:
        raise UsageError('use YYYYMM and a positive pair count')
    server = local_server(server)
    with pbf.open('rb') as source_file:
        osm_digest = file_digest(source_file, 'md5').hexdigest()
    r2, bucket = _r2_client(rw=upload)
    def get(key: str) -> bytes:
        return r2.get_object(Bucket=bucket, Key=key)['Body'].read()
    index = loads(get(input_prefix.rstrip('/') + '/index.json'))
    days = {day: meta for day, meta in index['days'].items() if day.replace('-', '').startswith(ym)}
    if not days:
        raise ValueError('animation month is not published')
    station_key, = {meta['stations'] for meta in days.values()}
    stations = loads(get(input_prefix.rstrip('/') + '/' + station_key))
    counts = Counter()
    for date, meta in sorted(days.items()):
        _, carry, rides = decode_day(decompress(get(input_prefix.rstrip('/') + '/' + meta['key'])))
        counts.update((r.source, r.destination) for r in rides[carry:])
        err(f'{date}: counted animation pairs')
    ranked = sorted((pair for pair in counts if pair[0] != pair[1]), key=lambda pair: (-counts[pair], pair))
    def region(pair: tuple[int, int]) -> str:
        sid = stations[pair[0]]['id']
        return 'JC' if sid.startswith('JC') else 'HB' if sid.startswith('HB') else 'NYC'
    chosen = []
    for name, quota in [('JC', pairs // 4), ('HB', pairs // 4), ('NYC', pairs - 2 * (pairs // 4))]:
        chosen.extend([pair for pair in ranked if region(pair) == name][:quota])
    chosen_set = set(chosen)
    chosen.extend(pair for pair in ranked if pair not in chosen_set)
    chosen = chosen[:pairs]
    records = []
    nodes = []
    route_indices = []
    output.mkdir(parents=True, exist_ok=True)
    engine = environ.get('OSRM_IMAGE', 'self-hosted bicycle profile')
    context = md5(dumps([osm_digest, engine]).encode()).hexdigest()
    cache = output / 'responses' / context
    cache.mkdir(parents=True, exist_ok=True)
    for number, (a, b) in enumerate(chosen):
        source, destination = stations[a], stations[b]
        coordinates = f'{source["lng"]},{source["lat"]};{destination["lng"]},{destination["lat"]}'
        path = cache / f'{a}-{b}.json'
        if path.exists() and environ.get('OSRM_IMAGE'):
            result = loads(path.read_text())
        else:
            url = server + '/route/v1/bike/' + coordinates + '?' + urlencode({'alternatives': 3, 'annotations': 'nodes', 'geometries': 'geojson', 'overview': 'full'})
            try:
                with urlopen(url, timeout=60) as response:
                    result = loads(response.read())
            except HTTPError as error:
                result = loads(error.read())
                if result.get('code') not in ('NoRoute', 'NoSegment'):
                    raise
            path.write_text(dumps(result, sort_keys=True) + '\n')
        record = {'source': source['id'], 'destination': destination['id'], 'count': counts[a, b], 'region': region((a, b)), 'routes': []}
        code = result['code']
        if code not in ('Ok', 'NoRoute', 'NoSegment'):
            raise ValueError(f'route error: {result}')
        snaps = [waypoint['distance'] for waypoint in result.get('waypoints', [])]
        record['snaps'] = snaps
        record['fallback'] = code if code != 'Ok' else 'snap_too_far' if max(snaps) > 250 else None
        if record['fallback'] is None:
            alternatives = [r for r in result['routes'] if len(r['legs'][0]['annotation']['nodes']) >= 2 and r['distance'] > 0]
            if not alternatives:
                record['fallback'] = 'no_full_osm_edge'
            else:
                for route, weight in zip(alternatives, route_weights([r['distance'] for r in alternatives])):
                    node_path = [node for node, _ in groupby(route['legs'][0]['annotation']['nodes'])]
                    nodes.append(node_path)
                    record['routes'].append({'weight': weight, 'distance': route['distance'], 'geometry': route['geometry']['coordinates']})
                    route_indices.append((len(records), len(record['routes']) - 1))
        records.append(record)
        if (number + 1) % 50 == 0:
            err(f'routed {number + 1}/{len(chosen)} pairs')
    needed = {node for path in nodes for node in path}
    points = {}
    class Nodes(osmium.SimpleHandler):
        def node(self, node: osmium.osm.Node) -> None:
            if node.id in needed:
                points[node.id] = (node.location.lon, node.location.lat)
    Nodes().apply_file(str(pbf))
    if set(points) != needed:
        raise ValueError(f'{len(needed - set(points))} routing nodes absent from the OSM extract')
    segments, codes = segment_graph(nodes, points)
    for (pair_index, route_index), path in zip(route_indices, codes):
        records[pair_index]['routes'][route_index]['segments'] = path
    payload = {'version': 1, 'month': ym, 'inferred': True, 'segments': segments, 'pairs': records}
    packed = compress(dumps(payload, separators=(',', ':'), sort_keys=True).encode(), compresslevel=9, mtime=0)
    digest = md5(packed).hexdigest()
    key = f'objects/{digest}.json.gz'
    (output / 'sample.json.gz').write_bytes(packed)
    summary = {
        'version': 1, 'key': key, 'bytes': len(packed), 'month': ym, 'pairs': len(records),
        'routedPairs': sum(bool(p['routes']) for p in records), 'alternatives': len(nodes),
        'segments': len(segments), 'vertices': sum(len(s['path']) for s in segments),
        'fallbacks': dict(Counter(p['fallback'] for p in records if p['fallback'])),
        'regions': dict(Counter(p['region'] for p in records)),
        'osmMd5': osm_digest, 'engine': engine,
    }
    (output / 'summary.json').write_text(dumps(summary, indent=2, sort_keys=True) + '\n')
    if upload:
        r2.put_object(Bucket=bucket, Key=prefix.rstrip('/') + '/' + key, Body=packed, ContentType='application/gzip', CacheControl='public, max-age=31536000, immutable')
        r2.put_object(Bucket=bucket, Key=prefix.rstrip('/') + '/index.json', Body=dumps(summary, sort_keys=True).encode() + b'\n', ContentType='application/json', CacheControl='public, max-age=60')
    print(dumps(summary, indent=2, sort_keys=True))
