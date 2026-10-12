"""Complete, resumable NJ bicycle routing on HCCS Batch."""
from __future__ import annotations

from collections import Counter
from copy import deepcopy
from gzip import compress, decompress
from hashlib import file_digest, md5
from itertools import groupby
from json import dumps, loads
from os import environ
from pathlib import Path
from re import fullmatch
from subprocess import Popen, run
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

from botocore.exceptions import ClientError
from click import Path as ClickPath, UsageError, option
from utz import err

from .flow_routes import flow_routes, route_weights, segment_graph, wait
from .gbfs_cli import _r2_client

OSRM_IMAGE = 'ghcr.io/project-osrm/osrm-backend@sha256:84074e1e66bd7eedc7fe52ece0f4deb2e6865454855ae77675c5f753d281fd58'
PREFIX = 'routes-nj/v1'


def nj_stations(stations: list[dict]) -> list[dict]:
    selected = sorted((s for s in stations if s['id'].startswith(('JC', 'HB'))), key=lambda s: s['id'])
    if len({s['id'] for s in selected}) != len(selected):
        raise ValueError('duplicate NJ station IDs')
    return selected


def route_record(source: dict, destination: dict, result: dict) -> dict:
    code = result['code']
    if code not in ('Ok', 'NoRoute', 'NoSegment'):
        raise ValueError(f'route error: {code}')
    snaps = [w['distance'] for w in result.get('waypoints', [])]
    if code == 'Ok' and len(snaps) != 2:
        raise ValueError('routing response must have two snapped endpoints')
    fallback = code if code != 'Ok' else 'snap_too_far' if max(snaps) > 250 else None
    alternatives = []
    if fallback is None:
        for route in result['routes']:
            nodes = [node for node, _ in groupby(route['legs'][0]['annotation']['nodes'])]
            if len(nodes) >= 2 and route['distance'] > 0:
                alternatives.append({'nodes': nodes, 'distance': route['distance']})
        if not alternatives:
            fallback = 'no_full_osm_edge'
    if alternatives:
        for route, weight in zip(alternatives, route_weights([r['distance'] for r in alternatives])):
            route['weight'] = weight
    return {'source': source['id'], 'destination': destination['id'], 'routes': alternatives, 'snaps': snaps, 'fallback': fallback}


def checkpoint_context(stations: list[dict], osm_md5: str) -> str:
    return md5(dumps([stations, osm_md5, OSRM_IMAGE, 'bicycle', 3, 250], sort_keys=True).encode()).hexdigest()


@flow_routes.command('nj')
@option('-d', '--date', 'ym', default='202609', show_default=True, help='Published animation station-table month (includes historical stations).')
@option('-i', '--input-prefix', default='rides-anim-candidate/v1', show_default=True)
@option('-k', '--prefix', default=PREFIX, show_default=True)
@option('-o', '--output', type=ClickPath(path_type=Path), default=Path('tmp/routes-nj'))
@option('-p', '--snapshot', default='20261012', show_default=True, help='Immutable OSM snapshot identifier; reused across Spot retries.')
def nj(
    ym: str,
    input_prefix: str,
    prefix: str,
    output: Path,
    snapshot: str,
) -> None:
    import osmium
    if not fullmatch(r'\d{6}', ym) or not fullmatch(r'\d{8}', snapshot):
        raise UsageError('use YYYYMM and YYYYMMDD snapshot identifiers')
    if not prefix or prefix.startswith('/') or '..' in Path(prefix).parts:
        raise UsageError('prefix must be a relative bucket path')
    output.mkdir(parents=True, exist_ok=True)
    r2, bucket = _r2_client(rw=True)
    def get(key: str) -> bytes | None:
        try:
            return r2.get_object(Bucket=bucket, Key=key)['Body'].read()
        except ClientError as error:
            if error.response['Error']['Code'] in ('NoSuchKey', '404'):
                return None
            raise
    def put(key: str, body: bytes, mutable: bool = False) -> None:
        r2.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json' if mutable else 'application/gzip', CacheControl='public, max-age=60' if mutable else 'public, max-age=31536000, immutable')
    def publish(data: dict) -> dict:
        blob = compress(dumps(data, sort_keys=True, separators=(',', ':')).encode(), mtime=0)
        key = f'objects/{md5(blob).hexdigest()}.json.gz'
        put(f'{prefix}/{key}', blob)
        return {'key': key, 'bytes': len(blob)}
    index = loads(get(f'{input_prefix}/index.json'))
    station_keys = {m['stations'] for d, m in index['days'].items() if d.replace('-', '').startswith(ym)}
    if len(station_keys) != 1:
        raise ValueError('month requires exactly one published station table')
    station_key, = station_keys
    stations = nj_stations(loads(get(f'{input_prefix}/{station_key}')))
    if len(stations) < 2:
        raise ValueError('station table has fewer than two NJ stations')
    pbf = output / 'NewYork.osm.pbf'
    pbf_key = f'{prefix}/inputs/{snapshot}/NewYork.osm.pbf'
    # Exact-key reads only: no historical R2 inventory scans.
    saved = get(pbf_key)
    if saved is not None:
        pbf.write_bytes(saved)
        del saved
    else:
        run(['curl', '--fail', '--location', '--connect-timeout', '15', '--max-time', '600', '--retry', '3', '--retry-all-errors', 'https://data.bbbike.org/osm/bbbike/NewYork/NewYork.osm.pbf', '--output', str(pbf)], check=True)
        r2.upload_file(str(pbf), bucket, pbf_key)
    with pbf.open('rb') as source_file:
        osm_md5 = file_digest(source_file, 'md5').hexdigest()
    context = checkpoint_context(stations, osm_md5)
    graph = pbf.with_suffix('').with_suffix('.osrm')
    threads = environ.get('ROUTE_THREADS', '4')
    for executable, args in [('osrm-extract', ['-p', '/opt/bicycle.lua', str(pbf)]), ('osrm-partition', [str(graph)]), ('osrm-customize', [str(graph)])]:
        run([executable, '--threads', threads, *args], check=True)
    server = 'http://127.0.0.1:51991'
    router = Popen(['osrm-routed', '--algorithm', 'mld', '--threads', threads, '--port', '51991', str(graph)])
    records = []
    resumed = 0
    try:
        wait.callback(server)
        for number, source in enumerate(stations):
            checkpoint = f'{prefix}/checkpoints/{context}/{source["id"]}.json.gz'
            cached = get(checkpoint)
            destinations = [s for s in stations if s['id'] != source['id']]
            if cached is not None:
                origin = loads(decompress(cached))
                if [(r['source'], r['destination']) for r in origin] != [(source['id'], d['id']) for d in destinations]:
                    raise ValueError('checkpoint pair coverage disagrees with its context')
                resumed += 1
            else:
                origin = []
                for destination in destinations:
                    coords = f'{source["lng"]},{source["lat"]};{destination["lng"]},{destination["lat"]}'
                    url = server + '/route/v1/bike/' + coords + '?' + urlencode({'alternatives': 3, 'annotations': 'nodes', 'geometries': 'geojson', 'overview': 'false'})
                    try:
                        with urlopen(url, timeout=60) as response:
                            result = loads(response.read())
                    except HTTPError as error:
                        result = loads(error.read())
                        if result.get('code') not in ('NoRoute', 'NoSegment'):
                            raise
                    origin.append(route_record(source, destination, result))
                put(checkpoint, compress(dumps(origin, sort_keys=True, separators=(',', ':')).encode(), mtime=0))
            records.extend(origin)
            err(f'NJ origin {number + 1}/{len(stations)}: {source["id"]}; {len(records)} ordered pairs; reused {resumed} origins')
    finally:
        router.terminate()
        router.wait(timeout=30)
    paths = [r['nodes'] for pair in records for r in pair['routes']]
    needed = {node for path in paths for node in path}
    points = {}
    class Nodes(osmium.SimpleHandler):
        def node(self, node: osmium.osm.Node) -> None:
            if node.id in needed:
                points[node.id] = (node.location.lon, node.location.lat)
    Nodes().apply_file(str(pbf))
    if set(points) != needed:
        raise ValueError(f'{len(needed - set(points))} routing nodes absent from the OSM extract')
    segments, codes = segment_graph(paths, points)
    for route, path in zip((r for pair in records for r in pair['routes']), codes):
        del route['nodes']
        route['segments'] = path
    graph_meta = publish({'version': 1, 'segments': segments})
    origins = {s['id']: publish({'version': 1, 'pairs': [r for r in records if r['source'] == s['id']]}) for s in stations}
    destinations = {s['id']: publish({'version': 1, 'pairs': [r for r in records if r['destination'] == s['id']]}) for s in stations}
    expected = len(stations) * (len(stations) - 1)
    if len(records) != expected or len({(r['source'], r['destination']) for r in records}) != expected:
        raise ValueError('NJ route catalog is incomplete')
    summary = {
        'version': 1, 'scope': 'all ordered non-roundtrip JC/HB station pairs',
        'stations': stations, 'stationTable': station_key, 'stationMonth': ym,
        'pairs': expected, 'routedPairs': sum(bool(p['routes']) for p in records),
        'fallbacks': dict(sorted(Counter(p['fallback'] for p in records if p['fallback']).items())),
        'segments': len(segments), 'vertices': sum(len(s['path']) for s in segments),
        'alternatives': len(paths), 'graph': graph_meta, 'origins': origins, 'destinations': destinations,
        'bytes': graph_meta['bytes'] + sum(m['bytes'] for m in origins.values()) + sum(m['bytes'] for m in destinations.values()),
        'context': context, 'osmMd5': osm_md5, 'osmKey': pbf_key, 'engine': OSRM_IMAGE,
    }
    (output / 'summary.json').write_text(dumps(summary, indent=2, sort_keys=True) + '\n')
    put(f'{prefix}/index.json', dumps(summary, sort_keys=True).encode() + b'\n', mutable=True)
    print(dumps({k: v for k, v in summary.items() if k not in ('stations', 'origins', 'destinations')}, indent=2, sort_keys=True))


@flow_routes.command('submit-nj')
@option('-n', '--dry-run', is_flag=True)
@option('-s', '--image', required=True, help='Pinned HCCS ctbk-reproc:routes-SHA image.')
def submit_nj(dry_run: bool, image: str) -> None:
    import boto3
    if not fullmatch(r'688066488567\.dkr\.ecr\.us-east-1\.amazonaws\.com/ctbk-reproc:routes-[a-f0-9]{8,40}', image):
        raise UsageError('use a pinned HCCS routing image')
    command = ['ctbk', 'flow-routes', 'nj']
    plan = {'queue': 'ctbk-reproc', 'image': image, 'architecture': 'X86_64', 'vcpus': 4, 'memory': 16384, 'command': command}
    if dry_run:
        print(dumps(plan, indent=2))
        return
    if boto3.client('sts').get_caller_identity()['Account'] != '688066488567':
        raise UsageError('submit requires the HCCS AWS account')
    batch = boto3.client('batch', region_name='us-east-1')
    latest = max(batch.describe_job_definitions(jobDefinitionName='ctbk-reproc', status='ACTIVE')['jobDefinitions'], key=lambda d: d['revision'])
    props = deepcopy(latest['containerProperties'])
    props.update(image=image, command=command, runtimePlatform={'cpuArchitecture': 'X86_64', 'operatingSystemFamily': 'LINUX'}, resourceRequirements=[{'type': 'VCPU', 'value': '4'}, {'type': 'MEMORY', 'value': '16384'}])
    names = {'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY'}
    props['secrets'] = [s for s in props.get('secrets', []) if s['name'] in names]
    if {s['name'] for s in props['secrets']} != names:
        raise UsageError('base definition must bind the R2 pair from Secrets Manager')
    props['environment'] = [{'name': 'CLOUDFLARE_ACCOUNT_ID', 'value': '2363642879f18d37d52dca114059937e'}]
    definition = batch.register_job_definition(jobDefinitionName='ctbk-reproc-routes', type='container', containerProperties=props, platformCapabilities=['FARGATE'], retryStrategy={'attempts': 3, 'evaluateOnExit': [{'onStatusReason': 'Your Spot Task*', 'action': 'RETRY'}, {'onStatusReason': 'Host EC2*', 'action': 'RETRY'}, {'onReason': '*', 'action': 'EXIT'}]})
    job = batch.submit_job(jobName='routes-nj', jobQueue=plan['queue'], jobDefinition=definition['jobDefinitionArn'])
    print(dumps({'jobId': job['jobId'], 'jobDefinition': definition['jobDefinitionArn'], **plan}, indent=2))
