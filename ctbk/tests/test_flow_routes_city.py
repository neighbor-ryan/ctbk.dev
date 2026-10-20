from collections import Counter
from gzip import decompress
from hashlib import md5
from io import BytesIO
from json import loads
from pathlib import Path
from threading import Barrier, Lock, get_ident
from types import SimpleNamespace

import pytest

from ctbk.flow_routes import encode_segments, segment_topology
from ctbk.flow_routes_city import RouteStore, add_volumes, city, city_population, object_blob, publish_catalogs


STATIONS = [{'id': 'A', 'lng': 0, 'lat': 0}, {'id': 'B', 'lng': 1, 'lat': 1}, {'id': 'C', 'lng': 2, 'lat': 2}]


def test_sparse_population_cap_samples_evenly_across_sorted_pairs():
    population = {'version': 1, 'stations': STATIONS, 'pairs': [[2, 0, 10], [0, 2, 20], [0, 1, 10], [1, 2, 30]]}
    assert city_population(population, 3) == {
        'A': [{'source': STATIONS[0], 'destination': STATIONS[1], 'count': 10}, {'source': STATIONS[0], 'destination': STATIONS[2], 'count': 20}],
        'B': [{'source': STATIONS[1], 'destination': STATIONS[2], 'count': 30}],
    }
    assert city_population(population, 2) == {
        'A': [{'source': STATIONS[0], 'destination': STATIONS[1], 'count': 10}],
        'B': [{'source': STATIONS[1], 'destination': STATIONS[2], 'count': 30}],
    }
    with pytest.raises(ValueError, match='^duplicate population pair$'):
        city_population(population | {'pairs': [[0, 1, 10], [0, 1, 20]]})


def test_streamed_topology_preserves_shared_streets_and_opposing_direction():
    chains, edges = segment_topology(iter([[1, 2, 3], [4, 2, 3], [3, 2, 1]]))
    assert chains == [(1, 2), (2, 3), (2, 4)]
    assert [encode_segments(path, edges) for path in [[1, 2, 3], [4, 2, 3], [3, 2, 1]]] == [[1, 2], [-3, 2], [-2, -1]]


def test_system_volumes_keep_expected_splits_repeated_segments_and_fallback_accounting():
    volumes = Counter()
    assert add_volumes({'routes': [{'segments': [1, 2], 'weight': .75}, {'segments': [1, 3, 1], 'weight': .25}]}, 100, volumes) is True
    assert add_volumes({'routes': [{'segments': [-2, -1], 'weight': 1}]}, 30, volumes) is True
    assert add_volumes({'routes': []}, 20, volumes) is False
    assert dict(sorted(volumes.items())) == {-2: 30, -1: 30, 1: 125, 2: 75, 3: 25}


def test_parallel_publication_bounds_live_payloads_keeps_caller_reads_and_serial_bytes():
    catalogs = [(sid, {'version': 1, 'pairs': [{'source': sid, 'destination': 'Z', 'routes': []}]}) for sid in ['A', 'B', 'C', 'D']]
    class Store(RouteStore):
        def __init__(self, parallel=False):
            self.objects = {}
            self.barrier = Barrier(2) if parallel else None
            self.lock = Lock()
            self.state = {'active': 0, 'peak': 0, 'generated': 0, 'completed': 0}

        def put(self, key, data, mutable=False):
            with self.lock:
                self.state['active'] += 1
                self.state['peak'] = max(self.state['peak'], self.state['active'])
            if self.barrier:
                self.barrier.wait(timeout=5)
            with self.lock:
                self.objects[key] = data
                self.state['active'] -= 1
                self.state['completed'] += 1

    serial, parallel = Store(), Store(parallel=True)
    expected = {sid: serial.publish(data) for sid, data in catalogs}
    caller = get_ident()
    def generate():
        for sid, data in catalogs:
            assert get_ident() == caller
            with parallel.lock:
                assert parallel.state['generated'] - parallel.state['completed'] < 2
                parallel.state['generated'] += 1
            yield sid, data
    result = publish_catalogs(parallel.publish, generate(), workers=2)
    assert list(result) == ['A', 'B', 'C', 'D']
    assert result == expected
    assert parallel.objects == serial.objects
    assert parallel.state == {'active': 0, 'peak': 2, 'generated': 4, 'completed': 4}


def test_parallel_publication_propagates_upload_failure():
    class Store:
        def publish(self, data):
            raise RuntimeError('upload failed')
    with pytest.raises(RuntimeError, match='^upload failed$'):
        publish_catalogs(Store().publish, [('A', {'version': 1})])
    with pytest.raises(ValueError, match='^catalog workers must be positive$'):
        publish_catalogs(Store().publish, [], workers=0)


@pytest.fixture
def promotion(monkeypatch, tmp_path):
    import ctbk.flow_routes_city as module
    source, target = 'routes-city-candidate/v1', 'routes-city/v1'
    objects, metas = {}, []
    for data in [{'version': 1, 'segments': []}, {'version': 1, 'pairs': []}, {'version': 1, 'volumes': []}]:
        blob = object_blob(data)
        key = f'objects/{md5(blob).hexdigest()}.json.gz'
        objects[f'{source}/{key}'] = blob
        metas.append({'key': key, 'bytes': len(blob)})
    index = {'version': 1, 'limit': 0, 'context': 'accepted', 'pairs': 1, 'graph': metas[0], 'origins': {'A': metas[1]}, 'destinations': {'B': metas[1]}, 'system': metas[2]}
    blob = module.dumps(index, sort_keys=True).encode()
    objects[f'{source}/index.json'] = blob
    objects[f'{source}/checkpoints/unindexed.json.gz'] = b'not copied'
    objects[f'{target}/index.json'] = b'previous production index'
    audit = tmp_path / 'audit.json'
    audit.write_text(module.dumps({'indexMd5': md5(blob).hexdigest(), 'context': 'accepted'}))
    state = SimpleNamespace(objects=objects, events=[], copy_error=False, lock=Lock(), index=index, source=source, target=target, audit=audit, module=module, metas=metas)
    class Client:
        def head_object(self, Bucket, Key):
            data = objects[Key]
            return {'ContentLength': len(data), 'ETag': f'"{md5(data).hexdigest()}"'}

        def copy_object(self, **kwargs):
            if state.copy_error:
                raise RuntimeError('copy failed')
            source_key = kwargs['CopySource']['Key']
            data = objects[source_key]
            assert kwargs == {'Bucket': 'test', 'Key': f'{target}/{source_key.removeprefix(source + "/")}', 'CopySource': {'Bucket': 'test', 'Key': source_key}, 'CopySourceIfMatch': f'"{md5(data).hexdigest()}"', 'MetadataDirective': 'COPY'}
            with state.lock:
                objects[kwargs['Key']] = data
                state.events.append(('copy', kwargs['Key']))
            return {'CopyObjectResult': {'ETag': f'"{md5(data).hexdigest()}"'}}
    class Store:
        def __init__(self, prefix, rw=True):
            self.prefix, self.bucket, self.r2 = prefix, 'test', Client()

        def get(self, key):
            state.events.append(('get', f'{self.prefix}/{key}'))
            return objects[f'{self.prefix}/{key}']

        def put(self, key, data, mutable=False):
            assert mutable is True
            state.events.append(('put', f'{self.prefix}/{key}'))
            objects[f'{self.prefix}/{key}'] = data
    monkeypatch.setattr(module, 'RouteStore', Store)
    return state


def test_promotion_copies_only_indexed_ca_objects_and_publishes_index_last(promotion):
    p = promotion
    p.module.promote_city.callback(audit=p.audit, input_prefix=p.source, prefix=p.target)
    assert p.events[0] == ('get', f'{p.source}/index.json')
    assert sorted(p.events[1:-1]) == [('copy', f'{p.target}/{meta["key"]}') for meta in sorted(p.metas, key=lambda meta: meta['key'])]
    assert p.events[-1] == ('put', f'{p.target}/index.json')
    assert {key.removeprefix(p.target + '/'): value for key, value in p.objects.items() if key.startswith(p.target + '/')} == {
        'index.json': p.objects[f'{p.source}/index.json'],
        **{meta['key']: p.objects[f'{p.source}/{meta["key"]}'] for meta in p.metas},
    }


@pytest.mark.parametrize('failure', ['capped', 'index_changed', 'object_changed', 'copy_failed'])
def test_promotion_keeps_previous_index_on_rejection_or_copy_failure(promotion, failure):
    p = promotion
    exception, message = ValueError, '^candidate index differs from the accepted audit$'
    if failure == 'capped':
        p.index['limit'] = 10000
        p.objects[f'{p.source}/index.json'] = p.module.dumps(p.index).encode()
        exception, message = p.module.UsageError, '^promotion requires an uncapped version-one city catalog$'
    elif failure == 'index_changed':
        p.objects[f'{p.source}/index.json'] += b' '
    elif failure == 'object_changed':
        p.objects[f'{p.source}/{p.metas[0]["key"]}'] = b'corrupt graph'
        message = '^source CA object hash or size disagrees with index$'
    else:
        p.copy_error = True
        exception, message = RuntimeError, '^copy failed$'
    with pytest.raises(exception, match=message):
        p.module.promote_city.callback(audit=p.audit, input_prefix=p.source, prefix=p.target)
    assert p.objects[f'{p.target}/index.json'] == b'previous production index'
    assert [event for event in p.events if event[0] == 'put'] == []


def test_city_builder_publishes_sparse_both_way_catalogs_and_exact_trip_coverage(monkeypatch, tmp_path):
    import sys
    import ctbk.flow_routes_city as module
    population = {'version': 1, 'stations': STATIONS[:2], 'pairs': [[0, 1, 100], [1, 0, 20]], 'dateRange': '202401-202412', 'minimum': 10}
    blob = object_blob(population)
    from hashlib import md5
    key = f'objects/{md5(blob).hexdigest()}.json.gz'
    buckets = {'routes-census/v1': {'index.json': module.dumps({'key': key, 'bytes': len(blob), 'nonRoundTripRides': 150}).encode(), key: blob}}
    class Store:
        def __init__(self, prefix, rw=True):
            self.prefix = prefix
            self.bucket = 'test'
            self.r2 = SimpleNamespace(download_file=lambda bucket, name, path: Path(path).write_bytes(b'osm'))
            self.objects = buckets.setdefault(prefix, {})

        def get(self, key):
            return self.objects[key]

        def optional(self, key):
            return self.objects.get(key)

        def put(self, key, data, mutable=False):
            self.objects[key] = data

        def publish(self, data):
            blob = object_blob(data)
            key = f'objects/{md5(blob).hexdigest()}.json.gz'
            self.put(key, blob)
            return {'key': key, 'bytes': len(blob)}

        def packed(self, meta):
            return loads(decompress(self.get(meta['key'])))
    class Nodes:
        def apply_file(self, path):
            for node in [1, 2]:
                self.node(SimpleNamespace(id=node, location=SimpleNamespace(lon=node - 1, lat=node - 1)))
    def response(url, timeout):
        result = {'code': 'NoRoute'} if '/1,1;0,0?' in url else {'code': 'Ok', 'waypoints': [{'distance': 0}, {'distance': 0}], 'routes': [{'distance': 100, 'legs': [{'annotation': {'nodes': [1, 2]}}]}]}
        return BytesIO(module.dumps(result).encode())
    monkeypatch.setitem(sys.modules, 'osmium', SimpleNamespace(SimpleHandler=Nodes))
    monkeypatch.setattr(module, 'RouteStore', Store)
    monkeypatch.setattr(module, 'run', lambda *args, **kwargs: None)
    monkeypatch.setattr(module, 'Popen', lambda *args: SimpleNamespace(terminate=lambda: None, wait=lambda timeout: None))
    monkeypatch.setattr(module.wait, 'callback', lambda server: None)
    monkeypatch.setattr(module, 'urlopen', response)
    city.callback(input_prefix='routes-census/v1', prefix='test-city', limit=0, output=tmp_path, snapshot='20261012')
    store = Store('test-city')
    index = loads(store.get('index.json'))
    assert {k: index[k] for k in ['pairs', 'routedPairs', 'routedRides', 'selectedRides', 'nonRoundTripRides', 'segments', 'alternatives', 'fallbacks']} == {
        'pairs': 2, 'routedPairs': 1, 'routedRides': 100, 'selectedRides': 120, 'nonRoundTripRides': 150, 'segments': 1, 'alternatives': 1, 'fallbacks': {'NoRoute': 1},
    }
    assert store.packed(index['origins']['A']) == store.packed(index['destinations']['B']) == {'version': 1, 'pairs': [{'source': 'A', 'destination': 'B', 'snaps': [0, 0], 'fallback': None, 'routes': [{'distance': 100, 'segments': [1], 'weight': 1}]}]}
    assert store.packed(index['origins']['B']) == store.packed(index['destinations']['A']) == {'version': 1, 'pairs': [{'source': 'B', 'destination': 'A', 'snaps': [], 'fallback': 'NoRoute', 'routes': []}]}
    assert store.packed(index['system'])['volumes'] == [[1, 100]]
    module.check_city.callback(prefix='test-city', output=tmp_path / 'audit.json')
    assert {k: loads((tmp_path / 'audit.json').read_text())[k] for k in ['pairs', 'routedPairs', 'routedRides', 'alternatives', 'fallbacks']} == {
        'pairs': 2, 'routedPairs': 1, 'routedRides': 100, 'alternatives': 1, 'fallbacks': {'NoRoute': 1},
    }
    parallel_report = loads((tmp_path / 'audit.json').read_text())
    bounded = module.bounded_results
    with monkeypatch.context() as serial:
        serial.setattr(module, 'bounded_results', lambda function, items, workers=8: bounded(function, items, workers=1))
        module.check_city.callback(prefix='test-city', output=tmp_path / 'serial-audit.json')
    assert loads((tmp_path / 'serial-audit.json').read_text()) == parallel_report
    # A second run reads the saved origin checkpoints rather than contacting the router.
    monkeypatch.setattr(module, 'urlopen', lambda *args, **kwargs: pytest.fail('checkpoint route was recomputed'))
    city.callback(input_prefix='routes-census/v1', prefix='test-city', limit=0, output=tmp_path, snapshot='20261012')
    assert loads(store.get('index.json'))['timings']['resumedOrigins'] == 2


def test_monthly_system_flow_excludes_carry_and_round_trips_and_reports_missing_routes(monkeypatch):
    from gzip import compress
    from hashlib import md5

    import calendar
    import ctbk.flow_routes_city as module
    from ctbk.ride_animation import Ride, encode_day

    days = [
        [Ride(0, 60, 0, 1, 0), Ride(60, 60, 1, 0, 0), Ride(120, 60, 0, 0, 0), Ride(180, 60, 0, 2, 0)],
        [Ride(-30, 60, 0, 1, 0), Ride(0, 60, 0, 1, 0)],
    ]
    blobs = [compress(encode_day(rides, 3, carry), mtime=0) for rides, carry in zip(days, [0, 1])]
    animation_index = {'version': 1, 'days': {
        f'2025-01-{i + 1:02d}': {'key': str(i), 'bytes': len(blob), 'md5': md5(blob).hexdigest(), 'stations': 'stations', 'carry': i, 'rides': len(days[i]) - i}
        for i, blob in enumerate(blobs)
    }, 'months': {'202501': {'sourceRides': 5, 'encodedRides': 5, 'final': False}}}
    graph_meta = {'key': 'graph', 'bytes': 1}
    catalog = {'version': 1, 'context': 'test-context', 'graph': graph_meta, 'origins': {'A': {'key': 'origin', 'bytes': 1}}}
    buckets = {
        'rides-anim-candidate/v1': {'index.json': module.dumps(animation_index).encode(), 'stations': module.dumps(STATIONS).encode(), **{str(i): blob for i, blob in enumerate(blobs)}},
        'routes-city/v1': {'index.json': module.dumps(catalog).encode(), 'origin': object_blob({'version': 1, 'pairs': [{'source': 'A', 'destination': 'B', 'routes': [{'segments': [1], 'weight': 1}]}]})},
        'flows/v1': {},
    }
    class Store:
        def __init__(self, prefix, rw=True):
            self.objects = buckets[prefix]

        def get(self, key):
            return self.objects[key]

        def optional(self, key):
            return self.objects.get(key)

        def put(self, key, data, mutable=False):
            self.objects[key] = data

        def packed(self, meta):
            return loads(decompress(self.get(meta['key'])))

        def publish(self, data):
            self.put('month', object_blob(data))
            return {'key': 'month', 'bytes': len(self.get('month'))}
    monkeypatch.setattr(module, 'RouteStore', Store)
    monkeypatch.setattr(calendar, 'monthrange', lambda year, month: (0, 2))
    module.system.callback(ym='202501', input_prefix='rides-anim-candidate/v1', prefix='flows/v1', routes_prefix='routes-city/v1')
    assert loads(decompress(buckets['flows/v1']['month'])) == {
        'version': 1, 'month': '202501', 'context': 'test-context', 'graph': graph_meta, 'routesPrefix': 'routes-city/v1',
        'volumes': [[1, 2]], 'sourceRides': 5, 'encodedRides': 5, 'routedRides': 2, 'fallbackRides': 2, 'roundTrips': 1,
        'provisional': True, 'animationObjects': [md5(blob).hexdigest() for blob in blobs],
    }
