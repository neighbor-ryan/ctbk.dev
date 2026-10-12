from gzip import compress, decompress
from hashlib import md5
from io import BytesIO
from json import dumps, loads
from types import SimpleNamespace

from click.testing import CliRunner

from ctbk.flow_routes import route_weights, sample, segment_graph, wait
from ctbk.ride_animation import Ride, encode_day


def test_weighted_alternatives():
    assert route_weights([100., 200.]) == [16 / 17, 1 / 17]


def test_shared_chains_and_direction_are_deterministic():
    paths = [[1, 2, 3, 4], [1, 2, 3, 5], [4, 3, 2, 1]]
    points = {n: (float(n), 0.) for n in range(1, 6)}
    segments, lists = segment_graph(paths, points)
    assert segments == [
        {'nodes': [1, 2, 3], 'path': [[1., 0.], [2., 0.], [3., 0.]]},
        {'nodes': [3, 4], 'path': [[3., 0.], [4., 0.]]},
        {'nodes': [3, 5], 'path': [[3., 0.], [5., 0.]]},
    ]
    assert lists == [[1, 2], [1, 3], [-2, -1]]
    reverse_segments, reverse_lists = segment_graph(list(reversed(paths)), points)
    assert reverse_segments == segments
    assert reverse_lists == list(reversed(lists))


def test_sample_counts_carry_once_and_publishes_exact_graph_shape(monkeypatch, tmp_path):
    pbf = tmp_path / 'test.pbf'
    pbf.write_bytes(b'fixture OSM')
    index = {'days': {'2025-06-01': {'key': 'day.bin.gz', 'stations': 'stations.json'}}}
    blobs = {
        'rides-anim-candidate/v1/index.json': dumps(index).encode(),
        'rides-anim-candidate/v1/stations.json': dumps([{'id': 'A', 'lng': -74., 'lat': 40.}, {'id': 'B', 'lng': -73., 'lat': 41.}]).encode(),
        'rides-anim-candidate/v1/day.bin.gz': compress(encode_day([Ride(-10, 20, 0, 1, 0), Ride(0, 20, 0, 1, 0), Ride(5, 30, 0, 1, 0)], 2, 1), mtime=0),
    }
    class R2:
        def get_object(self, **kwargs):
            return {'Body': BytesIO(blobs[kwargs['Key']])}
    monkeypatch.setattr('ctbk.flow_routes._r2_client', lambda **kwargs: (R2(), 'ctbk'))
    result = {'code': 'Ok', 'waypoints': [{'distance': 1.}, {'distance': 2.}], 'routes': [
        {'distance': 100., 'legs': [{'annotation': {'nodes': [1, 2, 3]}}], 'geometry': {'coordinates': [[1., 0.], [2., 0.], [3., 0.]]}},
        {'distance': 200., 'legs': [{'annotation': {'nodes': [1, 4, 3]}}], 'geometry': {'coordinates': [[1., 0.], [4., 0.], [3., 0.]]}},
    ]}
    class Response(BytesIO):
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.close()
    monkeypatch.setattr('ctbk.flow_routes.urlopen', lambda *args, **kwargs: Response(dumps(result).encode()))
    class Handler:
        def apply_file(self, path):
            for n in range(1, 5):
                self.node(SimpleNamespace(id=n, location=SimpleNamespace(lon=float(n), lat=0.)))
    monkeypatch.setitem(__import__('sys').modules, 'osmium', SimpleNamespace(SimpleHandler=Handler))
    monkeypatch.delenv('OSRM_IMAGE', raising=False)
    output = tmp_path / 'out'
    ran = CliRunner().invoke(sample, ['-n', '1', '-p', str(pbf), '-o', str(output)])
    assert ran.exit_code == 0, ran.exception
    data = loads(decompress((output / 'sample.json.gz').read_bytes()))
    assert data == {
        'version': 1, 'month': '202506', 'inferred': True,
        'segments': [
            {'nodes': [1, 2, 3], 'path': [[1., 0.], [2., 0.], [3., 0.]]},
            {'nodes': [1, 4, 3], 'path': [[1., 0.], [4., 0.], [3., 0.]]},
        ],
        'pairs': [{'source': 'A', 'destination': 'B', 'count': 2, 'region': 'NYC', 'fallback': None, 'snaps': [1., 2.], 'routes': [
            {'weight': 16 / 17, 'distance': 100., 'geometry': [[1., 0.], [2., 0.], [3., 0.]], 'segments': [1]},
            {'weight': 1 / 17, 'distance': 200., 'geometry': [[1., 0.], [4., 0.], [3., 0.]], 'segments': [2]},
        ]}],
    }
    packed = (output / 'sample.json.gz').read_bytes()
    assert loads(ran.stdout) == {
        'version': 1, 'key': f'objects/{md5(packed).hexdigest()}.json.gz', 'bytes': len(packed),
        'month': '202506', 'pairs': 1, 'routedPairs': 1, 'alternatives': 2, 'segments': 2, 'vertices': 6,
        'fallbacks': {}, 'regions': {'NYC': 1}, 'osmMd5': md5(b'fixture OSM').hexdigest(), 'engine': 'self-hosted bicycle profile',
    }
    assert CliRunner().invoke(wait).exit_code == 0
