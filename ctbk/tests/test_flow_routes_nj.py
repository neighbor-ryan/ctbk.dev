import pytest

from ctbk.flow_routes_nj import check_catalog, checkpoint_context, nj_stations, route_record


def test_complete_nj_station_population_includes_historical_ids():
    stations = [{'id': sid, 'lng': -74., 'lat': 40.7} for sid in ['NYC1', 'JC2', 'HB1', 'JC0']]
    selected = nj_stations(stations)
    assert [s['id'] for s in selected] == ['HB1', 'JC0', 'JC2']
    assert [(a['id'], b['id']) for a in selected for b in selected if a != b] == [
        ('HB1', 'JC0'), ('HB1', 'JC2'), ('JC0', 'HB1'), ('JC0', 'JC2'), ('JC2', 'HB1'), ('JC2', 'JC0'),
    ]


def test_checkpoint_identity_changes_with_coordinates_and_osm():
    stations = [{'id': 'JC1', 'lng': -74., 'lat': 40.7}]
    identity = checkpoint_context(stations, 'osm-a')
    assert checkpoint_context(stations, 'osm-a') == identity
    assert len({identity, checkpoint_context(stations, 'osm-b'), checkpoint_context([stations[0] | {'lat': 40.8}], 'osm-a')}) == 3


def test_directional_alternatives_deduplicate_nodes_before_weighting():
    result = {'code': 'Ok', 'waypoints': [{'distance': 10}, {'distance': 20}], 'routes': [
        {'distance': 100, 'legs': [{'annotation': {'nodes': [1, 1, 2, 3]}}]},
        {'distance': 200, 'legs': [{'annotation': {'nodes': [1, 4, 3]}}]},
        {'distance': 2, 'legs': [{'annotation': {'nodes': [1, 1]}}]},
    ]}
    assert route_record({'id': 'JC1'}, {'id': 'HB2'}, result) == {
        'source': 'JC1', 'destination': 'HB2', 'snaps': [10, 20], 'fallback': None,
        'routes': [{'nodes': [1, 2, 3], 'distance': 100, 'weight': 16 / 17}, {'nodes': [1, 4, 3], 'distance': 200, 'weight': 1 / 17}],
    }


def test_explicit_fallbacks_keep_pair_coverage():
    for result, reason, snaps in [
        ({'code': 'NoRoute'}, 'NoRoute', []),
        ({'code': 'NoSegment'}, 'NoSegment', []),
        ({'code': 'Ok', 'waypoints': [{'distance': 251}, {'distance': 10}]}, 'snap_too_far', [251, 10]),
        ({'code': 'Ok', 'waypoints': [{'distance': 0}, {'distance': 0}], 'routes': []}, 'no_full_osm_edge', [0, 0]),
    ]:
        assert route_record({'id': 'HB2'}, {'id': 'JC1'}, result) == {
            'source': 'HB2', 'destination': 'JC1', 'snaps': snaps, 'fallback': reason, 'routes': [],
        }


def test_full_catalog_gate_checks_both_indexes_and_directional_continuity():
    forward = {'source': 'JC1', 'destination': 'HB2', 'fallback': None, 'routes': [{'segments': [1, 2], 'distance': 100, 'weight': 1}]}
    backward = {'source': 'HB2', 'destination': 'JC1', 'fallback': None, 'routes': [{'segments': [-2, -1], 'distance': 110, 'weight': 1}]}
    index = {'stations': [{'id': 'JC1'}, {'id': 'HB2'}], 'segments': 2, 'pairs': 2, 'routedPairs': 2, 'alternatives': 2, 'fallbacks': {}}
    graph = {'version': 1, 'segments': [{'path': [[0, 0], [1, 1]]}, {'path': [[1, 1], [2, 2]]}]}
    origins = {'JC1': {'version': 1, 'pairs': [forward]}, 'HB2': {'version': 1, 'pairs': [backward]}}
    destinations = {'HB2': {'version': 1, 'pairs': [forward]}, 'JC1': {'version': 1, 'pairs': [backward]}}
    assert check_catalog(index, graph, origins, destinations) == {'pairs': 2, 'routedPairs': 2, 'alternatives': 2, 'fallbacks': {}}
    broken = {'version': 1, 'segments': [graph['segments'][0], {'path': [[9, 9], [2, 2]]}]}
    with pytest.raises(ValueError, match='^route segment chain is discontinuous$'):
        check_catalog(index, broken, origins, destinations)
    with pytest.raises(ValueError, match='^per-station catalog has incomplete coverage$'):
        check_catalog(index, graph, origins, destinations | {'JC1': {'version': 1, 'pairs': []}})
