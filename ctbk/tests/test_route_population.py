from collections import Counter

from ctbk.flow_routes import route_population


def test_annual_threshold_preserves_trip_coverage_and_excludes_round_trips():
    assert route_population(Counter({(0, 1): 10, (1, 0): 9, (1, 2): 12, (2, 0): 50, (0, 0): 3}), [{'id': 'JC001'}, {'id': 'HB001'}, {'id': '1234.01'}], 10) == {
        'pairs': [[0, 1, 10], [1, 2, 12], [2, 0, 50]], 'distinctPairs': 4,
        'nonRoundTripRides': 81, 'selectedRides': 72, 'roundTrips': 3,
        'regions': {'HB': 1, 'JC': 1, 'NYC': 1},
    }
