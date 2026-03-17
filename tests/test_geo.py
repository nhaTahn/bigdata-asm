from spark.common.geo import haversine_m


def test_haversine_zero_distance() -> None:
    assert haversine_m(10.0, 106.0, 10.0, 106.0) == 0.0


def test_haversine_positive_distance() -> None:
    dist = haversine_m(10.7769, 106.7009, 10.8231, 106.6297)
    assert 8000 <= dist <= 10000
