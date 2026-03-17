from datetime import datetime, timezone


def is_new_trip(prev_time: datetime | None, current_time: datetime, trip_gap_min: int) -> bool:
    if prev_time is None:
        return True
    return (current_time - prev_time).total_seconds() > trip_gap_min * 60


def test_trip_gap_logic() -> None:
    t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 1, 10, 20, tzinfo=timezone.utc)
    assert is_new_trip(t1, t2, 15)


def test_trip_gap_no_split() -> None:
    t1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 1, 10, 10, tzinfo=timezone.utc)
    assert not is_new_trip(t1, t2, 15)
