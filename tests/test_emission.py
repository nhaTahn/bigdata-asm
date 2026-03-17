from spark.common.emission import estimate_co2_kg, estimate_fuel_liter


def test_fuel_estimation_distance_and_idle() -> None:
    fuel = estimate_fuel_liter(10000, 600, fuel_liter_per_km=0.3, idle_liter_per_minute=0.02)
    assert abs(fuel - 3.2) < 1e-9


def test_co2_estimation() -> None:
    co2 = estimate_co2_kg(2.0, 2.68)
    assert abs(co2 - 5.36) < 1e-9
