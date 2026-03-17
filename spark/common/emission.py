from __future__ import annotations


def estimate_fuel_liter(distance_m: float, idle_s: float, fuel_liter_per_km: float, idle_liter_per_minute: float) -> float:
    distance_km = max(distance_m, 0.0) / 1000.0
    idle_min = max(idle_s, 0.0) / 60.0
    return distance_km * fuel_liter_per_km + idle_min * idle_liter_per_minute


def estimate_co2_kg(fuel_liter: float, co2_kg_per_liter_fuel: float) -> float:
    return max(fuel_liter, 0.0) * co2_kg_per_liter_fuel
