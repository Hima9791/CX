from typing import Dict, Tuple


def l1_distance(tmin: float, tmax: float, gmin: float, gmax: float) -> float:
    return abs(float(tmin) - float(gmin)) + abs(float(tmax) - float(gmax))


def jaccard_distance(tmin: float, tmax: float, gmin: float, gmax: float) -> float:
    # Intervals closed on both ends
    inter = max(0.0, min(float(tmax), float(gmax)) - max(float(tmin), float(gmin)))
    union = max(float(tmax), float(gmax)) - min(float(tmin), float(gmin))
    if union <= 0:
        return 1.0
    return 1.0 - (inter / union)


def linf_distance(tmin: float, tmax: float, gmin: float, gmax: float) -> float:
    """Endpoint-wise max error (both ends must be close). Units: °C."""
    return max(abs(float(tmin) - float(gmin)), abs(float(tmax) - float(gmax)))


def nearest_grade(
    tmin: float,
    tmax: float,
    bands: Dict[str, Tuple[float, float]],
    primary: str = "jaccard",
):
    """Score each grade band against the target range."""

    scored = []
    for name, (gmin, gmax) in bands.items():
        dJ = jaccard_distance(tmin, tmax, gmin, gmax)
        dL1 = l1_distance(tmin, tmax, gmin, gmax)
        dInf = linf_distance(tmin, tmax, gmin, gmax)
        scored.append((name, {"jaccard": dJ, "l1": dL1, "linf": dInf}))

    key = primary.lower()
    if key in ("ends", "ends_max"):
        key = "linf"

    scored.sort(
        key=lambda x: (
            x[1][key],
            x[1]["l1"],
            x[1]["jaccard"],
            x[0],
        )
    )
    best = scored[0] if scored else None
    return best, scored
