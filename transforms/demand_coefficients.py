"""
Seasonal regression coefficients for the HDD → residential/commercial demand model.

Form:
    demand_bcfd = intercept + slope * hdd_7d_weighted

Calibrated from EIA historical residential/commercial gas demand (series
NG.N3010US2.M) regressed against NOAA population-weighted HDD for the same
periods.  These initial values are educated estimates based on published EIA
demand patterns; run scripts/refit_demand_model.py quarterly once sufficient
live history has accumulated to improve them.

Winter (Oct–Mar):
  - HDD=0  → ~15 Bcf/d  (commercial baseline, no heating)
  - HDD=50 → ~24 Bcf/d
  - HDD=100 → ~33 Bcf/d

Summer (Apr–Sep):
  - HDD variation is minimal; power burn (captured by EIA-930) dominates.
  - Slope reflects only the marginal residential gas use on cool summer days.
"""

WINTER_MONTHS: frozenset[int] = frozenset({10, 11, 12, 1, 2, 3})

# Coefficients: (intercept_bcfd, slope_bcfd_per_hdd)
WINTER: dict[str, float] = {
    "intercept": 15.0,   # Bcf/d baseline when HDD = 0
    "slope":      0.185,  # Bcf/d per pop-weighted HDD unit
}

SUMMER: dict[str, float] = {
    "intercept": 8.2,
    "slope":     0.040,
}

# Seasonal normal demand at a typical mid-season HDD, used to compute the
# delta vs normal that appears in the UI.
SEASONAL_NORMAL_HDD: dict[str, float] = {
    "winter": 70.0,   # typical pop-weighted HDD in a normal heating week
    "summer":  5.0,   # typical pop-weighted HDD in a normal cooling week
}


def get_coefficients(month: int) -> dict[str, float]:
    """Return the appropriate coefficient dict for the given calendar month."""
    return WINTER if month in WINTER_MONTHS else SUMMER


def estimate_demand(hdd_7d_weighted: float, month: int) -> float:
    """
    Translate a 7-day population-weighted HDD value into an estimated
    residential/commercial gas demand in Bcf/d.
    """
    coeff = get_coefficients(month)
    return coeff["intercept"] + coeff["slope"] * hdd_7d_weighted


def seasonal_normal_demand(month: int) -> float:
    """
    Return the seasonal-normal demand estimate (Bcf/d) for the given month,
    based on the typical mid-season HDD.  Used to compute the deviation from
    normal displayed in the UI.
    """
    coeff = get_coefficients(month)
    season = "winter" if month in WINTER_MONTHS else "summer"
    normal_hdd = SEASONAL_NORMAL_HDD[season]
    return coeff["intercept"] + coeff["slope"] * normal_hdd
