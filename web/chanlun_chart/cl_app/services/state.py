"""
Shared application state for blueprints and services.

Currently used to track TradingView `/tv/history` request counters
to avoid excessive repeated requests within short intervals.
"""

# Counter for recent `/tv/history` requests keyed by `<symbol>_<resolution>`
history_req_counter: dict[str, dict[str, float | int]] = {}