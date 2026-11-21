"""Minimal RFC 6902 diff for ``STATE_DELTA``."""

from __future__ import annotations

from typing import Any


def json_diff(old: Any, new: Any, path: str = "") -> list[dict[str, Any]]:
    """Operations (``add``/``remove``/``replace``) that turn JSON value ``old`` into ``new``."""
    if isinstance(old, dict) and isinstance(new, dict):
        ops = [{"op": "remove", "path": _child(path, k)} for k in old if k not in new]
        for key, value in new.items():
            if key in old:
                ops += json_diff(old[key], value, _child(path, key))
            else:
                ops.append({"op": "add", "path": _child(path, key), "value": value})
        return ops
    if isinstance(old, list) and isinstance(new, list):
        common = min(len(old), len(new))
        ops = [op for i in range(common) for op in json_diff(old[i], new[i], f"{path}/{i}")]
        ops += [{"op": "add", "path": f"{path}/{i}", "value": new[i]} for i in range(common, len(new))]
        return ops + [{"op": "remove", "path": f"{path}/{i}"} for i in reversed(range(common, len(old)))]
    if type(old) is type(new) and old == new:
        return []
    return [{"op": "replace", "path": path, "value": new}]


def _child(path: str, key: object) -> str:
    return f"{path}/{str(key).replace('~', '~0').replace('/', '~1')}"
