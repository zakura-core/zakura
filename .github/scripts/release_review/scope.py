"""Validate literal repository paths and match configured diff exclusions."""

from pathlib import PurePosixPath

from .contracts import ReviewError


def safe_path(path: str) -> bool:
    return (bool(path) and not PurePosixPath(path).is_absolute()
            and all(part not in {".", ".."} for part in path.split("/"))
            and not any(ord(char) < 32 for char in path) and "\\" not in path)


def exclusion_paths(scope: dict) -> tuple[str, ...]:
    """Accept exact files or recursive directory prefixes ending in a slash."""
    if not isinstance(scope, dict) or set(scope) - {"exclude_paths"}:
        raise ReviewError("scope supports only exclude_paths")
    paths = scope.get("exclude_paths", [])
    if not isinstance(paths, list) or len(paths) > 256:
        raise ReviewError("scope.exclude_paths must be a list of at most 256 paths")
    for path in paths:
        if (not isinstance(path, str) or len(path) > 1024 or not safe_path(path)
                or path != path.strip() or "//" in path or any(char in path for char in "*?[]")
                or not path.rstrip("/")):
            raise ReviewError("exclusions must be literal repository-relative paths; use a trailing / for directories")
    return tuple(dict.fromkeys(paths))


def matching_exclusion(path: str, exclusions: tuple[str, ...]) -> str | None:
    return next((rule for rule in exclusions
                 if path == rule or (rule.endswith("/") and path.startswith(rule))), None)
