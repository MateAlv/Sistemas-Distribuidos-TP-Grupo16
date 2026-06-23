from pathlib import Path


def resolve_compose_file(root: Path, compose_file: str | None, *default_parts: str) -> Path:
    if compose_file:
        path = Path(compose_file)
        if not path.is_absolute():
            path = root / path
    else:
        name = "-".join(part.replace("_", "-").lower() for part in default_parts)
        path = root / "tmp" / "crash-tests" / f"docker-compose.{name}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
