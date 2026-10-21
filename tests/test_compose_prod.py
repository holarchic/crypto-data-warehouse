"""The prod override caps the memory of every compose service, one-shot profiles included."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent


class _ComposeLoader(yaml.SafeLoader):
    """SafeLoader that reads compose's merge tags (`!reset`, `!override`) as null."""


_ComposeLoader.add_multi_constructor("!", lambda loader, suffix, node: None)


def _services(name: str) -> dict:
    return yaml.load((ROOT / name).read_text(), Loader=_ComposeLoader)["services"]


def test_every_service_has_a_prod_memory_limit():
    services = _services("docker-compose.yml")
    prod = _services("docker-compose.prod.yml")

    # Compose reads a zero limit as no limit, so the value must be a positive size.
    unlimited = [
        name
        for name in services
        if not re.fullmatch(
            r"[1-9][0-9]*[kmg]?",
            str((prod.get(name) or {}).get("deploy", {}).get("resources", {}).get("limits", {}).get("memory")),
        )
    ]

    assert "hl-candles-repair" in services
    assert unlimited == []
