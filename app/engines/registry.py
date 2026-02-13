from typing import Dict, Any, Optional

# Engine Registry
# Maps schema_version to engine module path or identifier

ENGINE_REGISTRY = {
    1: "app.engines.v1",
    2: "app.engines.v2"
}

ACTIVE_ENGINE_VERSION = 2

def get_engine_module(version: int):
    """
    Returns the module path for the requested engine version.
    """
    return ENGINE_REGISTRY.get(version)
