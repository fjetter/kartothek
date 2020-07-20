import intake  # noqa: F401 - Required for their plugin discovery system

from .datasource import KartothekDatasetSource

__all__ = ["KartothekDatasetSource"]
