"""txn_pipeline: a production-style educational financial transaction data pipeline.

Synthetic data only. See README.md for the full disclaimer.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("txn-data-pipeline")
except PackageNotFoundError:  # pragma: no cover - only when running from a raw checkout
    __version__ = "0.0.0+unknown"

PIPELINE_VERSION = __version__

__all__ = ["PIPELINE_VERSION", "__version__"]
