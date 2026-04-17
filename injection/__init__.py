"""
SDTM Error Injection Test Harness

Generates test datasets with known, labeled errors for validator testing.
"""

__version__ = "0.1.0"

from injection.engine import InjectionEngine
from injection.manifest import InjectionManifest, MutationRecord

__all__ = [
    "InjectionEngine",
    "InjectionManifest",
    "MutationRecord",
]
