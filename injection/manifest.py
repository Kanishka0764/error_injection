"""
Manifest structures for tracking injected errors.
Provides MutationRecord, InjectionManifest, JSON serialization, and validator scoring.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
import json
import pandas as pd


@dataclass
class MutationRecord:
    """Record of a single injected error."""

    error_id: str  # "INJ-SD0013-AE-001"
    rule_id: str  # "SD0013"
    rule_message: str  # "SDTM-derived variable not calculated"
    category: str  # "date_cross_domain"
    primitive: str  # "invert_date_order"
    domain: str  # "AE"
    usubjid: str  # Subject ID
    row_index: int  # Row index in domain DataFrame
    seq_value: Optional[str] = None  # AESEQ, EXSEQ, etc., nullable

    # Variables modified: {field: {original, injected}}
    variables_modified: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Fields re-derived due to corruption: {field: {original, new}}
    re_derived: Dict[str, Dict[str, str]] = field(default_factory=dict)

    # Rules expected to also be triggered by this injection
    expected_co_violations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MutationRecord":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class InjectionManifest:
    """Ground truth manifest for all injected errors."""

    generated_at: str  # ISO timestamp
    seed: int
    mode: str  # "compound" or "isolated"
    source_dir: str
    profile: str
    rate: float
    density_cap: int
    prioritize_rules: bool
    rules_available: int
    rules_injectable: int
    rules_skipped: int
    rules_injected: int
    total_mutations: int

    errors: List[MutationRecord] = field(default_factory=list)
    skipped_rules: List[Dict[str, str]] = field(default_factory=list)  # {rule_id, reason}

    def add_error(self, error: MutationRecord) -> None:
        """Add a mutation record."""
        self.errors.append(error)

    def add_skipped_rule(self, rule_id: str, reason: str) -> None:
        """Record a skipped rule and reason."""
        self.skipped_rules.append({"rule_id": rule_id, "reason": reason})

    def get_summary_by_rule(self) -> Dict[str, int]:
        """Count errors per rule."""
        summary = {}
        for error in self.errors:
            summary[error.rule_id] = summary.get(error.rule_id, 0) + 1
        return summary

    def get_summary_by_category(self) -> Dict[str, int]:
        """Count errors per category."""
        summary = {}
        for error in self.errors:
            summary[error.category] = summary.get(error.category, 0) + 1
        return summary

    def get_summary_by_domain(self) -> Dict[str, int]:
        """Count errors per domain."""
        summary = {}
        for error in self.errors:
            summary[error.domain] = summary.get(error.domain, 0) + 1
        return summary

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "generated_at": self.generated_at,
            "seed": self.seed,
            "mode": self.mode,
            "source_dir": self.source_dir,
            "profile": self.profile,
            "rate": self.rate,
            "density_cap": self.density_cap,
            "prioritize_rules": self.prioritize_rules,
            "rules_available": self.rules_available,
            "rules_injectable": self.rules_injectable,
            "rules_skipped": self.rules_skipped,
            "rules_injected": self.rules_injected,
            "total_mutations": self.total_mutations,
            "errors": [e.to_dict() for e in self.errors],
            "skipped_rules": self.skipped_rules,
            "summary_by_rule": self.get_summary_by_rule(),
            "summary_by_category": self.get_summary_by_category(),
            "summary_by_domain": self.get_summary_by_domain(),
        }

    def to_json(self, output_path: Path) -> None:
        """Write manifest to JSON file."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def from_json(cls, json_path: Path) -> "InjectionManifest":
        """Load manifest from JSON file."""
        with open(json_path) as f:
            data = json.load(f)

        # Reconstruct errors
        errors = [MutationRecord.from_dict(e) for e in data.pop("errors", [])]
        skipped_rules = data.pop("skipped_rules", [])
        data.pop("summary_by_rule", None)  # These are computed
        data.pop("summary_by_category", None)
        data.pop("summary_by_domain", None)

        manifest = cls(**data)
        manifest.errors = errors
        manifest.skipped_rules = skipped_rules
        return manifest


def score_validator(
    manifest_path: Path,
    validator_output_path: Path,
    rule_id_column: str = "Rule ID",
    domain_column: str = "Domain",
    usubjid_column: str = "USUBJID",
) -> pd.DataFrame:
    """
    Score validator against ground truth manifest.

    Compares manifest injected errors vs validator findings.

    Args:
        manifest_path: Path to manifest.json
        validator_output_path: Path to validator output CSV
        rule_id_column: Column name for rule ID in validator output
        domain_column: Column name for domain in validator output
        usubjid_column: Column name for USUBJID in validator output

    Returns:
        DataFrame with per-rule metrics:
        - rule_id
        - expected (count in manifest)
        - detected (count in validator)
        - TP (true positive)
        - FN (false negative)
        - FP (false positive)
        - precision
        - recall
    """
    manifest = InjectionManifest.from_json(manifest_path)
    validator = pd.read_csv(validator_output_path, dtype=str)

    # Get expected errors
    expected = manifest.get_summary_by_rule()

    # Get detected errors from validator
    detected = {}
    if rule_id_column in validator.columns:
        detected = validator[rule_id_column].value_counts().to_dict()

    # Calculate metrics
    results = []
    all_rules = set(expected.keys()) | set(detected.keys())

    for rule_id in sorted(all_rules):
        exp_count = expected.get(rule_id, 0)
        det_count = detected.get(rule_id, 0)

        tp = min(exp_count, det_count)
        fn = max(0, exp_count - det_count)
        fp = max(0, det_count - exp_count)

        precision = tp / det_count if det_count > 0 else 0.0
        recall = tp / exp_count if exp_count > 0 else 0.0

        results.append({
            "rule_id": rule_id,
            "expected": exp_count,
            "detected": det_count,
            "TP": tp,
            "FN": fn,
            "FP": fp,
            "precision": precision,
            "recall": recall,
        })

    return pd.DataFrame(results)
