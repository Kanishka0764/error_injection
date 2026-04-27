"""
Output writer for dirty datasets, manifests, and reports.
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from injection.manifest import InjectionManifest


def write_datasets(
    datasets: Dict[str, pd.DataFrame],
    output_dir: Path,
    preserve_na: bool = False,
) -> None:
    """
    Write datasets to CSV files.

    Args:
        datasets: Dict mapping domain → DataFrame
        output_dir: Output directory
        preserve_na: If True, don't convert NaN back to empty string
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for domain, df in datasets.items():
        csv_path = output_dir / f"{domain}.csv"

        # Convert NaN back to empty string for compatibility
        if not preserve_na:
            df = df.fillna("")

        df.to_csv(csv_path, index=False)


def write_manifest(
    manifest: InjectionManifest,
    output_path: Path,
) -> None:
    """
    Write manifest to JSON file.

    Args:
        manifest: InjectionManifest object
        output_path: Path to output manifest.json
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest.to_json(output_path)


def write_report(
    manifest: InjectionManifest,
    skipped_rules: Optional[List[tuple]] = None,
    warnings: Optional[List[str]] = None,
    output_path: Optional[Path] = None,
) -> Optional[str]:
    """
    Write human-readable report.

    Args:
        manifest: InjectionManifest with all errors
        skipped_rules: List of (rule_id, reason) tuples
        warnings: List of validation warnings
        output_path: Output path. If None, returns string instead.

    Returns:
        Report text if output_path is None, else None
    """
    lines = []

    # Header
    lines.append("=" * 80)
    lines.append("SDTM ERROR INJECTION REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Metadata
    lines.append("METADATA")
    lines.append("-" * 80)
    lines.append(f"Generated: {manifest.generated_at}")
    lines.append(f"Seed: {manifest.seed}")
    lines.append(f"Mode: {manifest.mode}")
    lines.append(f"Profile: {manifest.profile}")
    lines.append(f"Source: {manifest.source_dir}")
    lines.append(f"Rate: {manifest.rate * 100:.1f}%")
    lines.append(f"Density Cap: {manifest.density_cap}")
    lines.append(f"Prioritize Rules: {manifest.prioritize_rules}")
    lines.append("")

    # Rule statistics
    lines.append("RULE STATISTICS")
    lines.append("-" * 80)
    lines.append(f"Rules Available: {manifest.rules_available}")
    lines.append(f"Rules Injectable: {manifest.rules_injectable}")
    lines.append(f"Rules Injected: {manifest.rules_injected}")
    lines.append(f"Rules Skipped: {manifest.rules_skipped}")
    lines.append(f"Total Mutations: {manifest.total_mutations}")
    lines.append("")

    # Skipped rules
    if skipped_rules:
        lines.append("SKIPPED RULES")
        lines.append("-" * 80)
        for rule_id, reason in skipped_rules:
            lines.append(f"  {rule_id}: {reason}")
        lines.append("")

    # Summaries
    lines.append("SUMMARY BY RULE")
    lines.append("-" * 80)
    by_rule = manifest.get_summary_by_rule()
    for rule_id in sorted(by_rule.keys()):
        lines.append(f"  {rule_id}: {by_rule[rule_id]}")
    lines.append("")

    lines.append("SUMMARY BY CATEGORY")
    lines.append("-" * 80)
    by_cat = manifest.get_summary_by_category()
    for cat in sorted(by_cat.keys()):
        lines.append(f"  {cat}: {by_cat[cat]}")
    lines.append("")

    lines.append("SUMMARY BY DOMAIN")
    lines.append("-" * 80)
    by_domain = manifest.get_summary_by_domain()
    for domain in sorted(by_domain.keys()):
        lines.append(f"  {domain}: {by_domain[domain]}")
    lines.append("")

    # Sample errors (first 10)
    if manifest.errors:
        lines.append("SAMPLE ERRORS (first 10)")
        lines.append("-" * 80)
        for i, error in enumerate(manifest.errors[:10]):
            lines.append(f"\n  {i+1}. {error.error_id}")
            lines.append(f"     Rule: {error.rule_id}")
            lines.append(f"     Domain: {error.domain}")
            lines.append(f"     Subject: {error.usubjid}")
            lines.append(f"     Primitive: {error.primitive}")
            if error.variables_modified:
                lines.append(f"     Modified: {list(error.variables_modified.keys())}")
        lines.append("")

    # Warnings
    if warnings:
        lines.append("WARNINGS")
        lines.append("-" * 80)
        for warning in warnings:
            lines.append(f"  WARNING: {warning}")
        lines.append("")

    report_text = "\n".join(lines)

    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(report_text)
    else:
        return report_text


def copy_clean_datasets(
    source_datasets: Dict[str, pd.DataFrame],
    output_dir: Path,
) -> None:
    """
    Copy clean datasets unchanged to clean/ subdirectory.

    Args:
        source_datasets: Original clean datasets
        output_dir: Output directory (typically output/clean/)
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for domain, df in source_datasets.items():
        csv_path = output_dir / f"{domain}.csv"
        df.fillna("").to_csv(csv_path, index=False)
