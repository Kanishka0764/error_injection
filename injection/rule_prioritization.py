"""
Rule prioritization for injection pipeline.
Sorts rules by eligible row count (ascending) to prevent starvation.

Low-volume rules (few eligible rows) are injected first to ensure they
claim rows before high-volume rules consume subjects via the density cap.
"""

from typing import List, Tuple
import pandas as pd

from injection.rule_parser import RuleSpec


def prioritize_rules(
    rule_list: List[RuleSpec],
    datasets: dict,
) -> List[RuleSpec]:
    """
    Sort rules by number of eligible rows (ascending).

    Fewest eligible rows = highest priority = runs first.

    This guarantees low-volume rules claim their rows before high-volume
    rules can consume subjects via the density cap.

    Args:
        rule_list: List of RuleSpec objects to sort
        datasets: Dict mapping domain → DataFrame (for eligibility evaluation)

    Returns:
        Sorted list[RuleSpec] in ascending order by eligible row count
    """

    def eligible_row_count(rule: RuleSpec) -> Tuple[int, str]:
        """
        Count rows eligible for a rule.

        Returns:
            Tuple of (count, rule_id) for sorting with tie-breaking
        """
        domain = rule.domain

        # Handle generic domain (applies to multiple)
        if domain == "--":
            # For generic rules, use first expanded domain
            domain = rule.domain_expanded[0] if rule.domain_expanded else "--"

        df = datasets.get(domain)

        # Edge case 1: domain not loaded → count = 0
        if df is None or df.empty:
            return (0, rule.rule_id)

        # Edge case 2: no guard → all rows are eligible
        if rule.guard_expression is None:
            return (len(df), rule.rule_id)

        # Normal case: evaluate guard and count True rows
        try:
            count = 0
            for idx in range(len(df)):
                if rule.guard_expression.evaluate(df, idx):
                    count += 1
            return (count, rule.rule_id)
        except Exception:
            # If guard evaluation fails, assume all rows eligible
            return (len(df), rule.rule_id)

    # Sort by (eligible row count, rule_id)
    # This ensures: fewest rows first, and deterministic ordering on ties
    sorted_rules = sorted(rule_list, key=eligible_row_count)

    return sorted_rules


def get_rule_priorities(
    rule_list: List[RuleSpec],
    datasets: dict,
) -> List[Tuple[str, int, str]]:
    """
    Generate priority report for each rule.

    Useful for logging and debugging.

    Args:
        rule_list: List of RuleSpec objects
        datasets: Dict mapping domain → DataFrame

    Returns:
        List of tuples: (rule_id, eligible_row_count, primitive_name)
    """

    def get_count(rule: RuleSpec) -> int:
        domain = rule.domain
        if domain == "--":
            domain = rule.domain_expanded[0] if rule.domain_expanded else "--"

        df = datasets.get(domain)
        if df is None or df.empty:
            return 0

        if rule.guard_expression is None:
            return len(df)

        try:
            count = 0
            for idx in range(len(df)):
                if rule.guard_expression.evaluate(df, idx):
                    count += 1
            return count
        except Exception:
            return len(df)

    # Sort and return with metadata
    sorted_rules = prioritize_rules(rule_list, datasets)
    report = [(r.rule_id, get_count(r), r.primitive) for r in sorted_rules]

    return report
