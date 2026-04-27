"""
Rule parser for extracting rule definitions from Excel/CSV.
Parses Test_Case.csv and Error_Case.csv to build RuleSpec objects.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict
import csv
from pathlib import Path
from injection.primitives import GuardExpression


@dataclass
class RuleSpec:
    """Specification for a single validation rule."""

    rule_id: str  # e.g., "SD0013"
    primitive: str  # e.g., "invert_date_order"
    params: Dict  # Primitive-specific parameters
    domain: str  # e.g., "DM", "AE", or "--" for generic
    domain_expanded: List[str] = None  # Domains this rule applies to
    guard_expression: Optional[GuardExpression] = None
    category: str = ""
    rule_message: str = ""

    def __post_init__(self):
        if self.domain_expanded is None:
            self.domain_expanded = [self.domain]

    def is_injectable(self) -> bool:
        """Check if rule is injectable (not in skip list)."""
        non_injectable = ["SD1071", "SD9999", "SD0062", "SD1119", "SD1368"]
        return self.rule_id not in non_injectable


class RuleParser:
    """Parse rule definitions from CSV files."""

    @staticmethod
    def parse_test_case_csv(csv_path: Path) -> Dict[str, dict]:
        """
        Parse Test_Case.csv to extract rule metadata.

        Expected columns:
        - Rule_ID
        - Domain
        - Domain_Expanded
        - Rule_Message
        - Category
        - Guard_Expression

        Returns:
            Dict mapping rule_id to metadata dict
        """
        rules = {}

        if not csv_path.exists():
            return rules

        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rule_id = row.get("Rule_ID", "").strip()
                if not rule_id:
                    continue

                rules[rule_id] = {
                    "domain": row.get("Domain", "").strip(),
                    "domain_expanded": [
                        d.strip()
                        for d in row.get("Domain_Expanded", "").split(",")
                        if d.strip()
                    ] or ["--"],
                    "rule_message": row.get("Rule_Message", "").strip(),
                    "category": row.get("Category", "").strip(),
                    "guard_expression": row.get("Guard_Expression", "").strip(),
                }

        return rules

    @staticmethod
    def parse_error_case_csv(csv_path: Path) -> Dict[str, list]:
        """
        Parse Error_Case.csv for I/V test vectors.

        Expected columns:
        - Rule_ID
        - Case_ID (I-X or V-X format)
        - ... test data ...

        Returns:
            Dict mapping rule_id to list of test vectors
        """
        test_vectors = {}

        if not csv_path.exists():
            return test_vectors

        with open(csv_path, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rule_id = row.get("Rule_ID", "").strip()
                if not rule_id:
                    continue

                if rule_id not in test_vectors:
                    test_vectors[rule_id] = []

                test_vectors[rule_id].append(row)

        return test_vectors
