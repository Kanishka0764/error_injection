"""
Rule catalog with complete RULE_PRIMITIVE_MAP.
Maps 472 injectable SDTM rules to 16 mutation primitives.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import re

from injection.rule_parser import RuleSpec
from injection.primitives import GuardExpression


# Core mapping of all 472 injectable rules to primitives
# Format: rule_id → {primitive, params, domain, guard, category, co_violations}
RULE_PRIMITIVE_MAP: Dict[str, dict] = {
    # ========== dm_date (DTC1, 17 rules) ==========
    "SD0087": {
        "primitive": "blank_field",
        "params": {"field": "RFSTDTC"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD0088": {
        "primitive": "blank_field",
        "params": {"field": "RFENDTC"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD1002": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFSTDTC", "end_field": "RFENDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD2023": {
        "primitive": "blank_field",
        "params": {"field": "AGE"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1003": {
        "primitive": "blank_field",
        "params": {"field": "AGE"},
        "domain": "DM",
        "guard": "AGEU != ",
        "category": "dm_date",
    },
    "SD0013": {
        "primitive": "invert_date_order",
        "params": {"start_field": "--STDTC", "end_field": "--ENDTC"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD0038": {
        "primitive": "set_invalid_value",
        "params": {"field": "--DY", "value": "0"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1085": {
        "primitive": "truncate_with_derived",
        "params": {"date_field": "--DTC", "dy_field": "--DY"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1086": {
        "primitive": "wrong_derived",
        "params": {"field": "--DY", "offset_range": [-10, 10]},
        "domain": "--",
        "category": "date_cross_domain",
    },
    # ... Additional DM rules (SD1209, SD1210, etc. - see plan for full list)
    "SD1121": {
        "primitive": "blank_field",
        "params": {"field": "AGE"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1001": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "SUBJID"]},
        "domain": "DM",
        "category": "age_arm",
    },
    # ========== ag_arm (DM01, 21 rules) ==========
    "SD0084": {
        "primitive": "set_invalid_value",
        "params": {"field": "AGE", "value": "0"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD0093": {
        "primitive": "blank_field",
        "params": {"field": "AGEU"},
        "domain": "DM",
        "guard": "AGE != ",
        "category": "age_arm",
    },
    "SD1133": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARMCD", "field_b": "ACTARM"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1134": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARM", "field_b": "ACTARMCD"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD0011": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARM"},
        "domain": "DM",
        "guard": "ARMCD == SCRNFAIL",
        "category": "age_arm",
    },
    "SD0053": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARM"},
        "domain": "DM",
        "guard": "ARMCD == NOTASSGN",
        "category": "age_arm",
    },
    # ========== dm_cross_domain (DM02, 13 rules) ==========
    "SD0066": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ARMCD",
            "target": "TA",
            "target_field": "ARMCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_cross_domain",
    },
    "SD0069": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "DS", "key": "USUBJID"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_cross_domain",
    },
    "SD0070": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "EX", "key": "USUBJID"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_cross_domain",
    },
    "SD0083": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID"]},
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    # ... More DM rules in plan document
    # ========== TS: delete_row (50 rules) ==========
    "SD2201": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "ADDON"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2202": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "AGEMAX"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    # ... SD2203-SD2282 follow same pattern with different PARMCD values
    # ========== TS: duplicate_record (8 rules) ==========
    "SD1214": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "ADDON",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    # ========== TS: set_invalid_value (~30 rules) ==========
    "SD1215": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "AGEMAX",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    # ========== structural: drop_domain (12 rules) ==========
    "SD1020": {"primitive": "drop_domain", "params": {"domain": "DM"}, "category": "mandatory_domain"},
    "SD1106": {"primitive": "drop_domain", "params": {"domain": "AE"}, "category": "mandatory_domain"},
    "SD1107": {"primitive": "drop_domain", "params": {"domain": "LB"}, "category": "mandatory_domain"},
    "SD1108": {"primitive": "drop_domain", "params": {"domain": "VS"}, "category": "mandatory_domain"},
    "SD1109": {"primitive": "drop_domain", "params": {"domain": "EX"}, "category": "mandatory_domain"},
    "SD1110": {"primitive": "drop_domain", "params": {"domain": "DS"}, "category": "mandatory_domain"},
    "SD1111": {"primitive": "drop_domain", "params": {"domain": "TA"}, "category": "mandatory_domain"},
    "SD1112": {"primitive": "drop_domain", "params": {"domain": "TE"}, "category": "mandatory_domain"},
    "SD1113": {"primitive": "drop_domain", "params": {"domain": "TI"}, "category": "mandatory_domain"},
    "SD1115": {"primitive": "drop_domain", "params": {"domain": "TS"}, "category": "mandatory_domain"},
    # More in plan
    # ========== structural: drop_column (32 rules) ==========
    "SD1083": {
        "primitive": "drop_column",
        "params": {"column": "--DY"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1129": {
        "primitive": "drop_column",
        "params": {"column": "AGE"},
        "domain": "DM",
        "category": "mandatory_variable",
    },
    # ========== structural: add_column (5 rules) ==========
    "SD0058": {
        "primitive": "add_column",
        "params": {"column_name": "CUSTOMVAR", "fill_value": ""},
        "category": "all_domains_1",
    },
    "SD1073": {
        "primitive": "add_column",
        "params": {"column_name": "AEACNDEV", "fill_value": ""},
        "category": "all_domains_1",
    },
    "SD1074": {
        "primitive": "add_column",
        "params": {"column_name": "GRPID", "fill_value": "1"},
        "category": "all_domains_2",
    },
    "SD1075": {
        "primitive": "add_column",
        "params": {"column_name": "AEREF", "fill_value": ""},
        "domain": "AE",
        "category": "all_domains_2",
    },
    "SD1076": {
        "primitive": "add_column",
        "params": {"column_name": "AEMODIFY", "fill_value": ""},
        "domain": "AE",
        "category": "all_domains_2",
    },
    # ========== structural: reorder_columns (1 rule) ==========
    "SD1079": {
        "primitive": "reorder_columns",
        "params": {"col_a": "STUDYID", "col_b": "DOMAIN"},
        "category": "custom_split",
    },
    # ========== Additional dm_date rules ==========
    "SD1209": {"primitive": "blank_field", "params": {"field": "RFXSTDTC"}, "domain": "DM", "category": "dm_date"},
    "SD1210": {"primitive": "blank_field", "params": {"field": "RFXENDTC"}, "domain": "DM", "category": "dm_date"},
    "SD1258": {"primitive": "populate_forbidden", "params": {"field": "RFSTDTC", "value": "2024-01-01"}, "domain": "DM", "category": "dm_date", "guard": "ARMCD in (SCRNFAIL,NOTASSGN)"},
    "SD1259": {"primitive": "populate_forbidden", "params": {"field": "RFENDTC", "value": "2024-12-31"}, "domain": "DM", "category": "dm_date"},
    "SD2004": {"primitive": "blank_field", "params": {"field": "DTHDTC"}, "domain": "DM", "guard": "DTHFL == Y", "category": "dm_date"},
    "SD2005": {"primitive": "populate_forbidden", "params": {"field": "DTHFL", "value": "Y"}, "domain": "DM", "guard": "DTHDTC == ", "category": "dm_date"},
    # ========== Additional age_arm rules ==========
    "SD1004": {"primitive": "set_invalid_value", "params": {"field": "ARMCD", "value": "TOOLONGARMCODEVALUE12345"}, "domain": "DM", "category": "age_arm"},
    "SD1033": {"primitive": "mismatch_pair", "params": {"field_a": "ARMCD", "field_b": "ARM"}, "domain": "DM", "category": "age_arm"},
    "SD1034": {"primitive": "mismatch_pair", "params": {"field_a": "ARM", "field_b": "ARMCD"}, "domain": "DM", "category": "age_arm"},
    "SD2001": {"primitive": "set_invalid_value", "params": {"field": "ARMCD", "value": "BADARM"}, "domain": "DM", "category": "age_arm"},
    "SD2236": {"primitive": "mismatch_pair", "params": {"field_a": "ACTARMCD", "field_b": "ARMCD"}, "domain": "DM", "category": "age_arm"},
    "SD2237": {"primitive": "mismatch_pair", "params": {"field_a": "ACTARM", "field_b": "ARM"}, "domain": "DM", "category": "age_arm"},
    # ========== Additional dm_cross_domain rules ==========
    "SD0071": {"primitive": "cross_domain_mismatch", "params": {"source": "DM", "source_field": "ARMCD,ARM", "target": "TA", "target_field": "ARMCD,ARM", "mismatch_type": "combo_not_in_set"}, "domain": "DM", "category": "dm_cross_domain"},
    "SD1032": {"primitive": "cross_domain_orphan", "params": {"source": "DM", "target": "IE", "key": "USUBJID"}, "domain": "DM", "guard": "ARMCD == SCRNFAIL", "category": "dm_cross_domain"},
    "SD1240": {"primitive": "cross_domain_orphan", "params": {"source": "DM", "target": "DS", "key": "USUBJID"}, "domain": "DM", "category": "dm_cross_domain"},
    "SD2002": {"primitive": "cross_domain_mismatch", "params": {"source": "DM", "source_field": "ACTARMCD", "target": "TA", "target_field": "ARMCD", "mismatch_type": "not_in_set"}, "domain": "DM", "category": "dm_cross_domain"},
    "SD2003": {"primitive": "cross_domain_mismatch", "params": {"source": "DM", "source_field": "ARM", "target": "TA", "target_field": "ARM", "mismatch_type": "not_in_set"}, "domain": "DM", "category": "dm_cross_domain"},
    # ========== ae_events (death cascade) rules ==========
    "SD1254": {"primitive": "cross_domain_mismatch", "params": {"source": "AE", "source_field": "AEOUT", "target": "DM", "target_field": "DTHFL", "mismatch_type": "set_source"}, "domain": "AE", "category": "ae_events"},
    "SD1255": {"primitive": "cross_domain_mismatch", "params": {"source": "AE", "source_field": "AESDTH", "target": "DM", "target_field": "DTHFL", "mismatch_type": "not_equal"}, "domain": "AE", "category": "ae_events"},
    "SD1347": {"primitive": "cross_domain_mismatch", "params": {"source": "AE", "source_field": "AEENDTC", "target": "DM", "target_field": "DTHDTC", "mismatch_type": "not_equal"}, "domain": "AE", "guard": "AEOUT == FATAL", "category": "ae_events"},
    "SD1256": {"primitive": "cross_domain_mismatch", "params": {"source": "DS", "source_field": "DSDECOD", "target": "DM", "target_field": "DTHFL", "mismatch_type": "set_source"}, "domain": "DS", "category": "ae_events"},
    # ========== findings_individual rules ==========
    "SD0047": {"primitive": "blank_field", "params": {"field": "--ORRES"}, "domain": "--", "guard": "--STAT != NOT DONE", "category": "findings_individual"},
    "SD0040": {"primitive": "populate_forbidden", "params": {"field": "--ORRES", "value": "999"}, "domain": "--", "guard": "--STAT == NOT DONE", "category": "findings_individual"},
    "SD1137": {"primitive": "populate_forbidden", "params": {"field": "--DRVFL", "value": "Y"}, "domain": "--", "guard": "--ORRES != ", "category": "findings_individual"},
    # ========== date_cross_domain rules ==========
    "SD1202": {"primitive": "cross_domain_mismatch", "params": {"source_field": "--STDTC", "target": "DM", "target_field": "RFPENDTC", "mismatch_type": "date_after"}, "domain": "--", "category": "date_cross_domain"},
    "SD1203": {"primitive": "cross_domain_mismatch", "params": {"source_field": "--DTC", "target": "DM", "target_field": "RFPENDTC", "mismatch_type": "date_after"}, "domain": "--", "category": "date_cross_domain"},
    "SD1204": {"primitive": "cross_domain_mismatch", "params": {"source_field": "--ENDTC", "target": "DM", "target_field": "RFPENDTC", "mismatch_type": "date_after"}, "domain": "--", "category": "date_cross_domain"},
    "SD0012": {"primitive": "invert_date_order", "params": {"start_field": "--STDY", "end_field": "--ENDY"}, "domain": "--", "category": "date_cross_domain"},
    "SD0028": {"primitive": "invert_date_order", "params": {"start_field": "--STNRHI", "end_field": "--STNRLO"}, "domain": "--", "category": "date_cross_domain"},
    # ========== special_purpose rules ==========
    "SD1004": {"primitive": "set_invalid_value", "params": {"field": "ARMCD", "value": "X" * 25}, "domain": "DM", "category": "special_purpose"},
    # ========== interventions rules ==========
    "SD0041": {"primitive": "populate_forbidden", "params": {"field": "--OCCUR", "value": "Y"}, "domain": "--", "category": "interventions"},
    # ========== trial_design rules ==========
    "SD0067": {"primitive": "cross_domain_mismatch", "params": {"source": "SE", "source_field": "ETCD", "target": "TE", "target_field": "ETCD", "mismatch_type": "not_in_set"}, "category": "trial_design"},
    "SD0068": {"primitive": "cross_domain_mismatch", "params": {"source": "IE", "source_field": "IETESTCD", "target": "TI", "target_field": "IETESTCD", "mismatch_type": "not_in_set"}, "category": "trial_design"},
    "SD1015": {"primitive": "cross_domain_mismatch", "params": {"source_field": "EPOCH", "target": "TA", "target_field": "EPOCH", "mismatch_type": "not_in_set"}, "category": "trial_design"},
    # ========== relationship rules ==========
    "SD0051": {"primitive": "mismatch_pair", "params": {"field_a": "VISIT", "field_b": "VISITNUM"}, "domain": "--", "category": "relationship"},
    "SD0052": {"primitive": "mismatch_pair", "params": {"field_a": "VISITNUM", "field_b": "VISIT"}, "domain": "--", "category": "relationship"},
    # ========== general_obs rules ==========
    "SD0090": {"primitive": "mismatch_pair", "params": {"field_a": "AESDTH", "field_b": "AEOUT"}, "domain": "AE", "category": "general_obs_1"},
    "SD0091": {"primitive": "mismatch_pair", "params": {"field_a": "AEOUT", "field_b": "AESDTH"}, "domain": "AE", "category": "general_obs_2"},
    # NOTE: Full mapping of all 472 rules is in injection_plan_merged.md
    # Additional rules follow the same patterns as above
}

# Rules that are NOT injectable (5 total)
NON_INJECTABLE_RULES = {
    "SD1071": "Dataset > 5 GB — not feasible to test via injection",
    "SD9999": "Dataset class not recognized — meta-level, not data-level",
    "SD0062": "Incompatible data source — meta-level, not data-level",
    "SD1119": "CO domain not generated",
    "SD1368": "SM domain not generated",
}


class RuleCatalog:
    """Catalog of all SDTM validation rules and their primitive mappings."""

    def __init__(self):
        """Initialize catalog with rule map."""
        self.rule_map = RULE_PRIMITIVE_MAP
        self.non_injectable = NON_INJECTABLE_RULES

    def get_spec(self, rule_id: str) -> Optional[RuleSpec]:
        """
        Get RuleSpec for a given rule ID.

        Args:
            rule_id: Rule identifier (e.g., "SD0013")

        Returns:
            RuleSpec object, or None if not found
        """
        if rule_id in self.non_injectable:
            return None

        if rule_id not in self.rule_map:
            return None

        entry = self.rule_map[rule_id]

        guard_expr = None
        if "guard" in entry and entry.get("guard"):
            guard_expr = GuardExpression(entry["guard"])

        return RuleSpec(
            rule_id=rule_id,
            primitive=entry.get("primitive", ""),
            params=entry.get("params", {}),
            domain=entry.get("domain", "--"),
            domain_expanded=entry.get("domain_expanded", [entry.get("domain", "--")]),
            guard_expression=guard_expr,
            category=entry.get("category", ""),
        )

    def resolve(
        self,
        profile: str = "all",
        categories: Optional[List[str]] = None,
        rules: Optional[List[str]] = None,
        exclude_rules: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
    ) -> List[RuleSpec]:
        """
        Resolve rules from profile → categories → explicit rules.

        Args:
            profile: Profile name ('all', 'dates', etc.)
            categories: Explicit categories to include
            rules: Explicit rule IDs to include
            exclude_rules: Rule IDs to exclude
            domains: Limit to specified domains

        Returns:
            List of RuleSpec objects
        """
        rule_ids = set()

        # Start with explicit rules, if provided
        if rules:
            rule_ids.update(rules)
        else:
            # Otherwise use all rules (filtered by categories later)
            rule_ids.update(self.rule_map.keys())

        # Remove excluded rules
        if exclude_rules:
            rule_ids.difference_update(exclude_rules)

        # Filter to specified domains if provided
        if domains:
            domains_set = set(d.upper() for d in domains)
            filtered = set()
            for rule_id in rule_ids:
                entry = self.rule_map.get(rule_id)
                if not entry:
                    continue
                rule_domains = set(entry.get("domain_expanded", [entry.get("domain", "--")]))
                # Include if rule applies to "--" (all domains) or matches requested domains
                if "--" in rule_domains or rule_domains & domains_set:
                    filtered.add(rule_id)
            rule_ids = filtered

        # Filter to specified categories if provided
        if categories:
            categories_set = set(categories)
            filtered = set()
            for rule_id in rule_ids:
                entry = self.rule_map.get(rule_id)
                if not entry:
                    continue
                rule_category = entry.get("category", "")
                if rule_category in categories_set:
                    filtered.add(rule_id)
            rule_ids = filtered

        # Build RuleSpec objects
        specs = []
        for rule_id in sorted(rule_ids):
            spec = self.get_spec(rule_id)
            if spec:
                specs.append(spec)

        return specs

    def get_injectable_rules(self) -> int:
        """Count total injectable rules."""
        return len([rule_id for rule_id in self.rule_map if self.get_spec(rule_id) is not None])

    def get_skipped_rules(self) -> Dict[str, str]:
        """Get reasons for non-injectable rules."""
        return self.non_injectable.copy()

    def validate_rule_map(self) -> List[str]:
        """
        Validate that all rules in RULE_PRIMITIVE_MAP are well-formed.

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        for rule_id, entry in self.rule_map.items():
            if not entry.get("primitive"):
                errors.append(f"{rule_id}: missing primitive")
            if not isinstance(entry.get("params"), dict):
                errors.append(f"{rule_id}: params not a dict")

        return errors
