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

from typing import Dict

RULE_PRIMITIVE_MAP: Dict[str, dict] = {

    # ========== dm_date (20 rules) ==========
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
    "SD1258": {
        "primitive": "populate_forbidden",
        "params": {"field": "RFSTDTC", "value": "2024-01-01"},
        "domain": "DM",
        "guard": "ARMCD in (SCRNFAIL,NOTASSGN,NOTTRT,)",
        "category": "dm_date",
    },
    "SD1342": {
        "primitive": "blank_field",
        "params": {"field": "RFXSTDTC"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD1343": {
        "primitive": "blank_field",
        "params": {"field": "RFXENDTC"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD2004": {
        "primitive": "blank_field",
        "params": {"field": "DTHDTC"},
        "domain": "DM",
        "guard": "DTHFL == Y",
        "category": "dm_date",
    },
    "SD2005": {
        "primitive": "populate_forbidden",
        "params": {"field": "DTHDTC", "value": "2024-01-01"},
        "domain": "DM",
        "guard": "DTHFL == Y",
        "category": "dm_date",
    },
    "SD1209": {
        "primitive": "blank_field",
        "params": {"field": "RFXSTDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1210": {
        "primitive": "blank_field",
        "params": {"field": "RFXENDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1213": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFXSTDTC", "end_field": "RFXENDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1334": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFSTDTC", "end_field": "RFXSTDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1335": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFXENDTC", "end_field": "RFENDTC"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1366": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFSTDTC", "end_field": "DTHDTC"},
        "domain": "DM",
        "guard": "DTHFL == Y",
        "category": "dm_date",
    },
    "SD1376": {
        "primitive": "invert_date_order",
        "params": {"start_field": "RFXSTDTC", "end_field": "RFPENDTC"},
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
    "SD1121": {
        "primitive": "blank_field",
        "params": {"field": "AGE"},
        "domain": "DM",
        "category": "dm_date",
    },
    "SD1373": {
        "primitive": "populate_forbidden",
        "params": {"field": "RFSTDTC", "value": "2024-01-01"},
        "domain": "DM",
        "guard": "ARMCD in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD1375": {
        "primitive": "populate_forbidden",
        "params": {"field": "RFENDTC", "value": "2024-01-01"},
        "domain": "DM",
        "guard": "ARMCD in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",
    },
    "SD2021": {
        "primitive": "blank_field",
        "params": {"field": "RFPENDTC"},
        "domain": "DM",
        "category": "dm_date",
    },

    # ========== date_cross_domain (17 rules) ==========
    "SD0013": {
        "primitive": "invert_date_order",
        "params": {"start_field": "--STDTC", "end_field": "--ENDTC"},
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
    "SD1202": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--STDTC",
            "target": "DM",
            "target_field": "RFPENDTC",
            "mismatch_type": "date_after",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1203": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--DTC",
            "target": "DM",
            "target_field": "RFPENDTC",
            "mismatch_type": "date_after",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1204": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--ENDTC",
            "target": "DM",
            "target_field": "RFPENDTC",
            "mismatch_type": "date_after",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD0025": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--STDTC",
            "target": "DM",
            "target_field": "RFXSTDTC",
            "mismatch_type": "date_before",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD0028": {
        "primitive": "invert_date_order",
        "params": {"start_field": "--STNRLO", "end_field": "--STNRHI"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1330": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--STDTC",
            "target": "DM",
            "target_field": "RFSTDTC",
            "mismatch_type": "date_before",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD0012": {
        "primitive": "invert_date_order",
        "params": {"start_field": "--STDY", "end_field": "--ENDY"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1084": {
        "primitive": "wrong_derived",
        "params": {"field": "--DY", "offset_range": [-10, 10]},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1089": {
        "primitive": "truncate_with_derived",
        "params": {"date_field": "--STDTC", "dy_field": "--STDY"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1090": {
        "primitive": "wrong_derived",
        "params": {"field": "--STDY", "offset_range": [-10, 10]},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1093": {
        "primitive": "truncate_with_derived",
        "params": {"date_field": "--ENDTC", "dy_field": "--ENDY"},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1094": {
        "primitive": "wrong_derived",
        "params": {"field": "--ENDY", "offset_range": [-10, 10]},
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1331": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--ENDTC",
            "target": "DM",
            "target_field": "RFSTDTC",
            "mismatch_type": "date_before",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },
    "SD1319": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "--STDTC",
            "target": "DM",
            "target_field": "RFICDTC",
            "mismatch_type": "date_before",
        },
        "domain": "--",
        "category": "date_cross_domain",
    },

    # ========== age_arm (18 rules) ==========
    "SD1259": {
        "primitive": "blank_field",
        "params": {"field": "AGEU"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD0084": {
        "primitive": "set_invalid_value",
        "params": {"field": "AGE", "value": "0"},
        "domain": "DM",
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
        "params": {
            "field_a": "ARMCD",
            "field_b": "ARM",
            "expected": {"SCRNFAIL": "Screen Failure"},
        },
        "domain": "DM",
        "guard": "ARMCD == SCRNFAIL",
        "category": "age_arm",
    },
    "SD0053": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "ARMCD",
            "field_b": "ARM",
            "expected": {"NOTASSGN": "Not Assigned"},
        },
        "domain": "DM",
        "guard": "ARMCD == NOTASSGN",
        "category": "age_arm",
    },
    "SD1322": {
        "primitive": "invalid_codelist",
        "params": {"field": "COUNTRY", "valid_values": None},  # ISO 3166
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1349": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARM"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1361": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARMCD", "field_b": "ACTARM"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL, NOTASSGN)",
        "category": "age_arm",
    },
    "SD1362": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARMCD", "field_b": "ACTARM"},
        "domain": "DM",
        "guard": "ACTARMCD not in (SCRNFAIL, NOTASSGN, NOTTRT)",
        "category": "age_arm",
    },
    "SD2001": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARM"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "age_arm",
    },
    "SD2019": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARMCD", "field_b": "ACTARM"},
        "domain": "DM",
        "guard": "ACTARMCD not in (SCRNFAIL,NOTASSGN,NOTTRT,)",
        "category": "age_arm",
    },
    "SD2236": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARMCD", "field_b": "ARMCD"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD2237": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ACTARM", "field_b": "ARM"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1004": {
        "primitive": "set_invalid_value",
        "params": {"field": "ARMCD", "value": "TOOLONGARMCODEVALUE12345"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1033": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARM"},
        "domain": "DM",
        "category": "age_arm",
    },
    "SD1034": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARM", "field_b": "ARMCD"},
        "domain": "DM",
        "category": "age_arm",
    },
    "CT2001": {
        "primitive": "invalid_codelist",
        "params": {"field": "--TESTCD", "valid_values": None},
        "domain": "--",
        "category": "age_arm",
    },

    # ========== dm_cross_domain (12 rules) ==========
    "SD1208": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "RFXSTDTC",
            "target": "EX",
            "target_field": "EXSTDTC",
            "mismatch_type": "not_equal_min",
        },
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    "SD1001": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "SUBJID"]},
        "domain": "DM",
        "category": "dm_cross_domain",
    },
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
    "SD0071": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ARMCD,ARM",
            "target": "TA",
            "target_field": "ARMCD,ARM",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    "SD0083": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID"]},
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    "SD2002": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ACTARMCD",
            "target": "TA",
            "target_field": "ARMCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    "SD2003": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ARMCD",
            "target": "TA",
            "target_field": "ARM",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "DM",
        "category": "dm_cross_domain",
    },
    "SD0064": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "USUBJID",
            "target": "DM",
            "target_field": "USUBJID",
            "mismatch_type": "not_in_set",
        },
        "domain": "--",
        "category": "dm_cross_domain",
    },
    "SD0065": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "VISIT,VISITNUM",
            "target": "SV",
            "target_field": "VISIT,VISITNUM",
            "mismatch_type": "not_in_set",
        },
        "domain": "--",
        "category": "dm_cross_domain",
    },
    "SD1014": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SV",
            "source_field": "TAETORD",
            "target": "TA",
            "target_field": "TAETORD",
            "mismatch_type": "not_in_set",
        },
        "domain": "SV",
        "category": "dm_cross_domain",
    },
    "SD1015": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "EPOCH",
            "target": "TA",
            "target_field": "EPOCH",
            "mismatch_type": "not_in_set",
        },
        "domain": "--",
        "category": "dm_cross_domain",
    },
    "SD1023": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "VISIT,VISITNUM",
            "target": "TV",
            "target_field": "VISIT,VISITNUM",
            "mismatch_type": "not_in_set",
        },
        "domain": "--",
        "category": "dm_cross_domain",
    },

    # ========== special_purpose (84 rules) ==========
    "SD0038": {
        "primitive": "set_invalid_value",
        "params": {"field": "--DY", "value": "0"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0093": {
        "primitive": "blank_field",
        "params": {"field": "AGEU"},
        "domain": "DM",
        "guard": "AGE != ",
        "category": "special_purpose",
    },
    "SD1358": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "ARMCD", "field_b": "ARMNRS"},
        "domain": "DM",
        "guard": "ARMCD != ",
        "category": "special_purpose",
    },
    "SD1359": {
        "primitive": "blank_field",
        "params": {"field": "ARMNRS"},
        "domain": "DM",
        "guard": "ARMCD == ",
        "category": "special_purpose",
    },
    "SD1360": {
        "primitive": "populate_forbidden",
        "params": {"field": "ARMNRS", "value": "NOTASSGN"},
        "domain": "DM",
        "guard": "ARMCD != ",
        "category": "special_purpose",
    },
    "SD1363": {
        "primitive": "populate_forbidden",
        "params": {"field": "ARMCD", "value": "ACTIVE"},
        "domain": "DM",
        "guard": "ARMCD == NOTASSGN",
        "category": "special_purpose",
    },
    "SD1364": {
        "primitive": "populate_forbidden",
        "params": {"field": "ACTARMCD", "value": "ACTIVE"},
        "domain": "DM",
        "guard": "ACTARMCD == NOTTRT",
        "category": "special_purpose",
    },
    "SD2020": {
        "primitive": "populate_forbidden",
        "params": {"field": "AGETXT", "value": "18-65"},
        "domain": "DM",
        "guard": "AGE != ",
        "category": "special_purpose",
    },
    "SD2022": {
        "primitive": "blank_field",
        "params": {"field": "RFICDTC"},
        "domain": "DM",
        "category": "special_purpose",
    },
    "SD0069": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "DS", "key": "USUBJID"},
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "special_purpose",
    },
    "SD0070": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "EX", "key": "USUBJID"},
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,) and ACTARMCD != NOTTRT",
        "category": "special_purpose",
    },
    "SD1032": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "IE", "key": "USUBJID"},
        "guard": "ARMCD == SCRNFAIL",
        "category": "special_purpose",
    },
    "SD1240": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "DS", "key": "USUBJID"},
        "category": "special_purpose",
    },
    "SD1374": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "DS", "key": "USUBJID"},
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "special_purpose",
    },
    "SD1377": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "IE", "key": "USUBJID"},
        "guard": "ARMCD == SCRNFAIL",
        "category": "special_purpose",
    },
    "SD1316": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "SS", "target": "DS", "key": "USUBJID"},
        "guard": "SSSTRESC == DEAD",
        "category": "special_purpose",
    },
    "SD0007": {
        "primitive": "blank_field",
        "params": {"field": "--ORRES"},
        "domain": "--",
        "guard": "--STAT != NOT DONE",
        "category": "special_purpose",
    },
    "SD0024": {
        "primitive": "populate_forbidden",
        "params": {"field": "--REASND", "value": "EQUIPMENT FAILURE"},
        "domain": "--",
        "guard": "--STAT != NOT DONE",
        "category": "special_purpose",
    },
    "SD1043": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TESTCD", "value": "TOOLONGTESTCODE"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1212": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--STRESN", "field_b": "--STRESC"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1369": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TESTCD", "value": "INVALID!"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1370": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TEST", "value": "X" * 41},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1448": {
        "primitive": "populate_forbidden",
        "params": {"field": "--LOINC", "value": "99999-9"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD2239": {
        "primitive": "set_invalid_value",
        "params": {"field": "--STRESU", "value": "INVALID_UNIT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0002": {
        "primitive": "blank_field",
        "params": {"field": "--SEQ"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0003": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "DOMAIN", "USUBJID", "--SEQ"]},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0004": {
        "primitive": "blank_field",
        "params": {"field": "USUBJID"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0005": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--SEQ"]},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0022": {
        "primitive": "populate_forbidden",
        "params": {"field": "--STAT", "value": "NOT DONE"},
        "domain": "--",
        "guard": "--ORRES != ",
        "category": "special_purpose",
    },
    "SD0023": {
        "primitive": "blank_field",
        "params": {"field": "--STAT"},
        "domain": "--",
        "guard": "--REASND != ",
        "category": "special_purpose",
    },
    "SD0031": {
        "primitive": "blank_field",
        "params": {"field": "--STDTC"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0051": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "VISIT", "field_b": "VISITNUM"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0052": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "VISITNUM", "field_b": "VISIT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1088": {
        "primitive": "blank_field",
        "params": {"field": "--STDY"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1092": {
        "primitive": "blank_field",
        "params": {"field": "--ENDY"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1125": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--TPT", "field_b": "--TPTNUM"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1126": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--TPTNUM", "field_b": "--TPTREF"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1127": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--TPTREF", "field_b": "--ELTM"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1201": {
        "primitive": "blank_field",
        "params": {"field": "STUDYID"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1300": {
        "primitive": "set_invalid_value",
        "params": {"field": "--GRPID", "value": "X" * 41},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD2238": {
        "primitive": "set_invalid_value",
        "params": {"field": "--RESCAT", "value": "INVALID_CAT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0055": {
        "primitive": "blank_field",
        "params": {"field": "DOMAIN"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0063": {
        "primitive": "set_invalid_value",
        "params": {"field": "DOMAIN", "value": "XX"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0063A": {
        "primitive": "set_invalid_value",
        "params": {"field": "DOMAIN", "value": "TOOLONG"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1005": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "STUDYID",
            "target": "DM",
            "target_field": "STUDYID",
            "mismatch_type": "not_equal",
        },
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1011": {
        "primitive": "set_invalid_value",
        "params": {"field": "DOMAIN", "value": "LOWER"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1021": {
        "primitive": "blank_field",
        "params": {"field": "--SEQ"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1082": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TERM", "value": "X" * 201},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1095": {
        "primitive": "set_invalid_value",
        "params": {"field": "QNAM", "value": "TOOLONG_QNAM_VALUE"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1098": {
        "primitive": "set_invalid_value",
        "params": {"field": "QLABEL", "value": "X" * 41},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1124": {
        "primitive": "set_invalid_value",
        "params": {"field": "--CAT", "value": "X" * 201},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1236": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--SEQ"]},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1241": {
        "primitive": "set_invalid_value",
        "params": {"field": "--BODSYS", "value": "INVALID_BODSYS"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1242": {
        "primitive": "set_invalid_value",
        "params": {"field": "--BDSYCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1243": {
        "primitive": "set_invalid_value",
        "params": {"field": "--HLGT", "value": "INVALID_HLGT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1251": {
        "primitive": "set_invalid_value",
        "params": {"field": "--SOC", "value": "INVALID_SOC"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1287": {
        "primitive": "set_invalid_value",
        "params": {"field": "--PTCD", "value": "INVALID_PT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1288": {
        "primitive": "set_invalid_value",
        "params": {"field": "--HLGTCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1289": {
        "primitive": "set_invalid_value",
        "params": {"field": "--SOCCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1298": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TOXGR", "value": "X"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1326": {
        "primitive": "set_invalid_value",
        "params": {"field": "--LLTCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1352": {
        "primitive": "set_invalid_value",
        "params": {"field": "--STRESN", "value": "NaN"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "special_purpose",
    },
    "SD1380": {
        "primitive": "blank_field",
        "params": {"field": "STUDYID"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1440": {
        "primitive": "set_invalid_value",
        "params": {"field": "--HLT", "value": "INVALID_HLT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1441": {
        "primitive": "set_invalid_value",
        "params": {"field": "--HLTCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1442": {
        "primitive": "set_invalid_value",
        "params": {"field": "--LLT", "value": "INVALID_LLT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1443": {
        "primitive": "set_invalid_value",
        "params": {"field": "--PT", "value": "INVALID_PT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1444": {
        "primitive": "set_invalid_value",
        "params": {"field": "--LNMQ", "value": "INVALID"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1452": {
        "primitive": "set_invalid_value",
        "params": {"field": "--LLCD", "value": "INVALID_CD"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD2024": {
        "primitive": "populate_forbidden",
        "params": {"field": "POOLID", "value": "POOL001"},
        "domain": "--",
        "guard": "USUBJID != ",
        "category": "special_purpose",
    },
    "SD0015": {
        "primitive": "invert_date_order",
        "params": {"start_field": "--DUR", "end_field": "--DUR"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD0035": {
        "primitive": "blank_field",
        "params": {"field": "--ENDTC"},
        "domain": "--",
        "guard": "--ENDY != ",
        "category": "special_purpose",
    },
    "SD0042": {
        "primitive": "populate_forbidden",
        "params": {"field": "--OCCUR", "value": "Y"},
        "domain": "--",
        "guard": "--STAT == NOT DONE",
        "category": "special_purpose",
    },
    "SD0044": {
        "primitive": "blank_field",
        "params": {"field": "--STDTC"},
        "domain": "--",
        "guard": "--STDY != ",
        "category": "special_purpose",
    },
    "SD1030": {
        "primitive": "blank_field",
        "params": {"field": "RFSTDTC"},
        "domain": "DM",
        "guard": "--STRF != ",
        "category": "special_purpose",
    },
    "SD1031": {
        "primitive": "blank_field",
        "params": {"field": "RFENDTC"},
        "domain": "DM",
        "guard": "--ENRF != ",
        "category": "special_purpose",
    },
    "SD1036": {
        "primitive": "set_invalid_value",
        "params": {"field": "IDVAR", "value": "INVALID_VAR"},
        "domain": "RELREC",
        "category": "special_purpose",
    },
    "SD1037": {
        "primitive": "set_invalid_value",
        "params": {"field": "RDOMAIN", "value": "XX"},
        "domain": "RELREC",
        "category": "special_purpose",
    },
    "SD1039": {
        "primitive": "set_invalid_value",
        "params": {"field": "--CAT", "value": "--SCAT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1040": {
        "primitive": "set_invalid_value",
        "params": {"field": "--SCAT", "value": "--CAT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1041": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--CAT", "field_b": "--SCAT"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1042": {
        "primitive": "populate_forbidden",
        "params": {"field": "--DTC", "value": "2024-01-01"},
        "domain": "--",
        "guard": "--OCCUR == N",
        "category": "special_purpose",
    },
    "SD1047": {
        "primitive": "populate_forbidden",
        "params": {"field": "--DOSTXT", "value": "100 mg"},
        "domain": "--",
        "guard": "--DOSE != ",
        "category": "special_purpose",
    },
    "SD1096": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TERM", "value": "X" * 201},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1097": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SUPPAE",
            "source_field": "IDVARVAL",
            "target": "AE",
            "target_field": "AESEQ",
            "mismatch_type": "not_in_set",
        },
        "domain": "SUPPAE",
        "category": "special_purpose",
    },
    "SD1135": {
        "primitive": "set_invalid_value",
        "params": {"field": "EPOCH", "value": "INVALID_EPOCH"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1143": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "AE", "target": "SUPPAE", "key": "USUBJID"},
        "guard": "AESMIE == Y",
        "category": "special_purpose",
    },
    "SD1262": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SS",
            "source_field": "SSDTC",
            "target": "DS",
            "target_field": "DSSTDTC",
            "mismatch_type": "date_before",
        },
        "domain": "SS",
        "guard": "SSSTRESC == DEAD",
        "category": "special_purpose",
    },
    "SD1274": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TERM", "value": "OTHER"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1277": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TERM", "value": "MULTIPLE"},
        "domain": "--",
        "category": "special_purpose",
    },
    "SD1318": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "DM", "target": "DV", "key": "USUBJID"},
        "guard": "DTHFL == Y",
        "category": "special_purpose",
    },
    "SD1321": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "AE", "target": "SUPPAE", "key": "USUBJID"},
        "category": "special_purpose",
    },

    # ========== interventions (9 rules) ==========
    "SD1292": {
        "primitive": "blank_field",
        "params": {"field": "--ROUTE"},
        "domain": "--",
        "category": "interventions",
    },
    "SD1247": {
        "primitive": "blank_field",
        "params": {"field": "--DOSE"},
        "domain": "--",
        "category": "interventions",
    },
    "SD1248": {
        "primitive": "set_invalid_value",
        "params": {"field": "ECDOSE", "value": "10"},
        "domain": "EC",
        "guard": "ECOCCUR == N",
        "category": "interventions",
    },
    "SD1279": {
        "primitive": "blank_field",
        "params": {"field": "--DOSU"},
        "domain": "--",
        "guard": "--DOSE != ",
        "category": "interventions",
    },
    "SD0014": {
        "primitive": "blank_field",
        "params": {"field": "--TRTV"},
        "domain": "--",
        "category": "interventions",
    },
    "SD0041": {
        "primitive": "populate_forbidden",
        "params": {"field": "--OCCUR", "value": "Y"},
        "domain": "--",
        "guard": "--PRESP != Y",
        "category": "interventions",
    },
    "SD0043": {
        "primitive": "blank_field",
        "params": {"field": "--DOSE"},
        "domain": "--",
        "guard": "--OCCUR == Y",
        "category": "interventions",
    },
    "SD1273": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TRT", "value": "OTHER"},
        "domain": "--",
        "category": "interventions",
    },
    "SD1276": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TRT", "value": "MULTIPLE"},
        "domain": "--",
        "category": "interventions",
    },

    # ========== ae_events (8 rules) ==========
    "SD1254": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "AE",
            "source_field": "AEOUT",
            "target": "DM",
            "target_field": "DTHFL",
            "mismatch_type": "set_source_expect_target",
            "source_value": "FATAL",
            "expected_target": "Y",
        },
        "domain": "AE",
        "category": "ae_events",
    },
    "SD1255": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "AE",
            "source_field": "AESDTH",
            "target": "DM",
            "target_field": "DTHFL",
            "source_value": "Y",
            "expected_target": "Y",
        },
        "domain": "AE",
        "category": "ae_events",
    },
    "SD1347": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "AE",
            "source_field": "AEENDTC",
            "target": "DM",
            "target_field": "DTHDTC",
            "mismatch_type": "not_equal",
        },
        "domain": "AE",
        "guard": "AEOUT == FATAL",
        "category": "ae_events",
    },
    "SD1256": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DS",
            "source_field": "DSDECOD",
            "target": "DM",
            "target_field": "DTHFL",
            "source_value": "DEATH",
            "expected_target": "Y",
        },
        "domain": "DS",
        "category": "ae_events",
    },
    "SD1317": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DS",
            "source_field": "DSSTDTC",
            "target": "DM",
            "target_field": "DTHDTC",
            "mismatch_type": "not_equal",
        },
        "domain": "DS",
        "guard": "DSDECOD == DEATH",
        "category": "ae_events",
    },
    "SD1252": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SS",
            "source_field": "SSSTRESC",
            "target": "DM",
            "target_field": "DTHFL",
            "source_value": "DEAD",
            "expected_target": "Y",
        },
        "domain": "SS",
        "category": "ae_events",
    },
    "SD1261": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SS",
            "source_field": "SSDTC",
            "target": "DM",
            "target_field": "DTHDTC",
            "mismatch_type": "date_before",
        },
        "domain": "SS",
        "guard": "SSSTRESC == DEAD",
        "category": "ae_events",
    },
    "SD1253": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DD",
            "source_field": "USUBJID",
            "target": "DM",
            "target_field": "DTHFL",
            "mismatch_type": "absent",
        },
        "domain": "DD",
        "category": "ae_events",
    },

    # ========== events (27 rules) ==========
    "SD1367": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "DSSCAT", "EPOCH"]},
        "domain": "DS",
        "category": "events",
    },
    "SD1118": {
        "primitive": "set_invalid_value",
        "params": {"field": "AESER", "value": "Y"},
        "domain": "AE",
        "guard": "AESDTH != Y and AESLIFE != Y and AESHOSP != Y and AESDISAB != Y and AESCONG != Y and AESOD != Y",
        "category": "events",
    },
    "SD1249": {
        "primitive": "set_invalid_value",
        "params": {"field": "EXDOSE", "value": "0"},
        "domain": "EX",
        "guard": "EXTRT == PLACEBO",
        "category": "events",
    },
    "SD1449": {
        "primitive": "set_invalid_value",
        "params": {"field": "AESEV", "value": "INVALID_SEV"},
        "domain": "AE",
        "category": "events",
    },
    "SD0009": {
        "primitive": "blank_field",
        "params": {"field": "AESDTH"},
        "domain": "AE",
        "guard": "AESER == Y",
        "category": "events",
    },
    "SD0079": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ARMCD",
            "target": "EX",
            "target_field": "USUBJID",
            "mismatch_type": "present_when_absent",
            "source_value": "SCRNFAIL",
        },
        "domain": "DM",
        "guard": "ARMCD == SCRNFAIL",
        "category": "events",
    },
    "SD0080": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "AE",
            "source_field": "AESTDTC",
            "target": "DS",
            "target_field": "DSSTDTC",
            "mismatch_type": "date_after",
        },
        "domain": "AE",
        "category": "events",
    },
    "SD0082": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "EX",
            "source_field": "EXENDTC",
            "target": "DS",
            "target_field": "DSSTDTC",
            "mismatch_type": "date_after",
        },
        "domain": "EX",
        "category": "events",
    },
    "SD0090": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "AEOUT",
            "field_b": "AESDTH",
            "expected": {"FATAL": "Y"},
        },
        "domain": "AE",
        "guard": "AEOUT == FATAL",
        "category": "events",
    },
    "SD0091": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "AESDTH",
            "field_b": "AEOUT",
            "expected": {"Y": "FATAL"},
        },
        "domain": "AE",
        "guard": "AESDTH == Y",
        "category": "events",
    },
    "SD1035": {
        "primitive": "blank_field",
        "params": {"field": "DSDECOD"},
        "domain": "DS",
        "category": "events",
    },
    "SD1062": {
        "primitive": "set_invalid_value",
        "params": {"field": "AESER", "value": "Y"},
        "domain": "AE",
        "guard": "AESOD == Y",
        "category": "events",
    },
    "SD1105": {
        "primitive": "populate_forbidden",
        "params": {"field": "EPOCH", "value": "TREATMENT"},
        "domain": "DS",
        "guard": "DSCAT == PROTOCOL MILESTONE",
        "category": "events",
    },
    "SD1132": {
        "primitive": "set_invalid_value",
        "params": {"field": "AESER", "value": "N"},
        "domain": "AE",
        "guard": "AESDTH == Y",
        "category": "events",
    },
    "SD1144": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "MH",
            "source_field": "MHSTDTC",
            "target": "DM",
            "target_field": "RFSTDTC",
            "mismatch_type": "date_after",
        },
        "domain": "MH",
        "category": "events",
    },
    "SD1205": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "EX",
            "source_field": "EXSTDTC",
            "target": "DM",
            "target_field": "RFXSTDTC",
            "mismatch_type": "date_before",
        },
        "domain": "EX",
        "category": "events",
    },
    "SD1206": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "EX",
            "source_field": "EXSTDTC",
            "target": "DM",
            "target_field": "RFXENDTC",
            "mismatch_type": "date_after",
        },
        "domain": "EX",
        "category": "events",
    },
    "SD1207": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "EX",
            "source_field": "EXENDTC",
            "target": "DM",
            "target_field": "RFXENDTC",
            "mismatch_type": "date_after",
        },
        "domain": "EX",
        "category": "events",
    },
    "SD1290": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "DSCAT", "DSSCAT"]},
        "domain": "DS",
        "category": "events",
    },
    "SD1313": {
        "primitive": "set_invalid_value",
        "params": {"field": "DSTERM", "value": "COMPLETED"},
        "domain": "DS",
        "guard": "DSDECOD != COMPLETED",
        "category": "events",
    },
    "SD1314": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "DSTERM",
            "field_b": "DSDECOD",
            "expected": {"COMPLETED": "COMPLETED"},
        },
        "domain": "DS",
        "guard": "DSTERM == COMPLETED",
        "category": "events",
    },
    "SD1315": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "DSDECOD", "field_b": "DSTERM"},
        "domain": "DS",
        "category": "events",
    },
    "SD1332": {
        "primitive": "populate_forbidden",
        "params": {"field": "AEENDTC", "value": "2024-01-01"},
        "domain": "AE",
        "guard": "AEOUT not in (RECOVERED,RECOVERED/RESOLVED,)",
        "category": "events",
    },
    "SD1333": {
        "primitive": "blank_field",
        "params": {"field": "AEENDTC"},
        "domain": "AE",
        "guard": "AEOUT == RECOVERED/RESOLVED",
        "category": "events",
    },
    "SD1340": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "DM",
            "source_field": "ACTARMCD",
            "target": "EX",
            "target_field": "USUBJID",
            "mismatch_type": "present_when_absent",
            "source_value": "NOTTRT",
        },
        "domain": "DM",
        "guard": "ACTARMCD == NOTTRT",
        "category": "events",
    },
    "SD1346": {
        "primitive": "blank_field",
        "params": {"field": "AEDECOD"},
        "domain": "AE",
        "category": "events",
    },
    "SD1446": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "EX",
            "source_field": "EXSTDTC",
            "target": "DS",
            "target_field": "DSSTDTC",
            "mismatch_type": "date_after",
        },
        "domain": "EX",
        "category": "events",
    },

    # ========== findings_individual (22 rules) ==========
    "SD0047": {
        "primitive": "blank_field",
        "params": {"field": "--ORRES"},
        "domain": "--",
        "guard": "--STAT != NOT DONE",
        "category": "findings_individual",
    },
    "SD0040": {
        "primitive": "populate_forbidden",
        "params": {"field": "--ORRES", "value": "999"},
        "domain": "--",
        "guard": "--STAT == NOT DONE",
        "category": "findings_individual",
    },
    "SD1137": {
        "primitive": "populate_forbidden",
        "params": {"field": "--DRVFL", "value": "Y"},
        "domain": "--",
        "guard": "--ORRES != ",
        "category": "findings_individual",
    },
    "SD1138": {
        "primitive": "populate_forbidden",
        "params": {"field": "--DRVFL", "value": "Y"},
        "domain": "--",
        "guard": "--ORRES != ",
        "category": "findings_individual",
    },
    "SD0016": {
        "primitive": "blank_field",
        "params": {"field": "--STRESU"},
        "domain": "--",
        "guard": "--STRESN != ",
        "category": "findings_individual",
    },
    "SD0017": {
        "primitive": "blank_field",
        "params": {"field": "--STRESN"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "findings_individual",
    },
    "SD0018": {
        "primitive": "blank_field",
        "params": {"field": "--STRESC"},
        "domain": "--",
        "guard": "--ORRES != ",
        "category": "findings_individual",
    },
    "SD0026": {
        "primitive": "blank_field",
        "params": {"field": "--ORNRLO"},
        "domain": "--",
        "guard": "--ORNRHI != ",
        "category": "findings_individual",
    },
    "SD0027": {
        "primitive": "blank_field",
        "params": {"field": "--ORNRHI"},
        "domain": "--",
        "guard": "--ORNRLO != ",
        "category": "findings_individual",
    },
    "SD0029": {
        "primitive": "blank_field",
        "params": {"field": "--STNRLO"},
        "domain": "--",
        "guard": "--STNRHI != ",
        "category": "findings_individual",
    },
    "SD0030": {
        "primitive": "blank_field",
        "params": {"field": "--STNRHI"},
        "domain": "--",
        "guard": "--STNRLO != ",
        "category": "findings_individual",
    },
    "SD0036": {
        "primitive": "populate_forbidden",
        "params": {"field": "--NRIND", "value": "HIGH"},
        "domain": "--",
        "guard": "--STRESN == ",
        "category": "findings_individual",
    },
    "SD0045": {
        "primitive": "blank_field",
        "params": {"field": "--STRESC"},
        "domain": "--",
        "guard": "--STRESN != ",
        "category": "findings_individual",
    },
    "SD1122": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--STRESN", "field_b": "--STRESC"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "findings_individual",
    },
    "SD1123": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--STRESC", "field_b": "--STRESN"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "findings_individual",
    },
    "SD1272": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TESTCD", "value": "OTHER"},
        "domain": "--",
        "category": "findings_individual",
    },
    "SD1275": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TESTCD", "value": "MULTIPLE"},
        "domain": "--",
        "category": "findings_individual",
    },
    "SD1320": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--ORRES", "field_b": "--STRESC"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "findings_individual",
    },
    "SD1353": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "--STRESC", "field_b": "--ORRES"},
        "domain": "--",
        "guard": "--STRESU != ",
        "category": "findings_individual",
    },
    "SD1029": {
        "primitive": "set_invalid_value",
        "params": {"field": "--TERM", "value": "Non-ASCII\x80value"},
        "domain": "--",
        "category": "findings_individual",
    },
    "SD1078": {
        "primitive": "blank_field",
        "params": {"field": "--TEST"},
        "domain": "--",
        "category": "findings_individual",
    },
    "SD1116": {
        "primitive": "blank_field",
        "params": {"field": "--TESTCD"},
        "domain": "--",
        "category": "findings_individual",
    },

    # ========== findings_chain_1 (1 rule) ==========
    "SD1117": {
        "primitive": "blank_field",
        "params": {"field": "--ORRES"},
        "domain": "--",
        "guard": "--STAT != NOT DONE",
        "category": "findings_chain_1",
    },

    # ========== findings_chain_2 (6 rules) ==========
    "SD1131": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--TESTCD", "--VISITNUM", "--BLFL"]},
        "domain": "--",
        "guard": "--BLFL == Y",
        "category": "findings_chain_2",
    },
    "SD1371": {
        "primitive": "set_invalid_value",
        "params": {"field": "--NRIND", "value": "INVALID"},
        "domain": "--",
        "category": "findings_chain_2",
    },
    "SD1372": {
        "primitive": "blank_field",
        "params": {"field": "--NRIND"},
        "domain": "--",
        "guard": "--STNRHI != ",
        "category": "findings_chain_2",
    },
    "SD1439": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--TESTCD", "--BLFL"]},
        "domain": "--",
        "guard": "--BLFL == Y",
        "category": "findings_chain_2",
    },
    "SD1445": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--TESTCD", "--LOBXFL"]},
        "domain": "--",
        "guard": "--LOBXFL == Y",
        "category": "findings_chain_2",
    },
    "SD0006": {
        "primitive": "cross_domain_orphan",
        "params": {"source": "--", "target": "--", "key": "USUBJID"},
        "guard": "--BLFL == Y",
        "category": "findings_chain_2",
    },

    # ========== general_obs_1 (11 rules) ==========
    "SD0032": {
        "primitive": "blank_field",
        "params": {"field": "--TPT"},
        "domain": "--",
        "guard": "--TPTNUM != ",
        "category": "general_obs_1",
    },
    "SD0033": {
        "primitive": "blank_field",
        "params": {"field": "--TPTNUM"},
        "domain": "--",
        "guard": "--TPT != ",
        "category": "general_obs_1",
    },
    "SD0034": {
        "primitive": "blank_field",
        "params": {"field": "--TPTREF"},
        "domain": "--",
        "guard": "--TPTNUM != ",
        "category": "general_obs_1",
    },
    "SD0049": {
        "primitive": "blank_field",
        "params": {"field": "--STTPT"},
        "domain": "--",
        "guard": "--STRTPT != ",
        "category": "general_obs_1",
    },
    "SD0050": {
        "primitive": "blank_field",
        "params": {"field": "--ENTPT"},
        "domain": "--",
        "guard": "--ENRTPT != ",
        "category": "general_obs_1",
    },
    "SD1238": {
        "primitive": "blank_field",
        "params": {"field": "--ELTM"},
        "domain": "--",
        "guard": "--TPTREF != ",
        "category": "general_obs_1",
    },
    "SD1244": {
        "primitive": "blank_field",
        "params": {"field": "--TPTREF"},
        "domain": "--",
        "guard": "--ELTM != ",
        "category": "general_obs_1",
    },
    "SD1291": {
        "primitive": "blank_field",
        "params": {"field": "--REASND"},
        "domain": "--",
        "guard": "--STAT == NOT DONE",
        "category": "general_obs_1",
    },
    "SD1017": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SV",
            "source_field": "VISITNUM",
            "target": "TV",
            "target_field": "VISITNUM",
            "mismatch_type": "not_in_set",
        },
        "domain": "SV",
        "category": "general_obs_1",
    },
    "SD1018": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SV",
            "source_field": "VISITNUM,VISIT,VISITDY",
            "target": "TV",
            "target_field": "VISITNUM,VISIT,VISITDY",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "SV",
        "category": "general_obs_1",
    },
    "SD1019": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source_field": "VISITNUM",
            "target": "TV",
            "target_field": "VISITNUM",
            "mismatch_type": "not_in_set",
        },
        "domain": "--",
        "category": "general_obs_1",
    },

    # ========== general_obs_2 (2 rules) ==========
    "SD1339": {
        "primitive": "blank_field",
        "params": {"field": "EPOCH"},
        "domain": "--",
        "guard": "--DTC != ",
        "category": "general_obs_2",
    },
    "CT2002": {
        "primitive": "invalid_codelist",
        "params": {"field": "--TESTCD", "valid_values": None},
        "domain": "--",
        "category": "general_obs_2",
    },

    # ========== relationship (20 rules) ==========
    "SD1128": {
        "primitive": "set_invalid_value",
        "params": {"field": "RELTYPE", "value": "INVALID_TYPE"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1130": {
        "primitive": "blank_field",
        "params": {"field": "IDVARVAL"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1264": {
        "primitive": "set_invalid_value",
        "params": {"field": "RDOMAIN", "value": "ZZ"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1265": {
        "primitive": "blank_field",
        "params": {"field": "RDOMAIN"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1327": {
        "primitive": "set_invalid_value",
        "params": {"field": "RSUBJID", "value": "POOL001"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1328": {
        "primitive": "set_invalid_value",
        "params": {"field": "RSUBJID", "value": "USUBJID_COPY"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1365": {
        "primitive": "set_invalid_value",
        "params": {"field": "ADSLFLG", "value": "ADSLVAR"},
        "domain": "--",
        "category": "relationship",
    },
    "SD2006": {
        "primitive": "set_invalid_value",
        "params": {"field": "QNAM", "value": "MEDDRA_CODE"},
        "domain": "SUPPAE",
        "category": "relationship",
    },
    "SD0046": {
        "primitive": "blank_field",
        "params": {"field": "IDVAR"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD0086": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "RDOMAIN", "IDVAR", "IDVARVAL"]},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1120": {
        "primitive": "populate_forbidden",
        "params": {"field": "QVAL", "value": "See comments"},
        "domain": "SUPPAE",
        "category": "relationship",
    },
    "SD0072": {
        "primitive": "set_invalid_value",
        "params": {"field": "RDOMAIN", "value": "ZZ"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD0075": {
        "primitive": "set_invalid_value",
        "params": {"field": "IDVAR", "value": "INVALIDVAR"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD0077": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "RELREC",
            "source_field": "USUBJID,IDVARVAL",
            "target": "AE",
            "target_field": "USUBJID,AESEQ",
            "mismatch_type": "not_in_set",
        },
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1022": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SV",
            "source_field": "VISITNUM,VISIT",
            "target": "TV",
            "target_field": "VISITNUM,VISIT",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "SV",
        "category": "relationship",
    },
    "SD1026": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SE",
            "source_field": "ETCD",
            "target": "TE",
            "target_field": "ETCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "SE",
        "category": "relationship",
    },
    "SD1051": {
        "primitive": "set_invalid_value",
        "params": {"field": "IDVARVAL", "value": "99999"},
        "domain": "RELREC",
        "category": "relationship",
    },
    "SD1066": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SE",
            "source_field": "ETCD,ELEMENT",
            "target": "TE",
            "target_field": "ETCD,ELEMENT",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "SE",
        "category": "relationship",
    },
    "SD1067": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "IE",
            "source_field": "IETESTCD",
            "target": "TI",
            "target_field": "IETESTCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "IE",
        "category": "relationship",
    },
    "SD1072": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "IE",
            "source_field": "IETESTCD,IETEST",
            "target": "TI",
            "target_field": "IETESTCD,IETEST",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "IE",
        "category": "relationship",
    },

    # ========== ts_domain (26 rules) ==========
    "SD1069": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD != ",
        "category": "ts_domain",
    },
    "SD1070": {
        "primitive": "blank_field",
        "params": {"field": "TSPARMCD"},
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD1278": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "TSVAL", "field_b": "TSVALCD"},
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2252": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "CURTRT",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2254": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "COMPTRT",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2255": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "COMPTRT",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2259": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "INDIC",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2262": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "TRT",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2265": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "PCLAS",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2269": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "TDIGRP",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD2251": {
        "primitive": "mismatch_pair",
        "params": {
            "field_a": "TSVAL",
            "field_b": "TSVALCD",
            "filter_field": "TSPARMCD",
            "filter_value": "SEXPOP",
        },
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD1260": {
        "primitive": "blank_field",
        "params": {"field": "TSVCDVER"},
        "domain": "TS",
        "guard": "TSVCDREF != ",
        "category": "ts_domain",
    },
    "SD1306": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TDIGRP",
        "category": "ts_domain",
    },
    "SD1307": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TTYPE",
        "category": "ts_domain",
    },
    "SD1308": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TPHASE",
        "category": "ts_domain",
    },
    "SD1309": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TCNTRL",
        "category": "ts_domain",
    },
    "SD1310": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TINDTP",
        "category": "ts_domain",
    },
    "SD1311": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TITLE",
        "category": "ts_domain",
    },
    "SD1312": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == OBJPRIM",
        "category": "ts_domain",
    },
    "SD2017": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSVALNF == ",
        "category": "ts_domain",
    },
    "SD2018": {
        "primitive": "populate_forbidden",
        "params": {"field": "TSVAL", "value": "POPULATED"},
        "domain": "TS",
        "guard": "TSVALNF != ",
        "category": "ts_domain",
    },
    "SD1297": {
        "primitive": "populate_forbidden",
        "params": {"field": "TSVAL", "value": "NULL_FLAVOR"},
        "domain": "TS",
        "guard": "TSVALNF != ",
        "category": "ts_domain",
    },
    "SD1268": {
        "primitive": "mismatch_pair",
        "params": {"field_a": "TSVCDVER", "field_b": "TSVCDREF"},
        "domain": "TS",
        "guard": "TSVCDVER != ",
        "category": "ts_domain",
    },
    "SD2283": {
        "primitive": "invalid_codelist",
        "params": {"field": "TSVALNF", "valid_values": None},  # ISO 21090 NullFlavor
        "domain": "TS",
        "category": "ts_domain",
    },
    "SD0019": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == TBLIND",
        "category": "ts_domain",
    },
    "SD0020": {
        "primitive": "blank_field",
        "params": {"field": "TSVAL"},
        "domain": "TS",
        "guard": "TSPARMCD == STOPRULE",
        "category": "ts_domain",
    },

    # ========== ts_parmcd_req (43 rules) ==========
    # delete_row: delete the TS row where TSPARMCD = target value
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
    "SD2203": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "AGEMIN"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2204": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "LENGTH"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2205": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "PLANSUB"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2206": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "RANDOM"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2207": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SEXPOP"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2208": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "STOPRULE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2209": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TBLIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2210": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TCNTRL"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2211": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TDIGRP"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2212": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TINDTP"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2213": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TITLE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2214": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TPHASE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2215": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TTYPE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2216": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "CURTRT"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2217": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "OBJPRIM"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2218": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SPONSOR"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2219": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "TRT"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2221": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "REGID"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2222": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "OUTMSPRI"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2223": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "PCLAS"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2224": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "FCNTRY"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2225": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "ADAPT"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2226": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "DCUTDTC"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2227": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "DCUTDESC"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2228": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "INTMODEL"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2229": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "NARMS"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2230": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "STYPE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2231": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "INTTYPE"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2232": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SSTDTC"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2233": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SENDTC"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2234": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "ACTSUB"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2235": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "HLTSUBJI"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2273": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "EXTTIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2274": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "NCOHORT"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2275": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "OBJSEC"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2276": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "PDPSTIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2277": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "PDSTIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2278": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "PIPIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2279": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "RDIND"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2280": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SDTIGVER"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2281": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "SDTMVER"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },
    "SD2282": {
        "primitive": "delete_row",
        "params": {"filter_field": "TSPARMCD", "filter_value": "THERAREA"},
        "domain": "TS",
        "category": "ts_parmcd_req",
    },

    # ========== ts_parmcd_limit (9 rules) ==========
    # duplicate_record: duplicate the TS row for the given TSPARMCD (violates "only one allowed")
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
    "SD1216": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "AGEMAX",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1218": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "AGEMIN",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1220": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "LENGTH",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1222": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "PLANSUB",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1224": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "RANDOM",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1225": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "SEXPOP",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1227": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD"],
            "filter_field": "TSPARMCD",
            "filter_value": "NARMS",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },
    "SD1038": {
        "primitive": "duplicate_record",
        "params": {
            "key_fields": ["TSPARMCD", "TSSEQ"],
            "filter_field": "TSPARMCD",
            "filter_value": "TITLE",
        },
        "domain": "TS",
        "category": "ts_parmcd_limit",
    },

    # ========== ts_tsval (31 rules) ==========
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
    "SD1217": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "AGEMIN",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1219": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "LENGTH",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1221": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "PLANSUB",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1223": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "RANDOM",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1269": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "2.5",
            "filter_field": "TSPARMCD",
            "filter_value": "RANDQT",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1295": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "ADDON",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1296": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "ADAPT",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD1323": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "XX",
            "filter_field": "TSPARMCD",
            "filter_value": "FCNTRY",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2245": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "SEXPOP",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2246": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TBLIND",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2247": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TCNTRL",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2248": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TTYPE",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2249": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "INTTYPE",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2250": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "INTMODEL",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2253": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "CURTRT",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2257": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TDIGRP",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2258": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "INDIC",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2260": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TRT",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2261": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "THERAREA",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2263": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "PCLAS",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2264": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "SDTMVER",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2267": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "SPONSOR",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2268": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "OUTMSPRI",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2240": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "ACTSUB",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2241": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "DCUTDTC",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2242": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "DCUTDESC",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2243": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "SDTIGVER",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2244": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "NARMS",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2256": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "TPHASE",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },
    "SD2266": {
        "primitive": "set_invalid_value",
        "params": {
            "field": "TSVAL",
            "value": "INVALID",
            "filter_field": "TSPARMCD",
            "filter_value": "REGID",
        },
        "domain": "TS",
        "category": "ts_tsval",
    },

    # ========== trial_design (27 rules) ==========
    "SD0085": {
        "primitive": "blank_field",
        "params": {"field": "ELEMENT"},
        "domain": "TE",
        "category": "trial_design",
    },
    "SD0092": {
        "primitive": "blank_field",
        "params": {"field": "TAETORD"},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1045": {
        "primitive": "blank_field",
        "params": {"field": "ARMCD"},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1046": {
        "primitive": "blank_field",
        "params": {"field": "ARM"},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1150": {
        "primitive": "blank_field",
        "params": {"field": "VISITNUM"},
        "domain": "TV",
        "category": "trial_design",
    },
    "SD1151": {
        "primitive": "blank_field",
        "params": {"field": "VISIT"},
        "domain": "TV",
        "category": "trial_design",
    },
    "SD0067": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SE",
            "source_field": "ETCD",
            "target": "TE",
            "target_field": "ETCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "SE",
        "category": "trial_design",
    },
    "SD0068": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "IE",
            "source_field": "IETESTCD",
            "target": "TI",
            "target_field": "IETESTCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "IE",
        "category": "trial_design",
    },
    "SD0089": {
        "primitive": "blank_field",
        "params": {"field": "ETCD"},
        "domain": "TE",
        "category": "trial_design",
    },
    "SD1009": {
        "primitive": "blank_field",
        "params": {"field": "IETESTCD"},
        "domain": "TI",
        "category": "trial_design",
    },
    "SD1010": {
        "primitive": "blank_field",
        "params": {"field": "IETEST"},
        "domain": "TI",
        "category": "trial_design",
    },
    "SD1012": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "SE",
            "source_field": "ETCD,ELEMENT",
            "target": "TE",
            "target_field": "ETCD,ELEMENT",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "SE",
        "category": "trial_design",
    },
    "SD1013": {
        "primitive": "populate_forbidden",
        "params": {"field": "TAETORD", "value": "1"},
        "domain": "TA",
        "guard": "EPOCH == UNPLANNED",
        "category": "trial_design",
    },
    "SD1016": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "IE",
            "source_field": "IETESTCD,IETEST,IECAT",
            "target": "TI",
            "target_field": "IETESTCD,IETEST,IECAT",
            "mismatch_type": "combo_not_in_set",
        },
        "domain": "IE",
        "category": "trial_design",
    },
    "SD1027": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["ETCD", "ELEMENT"]},
        "domain": "TE",
        "category": "trial_design",
    },
    "SD1050": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["ETCD"]},
        "domain": "TE",
        "category": "trial_design",
    },
    "SD1052": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["ARMCD", "TAETORD"]},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1053": {
        "primitive": "set_invalid_value",
        "params": {"field": "ARMCD", "value": "INVALID_ARM"},
        "domain": "TA",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "trial_design",
    },
    "SD1060": {
        "primitive": "blank_field",
        "params": {"field": "IECAT"},
        "domain": "TI",
        "category": "trial_design",
    },
    "SD1064": {
        "primitive": "blank_field",
        "params": {"field": "TEDUR"},
        "domain": "TE",
        "category": "trial_design",
    },
    "SD1068": {
        "primitive": "blank_field",
        "params": {"field": "VISITDY"},
        "domain": "TV",
        "category": "trial_design",
    },
    "SD1266": {
        "primitive": "blank_field",
        "params": {"field": "EPOCH"},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1267": {
        "primitive": "blank_field",
        "params": {"field": "ARM"},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1271": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "ARMCD", "TAETORD", "ETCD"]},
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1286": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "IETESTCD", "TIVERS"]},
        "domain": "TI",
        "category": "trial_design",
    },
    "SD1354": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "TA",
            "source_field": "ARMCD",
            "target": "DM",
            "target_field": "ARMCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1378": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "TA",
            "source_field": "ETCD",
            "target": "TA",
            "target_field": "ARMCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "TA",
        "category": "trial_design",
    },
    "SD1379": {
        "primitive": "cross_domain_mismatch",
        "params": {
            "source": "TA",
            "source_field": "ETCD",
            "target": "SE",
            "target_field": "ETCD",
            "mismatch_type": "not_in_set",
        },
        "domain": "TA",
        "category": "trial_design",
    },

    # ========== mandatory_domain (14 rules) ==========
    "SD1020": {
        "primitive": "drop_domain",
        "params": {"domain": "DM"},
        "category": "mandatory_domain",
    },
    "SD1111": {
        "primitive": "drop_domain",
        "params": {"domain": "TA"},
        "category": "mandatory_domain",
    },
    "SD1109": {
        "primitive": "drop_domain",
        "params": {"domain": "TE"},
        "category": "mandatory_domain",
    },
    "SD1106": {
        "primitive": "drop_domain",
        "params": {"domain": "TV"},
        "category": "mandatory_domain",
    },
    "SD1110": {
        "primitive": "drop_domain",
        "params": {"domain": "TS"},
        "category": "mandatory_domain",
    },
    "SD1107": {
        "primitive": "drop_domain",
        "params": {"domain": "SE"},
        "category": "mandatory_domain",
    },
    "SD1108": {
        "primitive": "drop_domain",
        "params": {"domain": "VS"},
        "category": "mandatory_domain",
    },
    "SD1112": {
        "primitive": "drop_domain",
        "params": {"domain": "TI"},
        "category": "mandatory_domain",
    },
    "SD1113": {
        "primitive": "drop_domain",
        "params": {"domain": "AE"},
        "category": "mandatory_domain",
    },
    "SD1115": {
        "primitive": "drop_domain",
        "params": {"domain": "EX"},
        "category": "mandatory_domain",
    },
    "SD1270": {
        "primitive": "drop_domain",
        "params": {"domain": "PC"},
        "category": "mandatory_domain",
    },
    "SD1061": {
        "primitive": "drop_domain",
        "params": {"domain": "MB"},
        "category": "mandatory_domain",
    },
    "SD1355": {
        "primitive": "drop_domain",
        "params": {"domain": "TM"},
        "category": "mandatory_domain",
    },
    "SD1356": {
        "primitive": "drop_domain",
        "params": {"domain": "TM"},
        "category": "mandatory_domain",
    },

    # ========== mandatory_variable (19 rules) ==========
    "SD0056": {
        "primitive": "drop_column",
        "params": {"column": "STUDYID"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD0057": {
        "primitive": "drop_column",
        "params": {"column": "DOMAIN"},
        "domain": "--",
        "category": "mandatory_variable",
    },
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
    "SD2270": {
        "primitive": "drop_column",
        "params": {"column": "--DTC"},
        "domain": "--",
        "guard": "--DY exists",
        "category": "mandatory_variable",
    },
    "SD2271": {
        "primitive": "drop_column",
        "params": {"column": "--STDTC"},
        "domain": "--",
        "guard": "--STDY exists",
        "category": "mandatory_variable",
    },
    "SD2272": {
        "primitive": "drop_column",
        "params": {"column": "--ENDTC"},
        "domain": "--",
        "guard": "--ENDY exists",
        "category": "mandatory_variable",
    },
    "SD1087": {
        "primitive": "drop_column",
        "params": {"column": "--STDY"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1091": {
        "primitive": "drop_column",
        "params": {"column": "--ENDY"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1099": {
        "primitive": "drop_column",
        "params": {"column": "--CAT"},
        "domain": "--",
        "guard": "--SCAT exists",
        "category": "mandatory_variable",
    },
    "SD1101": {
        "primitive": "drop_column",
        "params": {"column": "--ENTPT"},
        "domain": "--",
        "guard": "--ENRTPT exists",
        "category": "mandatory_variable",
    },
    "SD1102": {
        "primitive": "drop_column",
        "params": {"column": "--ENRTPT"},
        "domain": "--",
        "guard": "--ENTPT exists",
        "category": "mandatory_variable",
    },
    "SD1103": {
        "primitive": "drop_column",
        "params": {"column": "--STTPT"},
        "domain": "--",
        "guard": "--STRTPT exists",
        "category": "mandatory_variable",
    },
    "SD1104": {
        "primitive": "drop_column",
        "params": {"column": "--STRTPT"},
        "domain": "--",
        "guard": "--STTPT exists",
        "category": "mandatory_variable",
    },
    "SD1044": {
        "primitive": "drop_column",
        "params": {"column": "--BLFL"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1250": {
        "primitive": "drop_column",
        "params": {"column": "--TPTNUM"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1280": {
        "primitive": "drop_column",
        "params": {"column": "--TOX"},
        "domain": "--",
        "guard": "--TOXGR exists",
        "category": "mandatory_variable",
    },
    "SD1299": {
        "primitive": "drop_column",
        "params": {"column": "--STDTC"},
        "domain": "--",
        "category": "mandatory_variable",
    },
    "SD1077": {
        "primitive": "drop_column",
        "params": {"column": "--REGFL"},
        "domain": "--",
        "category": "mandatory_variable",
    },

    # ========== mandatory_variable_2 (12 rules) ==========
    "SD1147": {
        "primitive": "drop_column",
        "params": {"column": "--TESTCD"},
        "domain": "--",
        "category": "mandatory_variable_2",
    },
    "SD1245": {
        "primitive": "drop_column",
        "params": {"column": "--TPTREF"},
        "domain": "--",
        "guard": "--TPTNUM exists",
        "category": "mandatory_variable_2",
    },
    "SD1246": {
        "primitive": "drop_column",
        "params": {"column": "--ELTM"},
        "domain": "--",
        "guard": "--TPTREF exists",
        "category": "mandatory_variable_2",
    },
    "SD1282": {
        "primitive": "drop_column",
        "params": {"column": "--TPT"},
        "domain": "--",
        "guard": "--TPTNUM exists",
        "category": "mandatory_variable_2",
    },
    "SD1283": {
        "primitive": "drop_column",
        "params": {"column": "--LAT"},
        "domain": "--",
        "guard": "--LOC missing",
        "category": "mandatory_variable_2",
    },
    "SD1284": {
        "primitive": "drop_column",
        "params": {"column": "--PORTOT"},
        "domain": "--",
        "guard": "--LOC missing",
        "category": "mandatory_variable_2",
    },
    "SD1285": {
        "primitive": "drop_column",
        "params": {"column": "--DIR"},
        "domain": "--",
        "guard": "--LOC missing",
        "category": "mandatory_variable_2",
    },
    "SD1293": {
        "primitive": "drop_column",
        "params": {"column": "--REASND"},
        "domain": "--",
        "guard": "--PRESP missing",
        "category": "mandatory_variable_2",
    },
    "SD1294": {
        "primitive": "drop_column",
        "params": {"column": "--STAT"},
        "domain": "--",
        "guard": "--PRESP missing",
        "category": "mandatory_variable_2",
    },
    "SD1357": {
        "primitive": "drop_column",
        "params": {"column": "MIDSDTC"},
        "domain": "--",
        "guard": "MIDS missing",
        "category": "mandatory_variable_2",
    },
    "SD1450": {
        "primitive": "drop_column",
        "params": {"column": "--ENINT"},
        "domain": "--",
        "category": "mandatory_variable_2",
    },
    "SD1451": {
        "primitive": "drop_column",
        "params": {"column": "--STINT"},
        "domain": "--",
        "category": "mandatory_variable_2",
    },

    # ========== custom_split (1 rule) ==========
    "SD1079": {
        "primitive": "reorder_columns",
        "params": {"col_a": "STUDYID", "col_b": "DOMAIN"},
        "domain": "--",
        "category": "custom_split",
    },

    # ========== all_domains_1 (3 rules) ==========
    "SD0058": {
        "primitive": "add_column",
        "params": {"column_name": "CUSTOMVAR", "fill_value": ""},
        "domain": "--",
        "category": "all_domains_1",
    },
    "SD1073": {
        "primitive": "add_column",
        "params": {"column_name": "AEACNDEV", "fill_value": ""},
        "domain": "--",
        "category": "all_domains_1",
    },
    "SD1074": {
        "primitive": "add_column",
        "params": {"column_name": "GRPID", "fill_value": "1"},
        "domain": "--",
        "category": "all_domains_1",
    },

    # ========== all_domains_2 (2 rules) ==========
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

    # ========== device (9 rules) ==========
    "SD1234": {
        "primitive": "blank_field",
        "params": {"field": "MIDSTYPE"},
        "domain": "--",
        "category": "device",
    },
    "SD1235": {
        "primitive": "blank_field",
        "params": {"field": "MIDS"},
        "domain": "--",
        "guard": "MIDSTYPE != ",
        "category": "device",
    },
    "SD1237": {
        "primitive": "blank_field",
        "params": {"field": "MIDSDTC"},
        "domain": "--",
        "guard": "MIDS != ",
        "category": "device",
    },
    "SD1453": {
        "primitive": "duplicate_record",
        "params": {"key_fields": ["STUDYID", "USUBJID", "--SEQ"]},
        "domain": "--",
        "category": "device",
    },
    "SD1233": {
        "primitive": "drop_domain",
        "params": {"domain": "DI"},
        "category": "device",
    },
    "SD1348": {
        "primitive": "drop_domain",
        "params": {"domain": "PP"},
        "category": "device",
    },
    "SD1350": {
        "primitive": "drop_domain",
        "params": {"domain": "PP"},
        "category": "device",
    },
    "SD1351": {
        "primitive": "drop_domain",
        "params": {"domain": "PP"},
        "category": "device",
    },
    "SD1263": {
        "primitive": "drop_domain",
        "params": {"domain": "MB"},
        "category": "device",
    },

    # ========== SD0001 — delete all rows (special_purpose cross-ref) ==========
    "SD0001": {
        "primitive": "delete_row",
        "params": {"filter_field": None, "filter_value": None},
        "domain": "--",
        "category": "special_purpose",
    },

    # ========== CT2003 (age_arm) ==========
    "CT2003": {
        "primitive": "invalid_codelist",
        "params": {"field": "AGEU", "valid_values": None},
        "domain": "DM",
        "category": "age_arm",
    },

    # ========== SD0021 (special_purpose) ==========
    "SD0021": {
        "primitive": "blank_field",
        "params": {"field": "USUBJID"},
        "domain": "--",
        "category": "special_purpose",
    },

    # ========== SD1336, SD1337, SD1338 (special_purpose — auto-skipped at runtime) ==========
    "SD1336": {
        "primitive": "drop_domain",
        "params": {"domain": "TR"},
        "category": "special_purpose",
    },
    "SD1337": {
        "primitive": "drop_domain",
        "params": {"domain": "RS"},
        "category": "special_purpose",
    },
    "SD1338": {
        "primitive": "drop_domain",
        "params": {"domain": "RS"},
        "category": "special_purpose",
    },
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
        category_rules: Optional[Dict[str, List[str]]] = None,
    ) -> List[RuleSpec]:
        """
        Resolve rules from profile → categories → explicit rules.

        Args:
            profile: Profile name ('all', 'dates', etc.)
            categories: Explicit categories to include
            rules: Explicit rule IDs to include
            exclude_rules: Rule IDs to exclude
            domains: Limit to specified domains
            category_rules: Dict mapping category name → list of rule IDs (from defaults.yaml)

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
            filtered = set()
            if category_rules:
                # Use explicit category_rules from defaults.yaml
                for category in categories:
                    category_rule_ids = category_rules.get(category, [])
                    filtered.update(category_rule_ids)
            else:
                # Fallback to filtering by category field in rule_map
                categories_set = set(categories)
                for rule_id in rule_ids:
                    entry = self.rule_map.get(rule_id)
                    if not entry:
                        continue
                    rule_category = entry.get("category", "")
                    if rule_category in categories_set:
                        filtered.add(rule_id)
            # Intersect with already-filtered rules
            rule_ids = rule_ids & filtered

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
