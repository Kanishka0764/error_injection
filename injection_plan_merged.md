# SDTM Error Injection Test Harness — Plan

## Context

The user has a validator product (Kwalify/P21) that detects errors in SDTM datasets.
They need a tool that generates **test datasets with known, labeled errors** so they can
measure their validator's detection accuracy. This is NOT an imputation system — no fixing,
no missingness repair. Pure error injection for validator testing.

**Pipeline:** Read clean CSVs → inject errors → re-derive dependent fields → output dirty CSVs + ground truth manifest

---

## Architecture: One Package, 10 Files

```
injection/
├── __init__.py               # exports run_injection()
├── engine.py                 # orchestrator: load → resolve → prioritize → inject → re-derive → validate → write
├── reader.py                 # load CSVs as Dict[str, pd.DataFrame] (dtype=str)
├── writer.py                 # write clean/ + dirty/ + manifest.json + report.txt
├── rule_parser.py            # parse block-structured Rule_Cases.csv → RuleBlock/TestVector
├── catalog.py                # RuleCatalog: rule_id → RuleSpec (primitive, params, domains)
├── rule_prioritization.py    # sort rules by eligible row count — fewest rows = highest priority
├── primitives.py             # 16 reusable mutation primitives (data-driven, not per-rule code)
├── manifest.py               # InjectionManifest + MutationRecord + JSON + score_validator()
└── config.py                 # load defaults.yaml, profiles, category rates

config/injection/
└── defaults.yaml             # profiles, category rates, density cap, skip list, prioritize_rules flag
```

Entrypoint script: run orchestration from `apply_primitive.py`.

**Key design decisions:**
1. **No cascade.py.** After injecting, the engine calls existing `ConformanceLayer` methods
   to re-derive all dependent fields from corrupted sources. Produces realistic errors.
2. **16 primitives, not 25 category functions.** Each rule maps declaratively to one primitive + params.
   Original 13 + `delete_row` (50 TS rules), `add_column` (5 structural rules), `reorder_columns` (1 rule).
3. **Domain availability check.** Auto-skip rules targeting domains not in the loaded data.
4. **5 rules are non-injectable** (SD1071 dataset size, SD9999/SD0062 meta-level, SD1119 CO domain, SD1368 SM domain).
5. **Rule prioritization.** Before the injection loop, rules are sorted by their eligible row count
   (ascending). The rule with the fewest eligible rows runs first, guaranteeing it claims rows before
   any high-volume rule can consume them via the density cap.

---

## Coverage Summary

### By the numbers

```
Total rules in Excel:               479 (477 unique + 2 version duplicates)
Rules mapped to primitives:         472  (16 primitives cover all)
Rules skipped (not feasible):         5  (SD1071, SD9999, SD0062, SD1119, SD1368)
Rules auto-skipped at runtime:      ~24  (domains not generated: CO, SM, PP, RS, TR, TU, MB, TD)

By primitive type:
  blank_field:              87     set_invalid_value:    82
  mismatch_pair:            57     delete_row:           50
  cross_domain_mismatch:    45     drop_column:          32
  populate_forbidden:       31     duplicate_record:     30
  invert_date_order:        21     drop_domain:          12
  cross_domain_orphan:       8     wrong_derived:         6
  add_column:                5     invalid_codelist:      4
  truncate_with_derived:     3     reorder_columns:       1

By level:
  Record-level rules:      400  (12 different primitives)
  Variable-level rules:     32  (drop_column + add_column + reorder_columns)
  Dataset-level rules:      12  (drop_domain)
  TS row-level rules:       50  (delete_row — conceptually record-level within TS)
```

### Auto-skipped rules (24 total — domains not generated)

These rules reference only domains we don't produce. The engine detects this
at runtime and logs them as `SKIPPED: domain not available`.

```
CO domain (6):  SD0072, SD0075, SD0077, SD1008, SD1065, SD1119
SM domain (1):  SD1368
PP domain (3):  SD1270, SD1348, SD1350, SD1351
RS domain (2):  SD1337, SD1338
TR domain (1):  SD1336
MB domain (2):  SD1061, SD1263
TD domain (1):  SD1301
TM domain (2):  SD1355, SD1356
DI domain (1):  SD1233
TU domain (1):  SD1336
```

*These may become injectable if the generator adds those domains in the future.*

### Generated domains (30)

```
AE, CM, DA, DD, DM, DS, DV, EC, EG, EX, FA, HO, IE, LB, MH,
PE, QS, RELREC, SC, SE, SS, SU, SUPPAE, SV, TA, TE, TI, TS, TV, VS
```

---

## Two Modes: Isolated vs Compound

### `--mode isolated` (one rule per dataset)

Generates N output folders, each with exactly **one rule violation**:

```
output/
├── SD0013/
│   ├── clean/        # copy of original CSVs
│   ├── dirty/        # one SD0013 violation injected + re-derived
│   └── manifest.json # exactly 1 error entry + any expected co-violations
├── SD0038/
│   ├── clean/
│   ├── dirty/
│   └── manifest.json
└── ...
```

Clean per-rule precision/recall measurement. No interaction between injected errors.

### `--mode compound` (default)

All selected rules injected into one dataset:

```
output/
├── clean/            # copy of original CSVs
├── dirty/            # all errors injected + re-derived
├── manifest.json     # all error entries
└── report.txt        # human-readable summary (includes priority order log)
```

Subject to **density cap** (max N violations per subject, default 5).

---

## Rule Prioritization (`rule_prioritization.py`)

### Why it is needed

Every rule needs valid rows to inject an error into. Different rules have different numbers
of eligible rows. For example, a rule like SD0087 (RFSTDTC blank) may only apply to 4 rows
in DM, while SD0002 (required field null) may apply to 55 rows across AE. When running in
`--mode compound`, the **density cap** limits how many violations a single subject can receive.
If a high-volume rule runs first, it may consume the subjects that a low-volume rule depends
on — the low-volume rule then finds no eligible rows and fires zero injections.

**Rule prioritization solves this:** sort all rules by their eligible row count (ascending)
before the injection loop starts. The scarcest rule runs first and claims its rows. The
most abundant rule runs last and still finds plenty of subjects.

### What `rule_prioritization.py` does — step by step

**Step 1 — Count eligible rows for each rule**

Input:
- `rule_list: list[RuleSpec]` — the list returned by `catalog.resolve()`. Each `RuleSpec`
  has: `rule_id`, `primary_domain` (e.g. `"DM"`, `"AE"`), and a `guard_expression`.
- `datasets: dict[str, pd.DataFrame]` — clean CSVs loaded by `reader.py`.

For each rule:
1. Look up `datasets[rule.primary_domain]` to get the domain DataFrame.
2. Apply `rule.guard_expression.evaluate(df)` — returns a boolean Series (True = row is eligible).
3. Count True values with `.sum()`.
4. Store as `(rule, count)` pair.

Output: a list of `(RuleSpec, int)` tuples.

```
Example:
[(SD0087, 4), (SD0013, 28), (SD0038, 9), (SD0002, 55)]
```

**Step 2 — Sort ascending by count**

Input: list of `(RuleSpec, int)` tuples from Step 1.

Sort by count ascending — fewest eligible rows first. Use `rule_id` as a secondary
sort key for determinism when two rules have the same count.

Output: sorted `list[RuleSpec]` (counts dropped — engine does not need them).

```
Sorted result:
[SD0087(4), SD0038(9), SD0013(28), SD0002(55)]
```

**Step 3 — Return sorted list to engine**

The sorted `list[RuleSpec]` is returned to `engine.py` and replaces the unsorted list
from `catalog.resolve()`. The injection loop then iterates in this priority order.

### Edge cases

| Situation | Handling |
|---|---|
| Domain not in `datasets` (e.g. `CO`) | `datasets.get(domain)` returns None → count = 0. Rule sorts to front but is skipped by the existing domain-availability check. |
| Rule has no guard expression | Guard is None → count = `len(df)` (all rows eligible). |
| Two rules have the same eligible row count | Secondary sort by `rule_id` (alphabetical) for determinism. |

### Code

```python
# injection/rule_prioritization.py

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    from .catalog import RuleSpec


def prioritize_rules(
    rule_list: list[RuleSpec],
    datasets: dict[str, pd.DataFrame],
) -> list[RuleSpec]:
    """
    Sort rules by number of eligible rows (ascending).
    Fewest eligible rows = highest priority = runs first in injection loop.

    This guarantees low-volume rules claim their rows before high-volume rules
    can consume subjects via the density cap.
    """

    def eligible_row_count(rule: RuleSpec) -> tuple[int, str]:
        domain = rule.primary_domain
        df = datasets.get(domain)

        # Edge case 1: domain not loaded
        if df is None or df.empty:
            return (0, rule.rule_id)

        # Edge case 2: no guard — all rows are eligible
        if rule.guard_expression is None:
            return (len(df), rule.rule_id)

        # Normal case: evaluate guard and count True rows
        mask = rule.guard_expression.evaluate(df)
        return (int(mask.sum()), rule.rule_id)

    return sorted(rule_list, key=eligible_row_count)
```

### How it connects to `engine.py`

Add **one call** immediately after `catalog.resolve()`, before the injection loop:

```python
# engine.py — inside InjectionEngine._run_compound() and _run_isolated()

# 3. Resolve rules (profile → categories → rules → exclude → domain filter)
active_rules = catalog.resolve(profile, categories, rules, exclude_rules, domains)

# 4. Domain availability check
injectable, skipped = self._filter_by_available_domains(active_rules, datasets)

# 5. ← NEW: Sort by eligible row count so scarcest rules inject first
from injection.rule_prioritization import prioritize_rules
injectable = prioritize_rules(injectable, datasets)

# 6. Dispatch by mode  (loop now iterates in priority order)
if mode == "isolated":
    return self._run_isolated(datasets, injectable, ...)
else:
    return self._run_compound(datasets, injectable, ...)
```

Nothing else in the injection loop changes — row selection, primitive calls, and
manifest logging are all unchanged.

### Priority order in `report.txt`

`writer.py` appends the priority order to `report.txt` so it is auditable:

```
Rule prioritization order (ascending by eligible rows):
  1. SD0087  —   4 eligible rows  [blank_field,  DM]
  2. SD0038  —   9 eligible rows  [set_invalid_value, DM]
  3. SD0013  —  28 eligible rows  [invert_date_order, AE/CM/DS/...]
  4. SD0002  —  55 eligible rows  [blank_field,  AE]
  ...
```

---

## Re-derivation After Injection

After each batch of mutations, the engine re-derives dependent fields using
existing `conformance/layer.py` code:

```python
# Already implemented and tested in the codebase:
conformance = ConformanceLayer(protocol_spec)
conformance.derive_missing_study_days(datasets)   # all --DY from --DTC + RFSTDTC
conformance.assign_epochs(datasets)               # EPOCH from SE intervals
conformance.apply_cross_domain_repairs(datasets)   # RFXSTDTC/RFXENDTC/RFENDTC from EX/DS
```

**Reuse from `conformance/layer.py`:**
| Method | What it re-derives | Line |
|---|---|---|
| `derive_missing_study_days()` | All --DY from --DTC + DM.RFSTDTC | 201 |
| `assign_epochs()` | EPOCH from SE intervals | 279 |
| `apply_cross_domain_repairs()` | DM.RFXENDTC from EX, DM.RFENDTC from DS, EPOCH | 370 |
| `_repair_dm_rfxendtc()` | DM.RFXENDTC = max(EX.EXENDTC) | 407 |
| `_repair_dm_rfendtc()` | DM.RFENDTC from DS disposition | 439 |
| `_derive_epochs_from_se()` | EPOCH for all domains from SE | called via 402 |

**Why this matters:** Corrupt `DM.RFSTDTC` → all `--DY` re-derived from wrong
RFSTDTC (or blanked if RFSTDTC blank). Only the intended rule fires. No cascade noise.
Matches real-world behavior: EDC systems derive DY from whatever date is entered.

**Expected co-violations:** Some injections necessarily trigger additional rules even
after re-derivation. Example: blank RFSTDTC (SD0087) → DY blanked → SD1085 also fires.
These are recorded in `expected_co_violations` in the manifest.

---

## 16 Mutation Primitives (`primitives.py`)

Each rule maps to exactly one primitive + parameters. No custom code per rule.
Complete mapping derived from systematic analysis of all 477 unique rules.

### Primitive Table — with rule counts and full rule coverage

| # | Primitive | Rules | What it does | How it covers the rules |
|---|---|---|---|---|
| 1 | `blank_field` | **87** | Clear a required field | **DM fields**: SD0087 (RFSTDTC), SD0088 (RFENDTC), SD0093 (AGEU), SD1342 (RFXSTDTC), SD1209 (RFXENDTC), SD2023 (AGE), SD1121 (AGE+AGETXT), SD1003 (AGE when AGEU), SD1343 (RFXSTDTC when treated), SD1380 (all identifiers). **Cross-domain conditional blanks**: SD0009 (AE serious qualifiers), SD1333 (AEOUT=RECOVERED but no AEENDTC), SD0002 (any Required variable NULL). **TS blanks**: SD1260 (TSVCDVER null when versioned), SD1306 (TDIGRP TSVAL null conditionally), SD2017 (TSVAL+TSVALNF both missing). **Generic `--` prefix rules**: SD0023 (--STAT null when --REASND), SD0031 (no start time info), SD0032 (--TPT null when --TPTNUM), SD0033 (--TPTNUM null), SD0034 (--TPTREF null), SD0049 (--STTPT null), SD0050 (--ENTPT null), SD1030 (--STRF populated but RFSTDTC null), SD1031 (--ENRF populated but RFENDTC null), SD1088 (--STDY not populated), SD1092 (--ENDY not populated), SD1339 (EPOCH null when DTC present). **EX/EC blanks**: SD1343, SD1373, SD1375. All use same logic: pick eligible row via guard → set field to empty string. |
| 2 | `set_invalid_value` | **82** | Set to forbidden/invalid value | **Forbidden literals**: SD0038 (DY=0), SD0084 (AGE=0/-1), SD1004 (ARMCD>20chars). **Reserved words**: SD1273 (--TRT='OTHER'), SD1276 (--TRT='MULTIPLE'), SD1274 (--TERM='OTHER'), SD1277 (--TERM='MULTIPLE'), SD1272 (--TESTCD='OTHER'), SD1275 (--TESTCD='MULTIPLE'). **Conditional invalids**: SD1132 (AESER≠Y), SD1062 (AESER≠Y when AESOD=Y), SD1248 (ECDOSE>0 when ECOCCUR=N), SD1249 (EXDOSE≠0 for PLACEBO), SD1365 (ADSL flags in SDTM). **Data quality**: SD1029 (non-ASCII chars), SD1096 (--TERM too long), SD1082 (variable length too long), SD1298 (non-numeric LBTOXGR). **Cross-reference invalids**: SD1053 (unexpected ARMCD), SD1051 (unexpected IDVAR/IDVARVAL), SD1327 (RSUBJID=POOLID), SD1328 (RSUBJID=USUBJID), SD2006 (MedDRA in SUPP). **TS invalids**: SD1215/SD1217 (AGEMAX/AGEMIN invalid), SD1219 (LENGTH invalid), SD1221 (PLANSUB invalid), SD1223 (RANDOM invalid), SD1269 (RANDQT not 0-1), SD1295 (ADDON invalid), SD1296 (ADAPT invalid), SD1323 (FCNTRY invalid), SD2245-SD2269 (all TSVAL/TSVCDREF invalids). **Redundancy**: SD1039 (--CAT redundant), SD1040 (--SCAT redundant), SD1041 (--CAT=--SCAT). All use same logic: pick row via guard → set field to specific bad value from catalog params. |
| 3 | `mismatch_pair` | **57** | Break paired field relationship | **ARM pairs**: SD0011 (ARMCD/ARM SCRNFAIL), SD0053 (ARMCD/ARM NOTASSGN), SD1033 (ARM not unique for ARMCD), SD1034 (ARMCD not unique for ARM), SD1133 (ACTARMCD/ACTARM), SD1134 (ACTARMCD/ACTARM), SD2236 (ACTARMCD≠ARMCD), SD2237 (ACTARM≠ARM), SD1358-SD1364 (ARM/ARMNRS combos). **Visit pairs**: SD0051 (VISIT/VISITNUM), SD0052 (VISITNUM/VISIT), SD1125-SD1127 (TPT/TPTNUM/TPTREF/ELTM combos). **AE pairs**: SD0090 (AESDTH≠Y when AEOUT=FATAL), SD0091 (AEOUT≠FATAL when AESDTH=Y). **DS pairs**: SD1314 (DSDECOD≠COMPLETED when DSTERM=COMPLETED), SD1315 (DSDECOD≠DSTERM). **Findings pairs**: SD1212 (STRESN≠STRESC), SD1353 (STRESC≠ORRES when units match). **TS pairs**: SD1268 (TSVCDREF null when TSVCDVER populated), SD1278 (TSVALCD/TSVAL mismatch), SD2251-SD2269 (all TSVAL/TSVALCD mismatch rules). All use same logic: read field_a value → set field_b to a value that violates the required relationship. |
| 4 | `delete_row` | **50** | Delete row(s) matching criteria | **All 50 are TS "Missing PARMCD" rules**: SD2201-SD2282 (Missing ADDON, AGEMAX, AGEMIN, LENGTH, PLANSUB, RANDOM, SEXPOP, STOPRULE, TBLIND, TCNTRL, TDIGRP, TINDTP, TITLE, TPHASE, TTYPE, CURTRT, OBJPRIM, SPONSOR, TRT, REGID, OUTMSPRI, PCLAS, FCNTRY, ADAPT, DCUTDTC, DCUTDESC, INTMODEL, NARMS, STYPE, INTTYPE, SSTDTC, SENDTC, ACTSUB, HLTSUBJI, EXTTIND, NCOHORT, OBJSEC, PDPSTIND, PDSTIND, PIPIND, RDIND, SDTIGVER, SDTMVER, THERAREA Trial Summary Parameters). Also SD0001 (no records in dataset — delete all rows). Logic: find TS row where TSPARMCD=target → delete it. |
| 5 | `cross_domain_mismatch` | **45** | Value in domain A conflicts with domain B | **TA lookups**: SD0066 (ARMCD not in TA), SD0071 (ARM/ARMCD not in TA), SD2002 (ACTARMCD not in TA), SD2003 (ARMCD/ARM invalid combo), SD1354 (ARMCD in TA not in DM). **TE/TI lookups**: SD0067 (ETCD not in TE), SD0068 (IETESTCD not in TI), SD1012 (ETCD/ELEMENT mismatch TE), SD1016 (IE vs TI fields). **TV/SV lookups**: SD1014 (TAETORD not in TA), SD1017 (VISITNUM not in TV), SD1018 (multi-field SV/TV mismatch), SD1023 (VISIT/VISITNUM not in TV). **Date cross-checks**: SD1202-SD1207 (DTC/STDTC/ENDTC vs RFPENDTC/RFXSTDTC/RFXENDTC), SD0080 (AESTDTC vs DSSTDTC), SD0082 (EXENDTC vs DSSTDTC), SD1144 (MHSTDTC vs RFSTDTC), SD1319 (STDTC vs RFICDTC), SD1446 (EXSTDTC vs DSSTDTC), SD1262 (SSDTC vs DSSTDTC). **Death cross-checks**: SD1254 (AEOUT=FATAL but DTHFL≠Y), SD1255 (DTHFL=Y but no FATAL AE), SD1347 (AEENDTC≠DTHDTC). **Epoch**: SD1015 (EPOCH not in TA), SD1339 (missing EPOCH). **Other**: SD0064 (USUBJID not in DM), SD1005 (STUDYID mismatch), SD0065 (VISIT/VISITNUM not in SV), SD1378/SD1379 (ETCD cross-checks). Logic: read valid set from target domain → set source to value NOT in set (or violating relationship). |
| 6 | `drop_column` | **32** | Remove required variable | **Paired variable rules**: SD1083 (--DY when --DTC present), SD2270 (--DTC when --DY present), SD2271 (--STDTC when --STDY), SD2272 (--ENDTC when --ENDY), SD1087 (--STDY when --STDTC), SD1091 (--ENDY when --ENDTC), SD1101-SD1104 (--ENTPT/--ENRTPT/--STTPT/--STRTPT pairs), SD1099 (--CAT when --SCAT), SD1450/SD1451 (--ENINT/--STINT). **Positional variables**: SD1283-SD1285 (--LAT/--PORTOT/--DIR when --LOC missing), SD1293/SD1294 (--REASND/--STAT when --PRESP missing). **DM variables**: SD1129 (AGE+AGETXT both missing). **Structural**: SD0056 (Required var not found), SD0057 (Expected var not found), SD1077 (Regulatory Expected), SD1280 (--TOX without --TOXGR), SD1044 (no --BLFL in Findings), SD1245/SD1246/SD1250/SD1282 (TPTREF/ELTM/TPTNUM/TPT combos), SD1357 (MIDSDTC without MIDS), SD1299 (no timing variables). Logic: remove the column from the DataFrame. |
| 7 | `populate_forbidden` | **31** | Set field that should be blank/absent | **DM forbidden**: SD1258 (RFSTDTC for non-treated), SD1373 (RFSTDTC for non-treated), SD1375 (RFENDTC for non-treated), SD1360 (ARMNRS when ARMCD+ACTARMCD populated), SD1363 (ARMCD/ARM for non-assigned), SD1364 (ACTARMCD/ACTARM for non-treated), SD2024 (both USUBJID+POOLID). **AE forbidden**: SD1332 (AEENDTC when NOT RECOVERED), SD1137 (DRVFL=Y when ORRES present). **Event forbidden**: SD1042 (timepoint for non-occurred), SD0041 (--OCCUR for unsolicited), SD1013 (TAETORD when UNPLAN), SD1105 (EPOCH when DSCAT=PROTOCOL MILESTONE). **Findings forbidden**: SD1047 (both DOSE+DOSTXT), SD2020 (both AGE+AGETXT). **TS forbidden**: SD2018 (both TSVAL+TSVALNF), SD1297 (TSVAL with null flavor). **SUPP**: SD1120 (comments in SUPPQUAL). Logic: find row where guard says field should be blank → set to non-blank value. |
| 8 | `duplicate_record` | **30** | Create duplicate key violation | **DM duplicates**: SD0083 (dup USUBJID in DM), SD1001 (dup SUBJID). **Seq duplicates**: SD0005/SD1236/SD1453 (dup --SEQ). **TS duplicates**: SD1038 (dup TSSEQ within TSPARMCD), SD1214/SD1216/SD1218/SD1220/SD1222/SD1224/SD1225/SD1227 (multiple PARMCD records — ADDON, AGEMAX, AGEMIN, LENGTH, PLANSUB, RANDOM, SEXPOP, NARMS). **Trial design duplicates**: SD1027 (dup ELEMENT in ETCD), SD1050 (dup ETCD in ELEMENT), SD1052 (dup TAETORD in ARMCD), SD1271 (dup TE combination), SD1286 (dup IETESTCD in TIVERS). **Findings duplicates**: SD0086 (dup SUPPQUAL), SD1439 (multiple BASELINE), SD1445 (multiple LOBXFL). **DS duplicates**: SD1290/SD1367 (multiple DS for same EPOCH). Logic: copy an existing row to create duplicate key. For TS "Multiple" rules: duplicate the TSPARMCD row. |
| 9 | `invert_date_order` | **21** | Make start > end (or range inversion) | **Date pairs**: SD0013 (--STDTC > --ENDTC, applies to 20+ domains), SD1002 (RFSTDTC > RFENDTC), SD0012 (--STDY > --ENDY), SD0015 (negative --DUR). **Numeric range**: SD0028 (--STNRHI < --STNRLO). **Cross-domain dates handled by cross_domain_mismatch** (SD1202-SD1207, SD0080, etc.). Logic: read end value → set start = end + random offset. For numeric: swap or offset. |
| 10 | `drop_domain` | **12** | Remove entire CSV file | SD1020 (DM), SD1106 (AE), SD1107 (LB), SD1108 (VS), SD1109 (EX), SD1110 (DS), SD1111 (SE), SD1112 (TA), SD1113 (TE), SD1115 (TS), SD1270 (PC when PP present), SD1061 (MB when MS present). Logic: delete the domain key from datasets dict. |
| 11 | `cross_domain_orphan` | **8** | Remove records in target domain for subject | SD0069 (no DS for subject), SD0070 (no EX for subject), SD1032 (no IE for SCRNFAIL), SD1240 (no DS informed consent), SD1374 (no DS for treated subject), SD1377 (no IE for SCREEN FAILURE), SD0006 (no BLFL=Y for subject), SD1318 (no DV for death). Logic: find subject in source → delete all their records from target domain. |
| 12 | `wrong_derived` | **6** | Offset a derived numeric value | SD1086 (wrong --DY), SD1090 (wrong --STDY), SD1094 (wrong --ENDY), SD1084 (DY derivation from RFSTDTC). Logic: compute correct value → add ±N offset. |
| 13 | `truncate_with_derived` | **3** | Partial date but DY populated | SD1085 (--DY imputed — DTC partial but DY populated), SD1089 (--STDY imputed), SD1093 (--ENDY imputed). Logic: truncate full date (2024-06-15) to partial (2024-06) but leave DY value intact. Validator should flag: DY can't be derived from partial date. |
| 14 | `add_column` | **5** | Add a column that shouldn't exist | SD0058 (variable not in SDTM model), SD1073 (prohibited variable), SD1074 (SEND-only variable), SD1075 (deprecated variable), SD1076 (permissible var in standard domain). Logic: add a column with plausible name + values to the DataFrame. **NEW primitive not in original plan.** |
| 15 | `invalid_codelist` | **4** | Use value not in controlled terminology | CT2001 (non-extensible codelist violation), CT2002 (extensible codelist), SD1322 (COUNTRY not in ISO 3166), SD2283 (TSVALNF not in ISO 21090). Logic: look up valid codelist values → set to value NOT in list. |
| 16 | `reorder_columns` | **1** | Put variable in wrong column order | SD1079 (variable in wrong order within domain). Logic: swap two column positions in the DataFrame. **NEW primitive not in original plan.** |

### Rules skipped (5 — not injectable)

| Rule | Reason |
|---|---|
| SD1071 | "Dataset > 5 GB" — not feasible to test via injection |
| SD1119 | CO domain not generated |
| SD1368 | SM domain not generated |
| SD9999 | "Dataset class not recognized" — meta-level, not data-level |
| SD0062 | "Incompatible data source" — meta-level, not data-level |

### Coverage summary

```
Total rules in Excel:          477 unique
Injectable:                    472 (16 primitives)
Skipped (not feasible):          5
```

### Primitive signatures

```python
# --- Original 13 (signatures unchanged) ---

def blank_field(df, row_idx, field) -> MutationRecord:
    """Clear a field. Returns original value."""

def populate_forbidden(df, row_idx, field, value) -> MutationRecord:
    """Set a field that should be blank/absent."""

def invert_date_order(df, row_idx, start_field, end_field, rng) -> MutationRecord:
    """Make start > end by offsetting start past end.
    Also handles numeric range inversion (STNRHI < STNRLO)."""

def set_invalid_value(df, row_idx, field, value) -> MutationRecord:
    """Set a forbidden literal value (0, -1, >20chars, non-ASCII, etc.).
    Value comes from catalog params — each rule specifies what invalid value to use."""

def mismatch_pair(df, row_idx, field_a, field_b, valid_pairs, rng) -> MutationRecord:
    """Break field_b so it no longer matches field_a.
    Reads the valid relationship, then sets field_b to violate it."""

def cross_domain_orphan(datasets, source_domain, target_domain, key_field,
                        usubjid, rng) -> MutationRecord:
    """Remove records from target domain for a subject."""

def duplicate_record(df, row_idx, key_fields, filter_field=None,
                     filter_value=None) -> MutationRecord:
    """Duplicate a row to create key violation.
    filter_field/filter_value: for TS rules, duplicate only the row matching
    TSPARMCD=filter_value."""

def wrong_derived(df, row_idx, field, offset_range, rng) -> MutationRecord:
    """Offset a numeric derived value (e.g., DY ± N)."""

def drop_column(df, column) -> MutationRecord:
    """Remove a column from a domain DataFrame."""

def drop_domain(datasets, domain) -> MutationRecord:
    """Remove a domain CSV entirely."""

def invalid_codelist(df, row_idx, field, valid_values, rng) -> MutationRecord:
    """Replace with a value NOT in the valid codelist."""

def truncate_with_derived(df, row_idx, date_field, dy_field, rng) -> MutationRecord:
    """Truncate date to partial (YYYY-MM) but leave DY populated (should be blank)."""

def cross_domain_mismatch(datasets, source_domain, source_row, source_field,
                          target_domain, target_field, mismatch_type,
                          rng) -> MutationRecord:
    """Read value from domain A, set conflicting value in domain B.
    mismatch_type: 'not_equal' | 'not_in_set' | 'absent' | 'inverted_date'
    """

# --- 3 NEW primitives ---

def delete_row(df, filter_field, filter_value) -> MutationRecord:
    """Delete row(s) matching filter criteria.
    Primary use: TS 'Missing PARMCD' rules — delete TS row where
    TSPARMCD=target_parmcd. Also SD0001 (delete all rows).
    Returns the deleted row data for manifest."""

def add_column(df, column_name, fill_value, position=None) -> MutationRecord:
    """Add a column that shouldn't exist in the domain.
    Used for structural rules (SD0058, SD1073-SD1076).
    column_name comes from catalog params (e.g., 'AEACNDEV' for SEND-only).
    fill_value: typically empty string or 'Y'."""

def reorder_columns(df, col_a, col_b) -> MutationRecord:
    """Swap two column positions to create wrong-order violation.
    Used only for SD1079. Swaps a core variable with an adjacent one."""
```

---

## Cross-Domain Rules — Full Breakdown

68 injectable cross-domain rules, grouped by injection pattern:

### Pattern 1: Lookup Orphan (9 rules) — `cross_domain_orphan` primitive

Subject exists in domain A but no matching record in domain B.

| Rule | Source | Target | Guard |
|---|---|---|---|
| SD0069 | DM (USUBJID) | DS | ARMCD not in (SCRNFAIL, NOTASSGN, null) |
| SD0070 | DM (USUBJID) | EX | ARMCD not in (SCRNFAIL, NOTASSGN, null), ACTARMCD≠NOTTRT |
| SD1032 | DM (USUBJID) | IE | ARMCD = SCRNFAIL |
| SD1240 | DM (USUBJID) | DS | (informed consent record) |
| SD1318 | DS (DSDECOD=DEATH) | DV | |
| SD1374 | DM (USUBJID) | DS | ACTARMCD not in (SCRNFAIL, NOTASSGN) |
| SD1377 | DM (ARMNRS) | IE | ARMNRS = SCREEN FAILURE |
| SD0006 | DM (USUBJID) | findings | no BLFL=Y for subject |
| SD0064 | other domains | DM | USUBJID not in DM |

**Injection:** Remove all records in target domain for the selected subject.

### Pattern 2: Value Lookup (18 rules) — `cross_domain_mismatch` primitive

Value in domain A must exist in domain B's codelist/records.

| Rule | Source.field | Target.field | Injection |
|---|---|---|---|
| SD0066 | DM.ARMCD | TA.ARMCD | Set ARMCD to value not in TA |
| SD0071 | DM.ARMCD+ARM | TA.ARMCD+ARM | Set ARM to mismatch TA combo |
| SD2002 | DM.ACTARMCD | TA.ARMCD | Set ACTARMCD to value not in TA |
| SD2003 | DM.ARMCD | TA.ARM | Set to invalid combo |
| SD0067 | SE.ETCD | TE.ETCD | Set ETCD to value not in TE |
| SD0068 | IE.IETESTCD | TI.IETESTCD | Set IETESTCD not in TI |
| SD1012 | SE.ETCD+ELEMENT | TE | Mismatch ETCD/ELEMENT combo |
| SD1014 | SV.TAETORD | TA | Invalid TAETORD |
| SD1015 | any.EPOCH | TA.EPOCH | Set EPOCH not in TA |
| SD1016 | IE fields | TI fields | Mismatch IE vs TI |
| SD1017 | SV.VISITNUM | TV.VISITNUM | VISITNUM not in TV |
| SD1018 | SV.VISITNUM+VISIT+VISITDY | TV | Multi-field mismatch |
| SD1023 | any | TV.VISIT+VISITNUM | Value not in TV |
| SD1354 | TA.ARMCD | DM.ARMCD | ARMCD in TA not in DM |
| SD1378 | TA.ETCD | TA.ARMCD | ETCD not in TA |
| SD1379 | TA.ETCD | SE.ETCD | ETCD not in SE |
| SD0064 | other.USUBJID | DM.USUBJID | USUBJID not in DM |
| SD1005 | any.STUDYID | DM.STUDYID | Invalid STUDYID |

**Injection:** Read valid values from target, set source to a value NOT in that set.

### Pattern 3: Date Cross-Check (21 rules) — `invert_date_order` or `cross_domain_mismatch`

Date in domain A must relate to date in domain B (before/after/equal).

| Rule | Source | Target | Relationship |
|---|---|---|---|
| SD1084-SD1094 | any.--DY/--DTC | DM.RFSTDTC | DY derivation rules (11 rules) |
| SD1202 | any.--STDTC | DM.RFPENDTC | STDTC ≤ RFPENDTC |
| SD1203 | any.--DTC | DM.RFPENDTC | DTC ≤ RFPENDTC |
| SD1204 | any.--ENDTC | DM.RFPENDTC | ENDTC ≤ RFPENDTC |
| SD1205 | EX.EXSTDTC | DM.RFXSTDTC | EXSTDTC ≥ RFXSTDTC |
| SD1206 | EX.EXSTDTC | DM.RFXENDTC | EXSTDTC ≤ RFXENDTC |
| SD1207 | EX.EXENDTC | DM.RFXENDTC | EXENDTC ≤ RFXENDTC |
| SD0080 | AE.AESTDTC | DS.DSSTDTC | AE start ≤ last DS date |
| SD0082 | EX.EXENDTC | DS.DSSTDTC | EX end ≤ last DS date |
| SD1144 | MH.MHSTDTC | DM.RFSTDTC | MHSTDTC ≤ RFSTDTC |
| SD1319 | any.--STDTC | DM.RFICDTC | STDTC ≥ RFICDTC |
| SD1446 | EX.EXSTDTC | DS.DSSTDTC | EX start ≤ last DS date |
| SD1262 | SS.SSDTC | DS.DSSTDTC | SSDTC ≥ DSSTDTC when DEAD |

**Injection:** Read the reference date from target domain, then set source date to
violate the relationship (e.g., set AESTDTC = DSSTDTC + 30 days for SD0080).

For DY rules (SD1084-SD1094): handled by re-derivation. Corrupt RFSTDTC or truncate
DTC → DY becomes wrong/blank after re-derivation. The `truncate_with_derived` and
`wrong_derived` primitives handle these directly.

### Pattern 4: Death Cascade (9 rules) — `cross_domain_mismatch` primitive

Multi-domain death consistency: AE.AESDTH ↔ DM.DTHFL ↔ DS.DSDECOD ↔ DM.DTHDTC ↔ SS.SSSTRESC

| Rule | What's violated | Source → Target |
|---|---|---|
| SD1254 | AEOUT=FATAL but DTHFL≠Y | AE.AEOUT → DM.DTHFL |
| SD1255 | AESDTH=Y but DTHFL≠Y | AE.AESDTH → DM.DTHFL |
| SD1256 | DSDECOD=DEATH but DTHFL≠Y | DS.DSDECOD → DM.DTHFL |
| SD1252 | SSSTRESC=DEAD but DTHFL≠Y | SS.SSSTRESC → DM.DTHFL |
| SD1253 | DD record exists but DTHFL≠Y | DD → DM.DTHFL |
| SD1347 | AEENDTC for fatal AE ≠ DTHDTC | AE.AEENDTC → DM.DTHDTC |
| SD1317 | DS DEATH date ≠ DTHDTC | DS.DSSTDTC → DM.DTHDTC |
| SD1261 | SS DEAD date ≠ DTHDTC | SS.SSDTC → DM.DTHDTC |
| SD1316 | SS DEAD but no DS DEATH record | SS.SSSTRESC → DS.DSDECOD |

**Injection strategy:** For subjects that died (DM.DTHFL=Y), break one link in the chain:
- SD1254: Set AE.AEOUT to 'FATAL' on a non-death subject (DTHFL≠Y)
- SD1347: On a death subject, change AE.AEENDTC so it ≠ DM.DTHDTC
- SD1316: On a death subject, remove the DS DEATH record

These require `cross_domain_mismatch` because the injector must read state from one
domain to create the right inconsistency in another.

### Pattern 5: Value Consistency (5 rules) — `cross_domain_mismatch` primitive

Value in A must be consistent with state in B (conditional logic).

| Rule | Logic | Injection |
|---|---|---|
| SD0079 | EX record exists when ARMCD=SCRNFAIL | Add EX record for SCRNFAIL subject |
| SD1340 | EX record exists when ACTARMCD=NOTTRT | Add EX record for NOTTRT subject |
| SD1030 | --STRF populated when RFSTDTC is null | Populate STRF on subject with blank RFSTDTC |
| SD1031 | --ENRF populated when RFENDTC is null | Populate ENRF on subject with blank RFENDTC |
| SD1367 | Multiple DS disposition for same DSSCAT/EPOCH | Duplicate a DS record |

### Pattern 6: RELREC Referential (4 rules)

| Rule | What's violated |
|---|---|
| SD0072 | RELREC.RDOMAIN references invalid domain |
| SD0075 | RELREC.IDVAR references invalid variable name |
| SD0077 | RELREC references non-existent record (USUBJID+SEQ combo) |
| SD1097 | SUPPAE references non-existent AE record |

**Injection:** Modify RELREC fields to point to non-existent records/domains/variables.
Uses `set_invalid_value` for SD0072/SD0075, `cross_domain_mismatch` for SD0077/SD1097.

### Pattern 7: SUPPQUAL (2 rules)

| Rule | What's violated |
|---|---|
| SD1143 | AE with AESMIE but no SUPPAE record with QNAM=AETRTEM |
| SD1321 | AE without treatment-emergent info in SUPPAE |

**Injection:** Remove SUPPAE records for a subject. Uses `cross_domain_orphan`.

---

## Rule → Primitive Mapping (Declarative Catalog)

Every rule maps to exactly one primitive + parameters. Full examples:

```python
RULE_PRIMITIVE_MAP = {
    # ========== dm_date (DTC1, 17 rules) ==========
    "SD0087": {"primitive": "blank_field", "params": {"field": "RFSTDTC"},
               "domain": "DM", "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
               "co_violations": ["SD1085", "SD1086"]},
    "SD0088": {"primitive": "blank_field", "params": {"field": "RFENDTC"},
               "domain": "DM", "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)"},
    "SD1002": {"primitive": "invert_date_order",
               "params": {"start_field": "RFSTDTC", "end_field": "RFENDTC"},
               "domain": "DM"},
    "SD1208": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DM", "source_field": "RFXSTDTC",
                          "target": "EX", "target_field": "EXSTDTC",
                          "mismatch_type": "not_equal_min"},
               "domain": "DM"},
    "SD1258": {"primitive": "populate_forbidden",
               "params": {"field": "RFSTDTC", "value": "2024-01-01"},
               "domain": "DM", "guard": "ARMCD in (SCRNFAIL,NOTASSGN,)"},
    "SD1342": {"primitive": "blank_field", "params": {"field": "RFXSTDTC"},
               "domain": "DM", "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)"},
    "SD1343": {"primitive": "blank_field", "params": {"field": "RFXENDTC"},
               "domain": "DM", "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)"},
    "SD2004": {"primitive": "blank_field", "params": {"field": "DTHDTC"},
               "domain": "DM", "guard": "DTHFL == Y"},
    "SD2005": {"primitive": "populate_forbidden",
               "params": {"field": "DTHFL", "value": "Y"},
               "domain": "DM", "guard": "DTHDTC == "},
    # ... SD1209, SD1210, SD1213, SD1259, SD1334, SD1335, SD1366, SD1373, SD1375, SD1376, SD2023

    # ========== date_cross_domain (DTC2, 7 rules) ==========
    "SD0013": {"primitive": "invert_date_order",
               "params": {"start_field": "--STDTC", "end_field": "--ENDTC"},
               "domain": "--"},
    "SD0038": {"primitive": "set_invalid_value",
               "params": {"field": "--DY", "value": "0"},
               "domain": "--"},
    "SD1085": {"primitive": "truncate_with_derived",
               "params": {"date_field": "--DTC", "dy_field": "--DY"},
               "domain": "--"},
    "SD1086": {"primitive": "wrong_derived",
               "params": {"field": "--DY", "offset_range": [-10, 10]},
               "domain": "--"},
    "SD1202": {"primitive": "cross_domain_mismatch",
               "params": {"source_field": "--STDTC", "target": "DM",
                          "target_field": "RFPENDTC", "mismatch_type": "date_after"},
               "domain": "--"},
    "SD1203": {"primitive": "cross_domain_mismatch",
               "params": {"source_field": "--DTC", "target": "DM",
                          "target_field": "RFPENDTC", "mismatch_type": "date_after"},
               "domain": "--"},
    "SD1204": {"primitive": "cross_domain_mismatch",
               "params": {"source_field": "--ENDTC", "target": "DM",
                          "target_field": "RFPENDTC", "mismatch_type": "date_after"},
               "domain": "--"},

    # ========== age_arm (DM01, 21 rules) ==========
    "SD0084": {"primitive": "set_invalid_value", "params": {"field": "AGE", "value": "0"},
               "domain": "DM"},
    "SD0093": {"primitive": "blank_field", "params": {"field": "AGEU"},
               "domain": "DM", "guard": "AGE != "},
    "SD1001": {"primitive": "duplicate_record",
               "params": {"key_fields": ["STUDYID", "SUBJID"]}, "domain": "DM"},
    "SD1003": {"primitive": "blank_field", "params": {"field": "AGE"},
               "domain": "DM", "guard": "AGEU != "},
    "SD1121": {"primitive": "blank_field", "params": {"field": "AGE"},
               "domain": "DM"},  # also blank AGETXT if present
    "SD1133": {"primitive": "mismatch_pair",
               "params": {"field_a": "ACTARMCD", "field_b": "ACTARM"}, "domain": "DM"},
    "SD1134": {"primitive": "mismatch_pair",
               "params": {"field_a": "ACTARM", "field_b": "ACTARMCD"}, "domain": "DM"},
    "SD0011": {"primitive": "mismatch_pair",
               "params": {"field_a": "ARMCD", "field_b": "ARM",
                          "expected": {"SCRNFAIL": "Screen Failure"}},
               "domain": "DM", "guard": "ARMCD == SCRNFAIL"},
    "SD0053": {"primitive": "mismatch_pair",
               "params": {"field_a": "ARMCD", "field_b": "ARM",
                          "expected": {"NOTASSGN": "Not Assigned"}},
               "domain": "DM", "guard": "ARMCD == NOTASSGN"},
    # ... SD1322, SD1349, SD1358-SD1364, SD2001, SD2019-SD2022, SD2236, SD2237

    # ========== dm_cross_domain (DM02, 13 rules) ==========
    "SD0066": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DM", "source_field": "ARMCD",
                          "target": "TA", "target_field": "ARMCD",
                          "mismatch_type": "not_in_set"},
               "domain": "DM", "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)"},
    "SD0069": {"primitive": "cross_domain_orphan",
               "params": {"source": "DM", "target": "DS", "key": "USUBJID"},
               "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)"},
    "SD0070": {"primitive": "cross_domain_orphan",
               "params": {"source": "DM", "target": "EX", "key": "USUBJID"},
               "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,) and ACTARMCD != NOTTRT"},
    "SD0071": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DM", "source_field": "ARMCD,ARM",
                          "target": "TA", "target_field": "ARMCD,ARM",
                          "mismatch_type": "combo_not_in_set"},
               "domain": "DM"},
    "SD0083": {"primitive": "duplicate_record",
               "params": {"key_fields": ["STUDYID", "USUBJID"]}, "domain": "DM"},
    "SD1004": {"primitive": "set_invalid_value",
               "params": {"field": "ARMCD", "value": "TOOLONGARMCODEVALUE12345"},
               "domain": "DM"},
    "SD1032": {"primitive": "cross_domain_orphan",
               "params": {"source": "DM", "target": "IE", "key": "USUBJID"},
               "guard": "ARMCD == SCRNFAIL"},
    "SD1240": {"primitive": "cross_domain_orphan",
               "params": {"source": "DM", "target": "DS", "key": "USUBJID"}},
    # ... SD1033, SD1034, SD1367, SD1374, SD1377, SD2002, SD2003

    # ========== death_cascade (from EVT1, 9 rules) ==========
    "SD1254": {"primitive": "cross_domain_mismatch",
               "params": {"source": "AE", "source_field": "AEOUT",
                          "target": "DM", "target_field": "DTHFL",
                          "mismatch_type": "set_source_expect_target",
                          "source_value": "FATAL", "expected_target": "Y"},
               "domain": "AE"},
    "SD1255": {"primitive": "cross_domain_mismatch",
               "params": {"source": "AE", "source_field": "AESDTH",
                          "target": "DM", "target_field": "DTHFL",
                          "source_value": "Y", "expected_target": "Y"},
               "domain": "AE"},
    "SD1347": {"primitive": "cross_domain_mismatch",
               "params": {"source": "AE", "source_field": "AEENDTC",
                          "target": "DM", "target_field": "DTHDTC",
                          "mismatch_type": "not_equal"},
               "domain": "AE", "guard": "AEOUT == FATAL"},
    "SD1256": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DS", "source_field": "DSDECOD",
                          "target": "DM", "target_field": "DTHFL",
                          "source_value": "DEATH", "expected_target": "Y"},
               "domain": "DS"},
    "SD1317": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DS", "source_field": "DSSTDTC",
                          "target": "DM", "target_field": "DTHDTC",
                          "mismatch_type": "not_equal"},
               "domain": "DS", "guard": "DSDECOD == DEATH"},
    "SD1252": {"primitive": "cross_domain_mismatch",
               "params": {"source": "SS", "source_field": "SSSTRESC",
                          "target": "DM", "target_field": "DTHFL",
                          "source_value": "DEAD", "expected_target": "Y"},
               "domain": "SS"},
    "SD1316": {"primitive": "cross_domain_orphan",
               "params": {"source": "SS", "target": "DS", "key": "USUBJID"},
               "guard": "SSSTRESC == DEAD"},
    "SD1261": {"primitive": "cross_domain_mismatch",
               "params": {"source": "SS", "source_field": "SSDTC",
                          "target": "DM", "target_field": "DTHDTC",
                          "mismatch_type": "date_before"},
               "domain": "SS", "guard": "SSSTRESC == DEAD"},
    "SD1253": {"primitive": "cross_domain_mismatch",
               "params": {"source": "DD", "source_field": "USUBJID",
                          "target": "DM", "target_field": "DTHFL",
                          "mismatch_type": "absent"},
               "domain": "DD"},

    # ========== findings rules (FA1+FA2, 36 rules) ==========
    "SD0047": {"primitive": "blank_field", "params": {"field": "--ORRES"},
               "domain": "--", "guard": "--STAT != NOT DONE"},
    "SD0040": {"primitive": "populate_forbidden",
               "params": {"field": "--ORRES", "value": "999"},
               "domain": "--", "guard": "--STAT == NOT DONE"},
    "SD1137": {"primitive": "populate_forbidden",
               "params": {"field": "--DRVFL", "value": "Y"},
               "domain": "--", "guard": "--ORRES != "},
    "SD1138": {"primitive": "populate_forbidden",
               "params": {"field": "--DRVFL", "value": "Y"},
               "domain": "--", "guard": "--ORRES != "},
    # ... SD0007, SD0016, SD0017, SD0024-SD0030, SD0036, SD0045, SD1043, SD1117,
    #     SD1122, SD1123, SD1131, SD1212, SD1272, SD1275, SD1320, SD1330, SD1353,
    #     SD1369-SD1372, SD1439, SD1445, SD1448, SD2239

    # ========== dataset-level (13 rules) ==========
    "SD1020": {"primitive": "drop_domain", "params": {"domain": "DM"}},
    "SD1111": {"primitive": "drop_domain", "params": {"domain": "TA"}},
    "SD1109": {"primitive": "drop_domain", "params": {"domain": "TE"}},
    "SD1106": {"primitive": "drop_domain", "params": {"domain": "TV"}},
    "SD1110": {"primitive": "drop_domain", "params": {"domain": "TS"}},
    "SD1107": {"primitive": "drop_domain", "params": {"domain": "SE"}},
    # ... SD1108, SD1112, SD1113, SD1115, SD1270

    # ========== variable-level (29 rules) ==========
    "SD1083": {"primitive": "drop_column", "params": {"column": "--DY"}, "domain": "--"},
    "SD1129": {"primitive": "drop_column", "params": {"column": "AGE"}, "domain": "DM"},
    "SD2270": {"primitive": "drop_column", "params": {"column": "--DTC"}, "domain": "--",
               "guard": "--DY exists"},
    # ... SD2271, SD2272, SD1087, SD1091, SD1099-SD1104, SD1283-SD1285,
    #     SD1293, SD1294, SD1355-SD1357, SD1280, SD1044, SD1245, SD1246,
    #     SD1250, SD1282, SD1450, SD1451

    # ========== TS rules: delete_row (50 rules) ==========
    "SD2201": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "ADDON"}, "domain": "TS"},
    "SD2202": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "AGEMAX"}, "domain": "TS"},
    "SD2203": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "AGEMIN"}, "domain": "TS"},
    "SD2204": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "LENGTH"}, "domain": "TS"},
    "SD2205": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "PLANSUB"}, "domain": "TS"},
    "SD2206": {"primitive": "delete_row", "params": {"filter_field": "TSPARMCD", "filter_value": "RANDOM"}, "domain": "TS"},
    # ... SD2207-SD2282 all follow same pattern with different TSPARMCD values
    "SD0001": {"primitive": "delete_row", "params": {"filter_field": None, "filter_value": None}, "domain": "--"},

    # ========== TS rules: duplicate_record (8 rules) ==========
    "SD1214": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "ADDON"}, "domain": "TS"},
    "SD1216": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "AGEMAX"}, "domain": "TS"},
    "SD1218": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "AGEMIN"}, "domain": "TS"},
    "SD1220": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "LENGTH"}, "domain": "TS"},
    "SD1222": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "PLANSUB"}, "domain": "TS"},
    "SD1224": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "RANDOM"}, "domain": "TS"},
    "SD1225": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "SEXPOP"}, "domain": "TS"},
    "SD1227": {"primitive": "duplicate_record", "params": {"key_fields": ["TSPARMCD"], "filter_field": "TSPARMCD", "filter_value": "NARMS"}, "domain": "TS"},

    # ========== TS rules: set_invalid_value (~30 rules) ==========
    "SD1215": {"primitive": "set_invalid_value", "params": {"field": "TSVAL", "value": "INVALID", "filter_field": "TSPARMCD", "filter_value": "AGEMAX"}, "domain": "TS"},
    "SD1217": {"primitive": "set_invalid_value", "params": {"field": "TSVAL", "value": "INVALID", "filter_field": "TSPARMCD", "filter_value": "AGEMIN"}, "domain": "TS"},
    # ... SD1219 (LENGTH), SD1221 (PLANSUB), SD1223 (RANDOM), SD1269 (RANDQT),
    #     SD1295 (ADDON), SD1296 (ADAPT), SD1323 (FCNTRY), SD2245-SD2268

    # ========== TS rules: mismatch_pair (~10 rules) ==========
    "SD2252": {"primitive": "mismatch_pair", "params": {"field_a": "TSVAL", "field_b": "TSVALCD", "filter_field": "TSPARMCD", "filter_value": "CURTRT"}, "domain": "TS"},
    # ... SD2255 (COMPTRT), SD2259 (INDIC), SD2262 (TRT), SD2265 (PCLAS), SD2269 (TDIGRP)

    # ========== structural rules: add_column (5 rules) ==========
    "SD0058": {"primitive": "add_column", "params": {"column_name": "CUSTOMVAR", "fill_value": ""}, "domain": "--"},
    "SD1073": {"primitive": "add_column", "params": {"column_name": "AEACNDEV", "fill_value": ""}, "domain": "--"},
    "SD1074": {"primitive": "add_column", "params": {"column_name": "GRPID", "fill_value": "1"}, "domain": "--"},
    "SD1075": {"primitive": "add_column", "params": {"column_name": "AEREF", "fill_value": ""}, "domain": "AE"},
    "SD1076": {"primitive": "add_column", "params": {"column_name": "AEMODIFY", "fill_value": ""}, "domain": "AE"},

    # ========== structural: reorder_columns (1 rule) ==========
    "SD1079": {"primitive": "reorder_columns", "params": {"col_a": "STUDYID", "col_b": "DOMAIN"}, "domain": "--"},
}
```

### How catalog.py auto-derives primitive + params from Test_Case.csv

The `RULE_PRIMITIVE_MAP` above is a **static dict** — every rule's primitive and params are
pre-defined at development time, not auto-inferred at runtime. This is deliberate:

1. **Why not auto-infer?** The Rule Message text is ambiguous. Auto-inference from NLP would
   produce ~10% wrong mappings. A static map is auditable and testable.

2. **What `catalog.py` does at runtime:**
   ```python
   class RuleCatalog:
       def __init__(self, excel_dir):
           self.test_cases = parse_test_cases(excel_dir / "Test_Case.csv")
           self.rule_map = RULE_PRIMITIVE_MAP  # static dict

       def get_spec(self, rule_id) -> RuleSpec:
           entry = self.rule_map[rule_id]
           tc = self.test_cases[rule_id]
           return RuleSpec(
               rule_id=rule_id,
               primitive=entry["primitive"],
               params=entry["params"],
               domain=entry.get("domain", tc.domain),
               domain_expanded=tc.domain_expanded,
               guard=entry.get("guard"),
               rule_message=tc.rule_message,
               category=tc.category,
           )
   ```

3. **Test_Case.csv provides runtime metadata** (not the primitive choice):
   - `Domain Expanded`: which domains to inject into
   - `Rule Message`: stored in manifest for human-readable error descriptions
   - `Dependent Domain` / `Dependent Domain Variable`: cross-domain lookup targets

4. **Error_Case.csv provides I/V test vectors** for 73 rules:
   - `I-` prefixed rows = invalid cases (should trigger the rule)
   - `V-` prefixed rows = valid cases (should NOT trigger)
   - Used in unit tests to verify each primitive produces correct I-vector output

---

## Guard Expression Evaluation

Guards filter which records are eligible for injection:

```python
def evaluate_guard(df: pd.DataFrame, idx: int, guard: str) -> bool:
    """
    Supported syntax:
      "ARMCD not in (SCRNFAIL,NOTASSGN,)"   → value not in set
      "ARMCD in (SCRNFAIL,NOTASSGN)"         → value in set
      "ARMCD == SCRNFAIL"                     → exact match
      "ARMCD != "                             → not blank
      "AGE != "                               → field is populated
      "--STAT != NOT DONE"                    → with prefix resolution
    """
```

Guards are evaluated per-row before injection. They are also used by
`rule_prioritization.py` at the start of each run to count eligible rows
and determine injection order.

---

## Generic Prefix Resolution

Many rules use `--` prefix (e.g., `--STDTC`, `--DY`). Resolved at injection time:

```python
def resolve_prefix(generic_var: str, domain: str, columns: list[str]) -> str | None:
    """
    '--STDTC' + 'AE' → 'AESTDTC'
    '--DY' + 'VS' → 'VSDY'
    '--DTC' + 'LB' → 'LBDTC'
    DM special cases: RFSTDTC (not DMSTDTC), RFENDTC (not DMENDTC)
    Returns None if resolved column doesn't exist in the domain.
    """
```

For `domain: "--"` rules (e.g., SD0013 applies to 20+ domains), the engine iterates
all applicable domains from `Test_Case.csv "Domain Expanded"`, intersected with
actually-loaded domains, and injects into each.

---

## Engine Orchestration (`engine.py`)

```python
class InjectionEngine:
    def run(self, input_dir, output_dir, mode, profile, categories,
            rules, exclude_rules, domains, rate, density_cap,
            seed, dry_run) -> InjectionManifest:

        # 1. Load clean data
        datasets = load_datasets(input_dir)

        # 2. Build catalog (parse Excel CSVs once)
        catalog = RuleCatalog(excel_dir=input_dir.parent / "imputation_ref" / "csv_export")

        # 3. Resolve rules (profile → categories → rules → exclude → domain filter)
        active_rules = catalog.resolve(profile, categories, rules, exclude_rules, domains)

        # 4. Domain availability check — auto-skip rules for missing domains
        injectable, skipped = self._filter_by_available_domains(active_rules, datasets)

        # 5. Rule prioritization — sort by eligible row count (fewest first)
        #    Ensures low-volume rules claim rows before high-volume rules consume
        #    subjects via the density cap.
        from injection.rule_prioritization import prioritize_rules
        injectable = prioritize_rules(injectable, datasets)

        # 6. Dispatch by mode
        if mode == "isolated":
            return self._run_isolated(datasets, injectable, ...)
        else:
            return self._run_compound(datasets, injectable, ...)

    def _run_compound(self, clean_datasets, rules, rate, density_cap, seed, ...):
        rng = np.random.default_rng(seed)
        dirty = deep_copy(clean_datasets)
        subject_counts = defaultdict(int)  # density cap

        # Order within level: DM record-level → trial design → other domains
        # → variable-level → dataset-level
        # Note: within record-level, ordering is already handled by prioritize_rules()
        ordered = self._order_rules(rules)

        # Inject all record-level + cross-domain rules (in priority order)
        for rule in ordered:
            if rule.is_dataset_level or rule.is_variable_level:
                continue
            targets = self._select_targets(dirty, rule, rate, rng,
                                           subject_counts, density_cap)
            mutations = apply_primitive(dirty, rule, targets, rng)
            self.manifest.add_all(mutations)
            for m in mutations:
                subject_counts[m.usubjid] += 1

        # Re-derive dependent fields from corrupted data
        conformance = ConformanceLayer(self.protocol_spec)
        conformance.derive_missing_study_days(dirty)
        conformance.assign_epochs(dirty)
        conformance.apply_cross_domain_repairs(dirty)

        # Record what changed due to re-derivation
        self._record_rederivations(clean_datasets, dirty)

        # Variable-level rules (drop columns)
        for rule in ordered:
            if rule.is_variable_level:
                apply_primitive(dirty, rule, [], rng)

        # Dataset-level rules (drop entire CSV files)
        for rule in ordered:
            if rule.is_dataset_level:
                apply_primitive(dirty, rule, [], rng)

        # Post-injection self-validation
        warnings = self._validate_injections(dirty)

        if not dry_run:
            copy_clean(clean_datasets, output_dir / "clean")
            write_datasets(dirty, output_dir / "dirty")
            write_manifest(self.manifest, output_dir / "manifest.json")
            write_report(self.manifest, skipped, warnings, output_dir / "report.txt")

        return self.manifest

    def _filter_by_available_domains(self, rules, datasets):
        injectable, skipped = [], []
        available = set(datasets.keys())
        for rule in rules:
            needed = self._domains_needed(rule)
            if needed and not (needed & available):
                skipped.append((rule.rule_id, f"needs {needed}, have {available}"))
            else:
                injectable.append(rule)
        return injectable, skipped
```

---

## Post-Injection Self-Validation

After injecting, verify each mutation actually created the intended violation:

```python
def _validate_injections(self, dirty):
    warnings = []
    for error in self.manifest.errors:
        if error.primitive == "invert_date_order":
            ...  # verify start > end in dirty data
        elif error.primitive == "blank_field":
            ...  # verify field is blank
        elif error.primitive == "cross_domain_orphan":
            ...  # verify no matching record in target domain
    return warnings
```

Any failure = injection bug. Logged as WARNING in report.

---

## Scoring Helper (`manifest.py` + script)

Use `manifest.score_validator(...)` directly after running `python apply_primitive.py`.

```python
def score_validator(manifest_path: Path, validator_output_path: Path,
                    rule_id_column: str = "Rule ID",
                    domain_column: str = "Domain",
                    usubjid_column: str = "USUBJID") -> pd.DataFrame:
    """
    Compare manifest ground truth vs validator findings.

    Returns per-rule DataFrame:
      rule_id | expected | detected | TP | FN | FP | precision | recall

    Also returns overall summary:
      total_expected, total_detected, overall_precision, overall_recall
    """
```

---

## Density Cap

```yaml
defaults:
  density_cap: 5  # max violations per subject
```

Prevents a subject from having 30+ different injected violations.
Keeps errors attributable. Set `--density-cap 0` to disable.

**Interaction with rule prioritization:** The density cap is what makes prioritization
necessary. Without prioritization, a high-volume rule running first may use up subjects
that a low-volume rule needs. Prioritization ensures the low-volume rule always runs before
the cap limits its available subjects.

---

## Ground Truth Manifest Format (`manifest.json`)

```json
{
  "generated_at": "2026-04-07T14:30:00",
  "seed": 42,
  "mode": "compound",
  "source_dir": "./output",
  "profile": "all",
  "rate": 0.05,
  "density_cap": 5,
  "prioritize_rules": true,
  "rules_available": 479,
  "rules_injectable": 455,
  "rules_skipped": 24,
  "rules_injected": 87,
  "total_mutations": 178,
  "skipped_rules": [
    {"rule_id": "SD0072", "reason": "domain CO not generated"},
    {"rule_id": "SD1368", "reason": "domain SM not generated"}
  ],
  "errors": [
    {
      "error_id": "INJ-SD0013-AE-001",
      "rule_id": "SD0013",
      "rule_message": "--STDTC is after --ENDTC",
      "category": "date_cross_domain",
      "primitive": "invert_date_order",
      "domain": "AE",
      "usubjid": "EMPEROR-HFPEF-SITE001-0001",
      "row_index": 3,
      "seq_value": "4",
      "variables_modified": {
        "AESTDTC": {"original": "2024-06-01", "injected": "2024-09-16"}
      },
      "re_derived": {
        "AESTDY": {"original": "36", "new": "143"}
      },
      "expected_co_violations": []
    },
    {
      "error_id": "INJ-SD0087-DM-001",
      "rule_id": "SD0087",
      "rule_message": "RFSTDTC is not provided for a randomized subject",
      "category": "dm_date",
      "primitive": "blank_field",
      "domain": "DM",
      "usubjid": "EMPEROR-HFPEF-SITE001-0002",
      "row_index": 1,
      "seq_value": null,
      "variables_modified": {
        "RFSTDTC": {"original": "2024-04-27", "injected": ""}
      },
      "re_derived": {
        "AESTDY": {"original": "36", "new": ""},
        "AEENDY": {"original": "40", "new": ""},
        "VSSTDY": {"original": "1", "new": ""}
      },
      "expected_co_violations": ["SD1085", "SD1086"]
    }
  ],
  "summary_by_rule": {"SD0013": 12, "SD0087": 3, "SD0038": 5},
  "summary_by_category": {"date_cross_domain": 24, "dm_date": 8},
  "summary_by_domain": {"AE": 38, "DM": 45, "VS": 22}
}
```

---

## User Control: 3 Levels + Filters

### Level 1: Profiles
```bash
--profile all              # all 25 categories (~455 injectable rules)
--profile dates            # dm_date + date_cross_domain (24 rules)
--profile dm               # dm_date + age_arm + dm_cross_domain (51 rules)
--profile findings         # findings_chain_1 + findings_chain_2 + findings_individual (57 rules)
--profile cross_domain     # dm_cross_domain + date_cross_domain (20 rules)
--profile ts               # ts_domain + ts_parmcd_req + ts_parmcd_limit + ts_tsval (106 rules)
--profile structural       # mandatory_domain + mandatory_variable + all_domains (75 rules)
--profile death            # death cascade rules from ae_events (9 rules)
```

### Level 2: Categories
```bash
--categories dm_date,date_cross_domain,ae_events
```

### Level 3: Rules
```bash
--rules SD0013,SD0038,SD0087
--exclude-rules SD1020,SD1083
```

### Filters
```bash
--domains AE,DM,VS         # only inject in these domains
--rate 0.10                 # 10% of eligible records per rule
--density-cap 3             # max 3 errors per subject
--no-prioritize            # disable rule prioritization (run in catalog order)
```

### Resolution order
```
1. --profile (default: all) → categories → rules
2. --categories overrides profile
3. --rules overrides everything
4. --exclude-rules removes from set
5. --domains filters to specified domains
6. Domain availability auto-filters non-generated domains
7. rule_prioritization.py sorts remaining injectable rules (unless --no-prioritize)
```

---

## Script Entrypoint (`apply_primitive.py`)

```python
from injection.engine import InjectionEngine

engine = InjectionEngine()
manifest = engine.run(
  input_dir=r"D:\Care2data_intern\sdtm_synth_package(39)\output_tj301_p21",
  output_dir=r"D:\Care2data_intern\op_for_errror_inj",
  mode="compound",
  profile="all",
)
```

---

## Config (`config/injection/defaults.yaml`)

```yaml
profiles:
  all:
    categories: [dm_date, date_cross_domain, age_arm, dm_cross_domain,
                 special_purpose, interventions, ae_events, events,
                 findings_individual, findings_chain_1, findings_chain_2,
                 general_obs_1, general_obs_2, relationship,
                 ts_domain, ts_parmcd_req, ts_parmcd_limit, ts_tsval,
                 trial_design, mandatory_domain, mandatory_variable,
                 mandatory_variable_2, custom_split,
                 all_domains_1, all_domains_2, device]
  dates:
    categories: [dm_date, date_cross_domain]
  dm:
    categories: [dm_date, age_arm, dm_cross_domain]
  findings:
    categories: [findings_chain_1, findings_chain_2, findings_individual]
  cross_domain:
    categories: [dm_cross_domain, date_cross_domain]
  ts:
    categories: [ts_domain, ts_parmcd_req, ts_parmcd_limit, ts_tsval]
  structural:
    categories: [mandatory_domain, mandatory_variable, mandatory_variable_2,
                 all_domains_1, all_domains_2]
  death:
    categories: [ae_events]  # filtered to death rules only

defaults:
  rate: 0.05
  max_errors_per_rule: 50
  density_cap: 5
  seed: 42
  prioritize_rules: true      # sort rules by eligible row count before injection

category_rates:
  dm_date: 0.10
  dm_cross_domain: 0.10
  age_arm: 0.10
  ae_events: 0.08
  date_cross_domain: 0.05
  findings_chain_1: 0.03
  findings_chain_2: 0.03
  ts_parmcd_req: 0.50
  ts_parmcd_limit: 0.50
  ts_tsval: 0.50
  mandatory_domain: 1.0

skip_by_default:
  - SD9999
```

---

## Implementation Order

| # | File | What | Test |
|---|---|---|---|
| 1 | `rule_parser.py` | Parse Rule_Cases.csv blocks + Error_Case.csv flat | All 454 rules parsed, I/V counts match |
| 2 | `catalog.py` | Build RuleCatalog + RULE_PRIMITIVE_MAP | profile 'all' → 479 rules; domain filter → 455 injectable |
| 3 | `reader.py` | Load CSVs dtype=str, keep_default_na=False | Load ./output, 30 domains |
| 4 | `manifest.py` | MutationRecord + InjectionManifest + JSON + score_validator() | Serialize, round-trip, scoring |
| 5 | `config.py` + `defaults.yaml` | Config, profiles, rates, density cap, prioritize_rules flag | All profiles resolve |
| 6 | `primitives.py` | 16 primitives with guard evaluation + prefix resolution | Unit test each on synthetic DataFrames |
| 7 | `rule_prioritization.py` | Count eligible rows per rule + sort ascending | Unit test: sorted order matches expected; edge cases (no domain, no guard, tie) all handled |
| 8 | `writer.py` | clean/ + dirty/ + manifest.json + report.txt + priority order log | Round-trip preservation |
| 9 | `engine.py` | Full orchestration + prioritize_rules() call + re-derivation + self-validation | e2e on EMPEROR-HFPEF output |
| 10 | `apply_primitive.py` | Run engine orchestration entrypoint | e2e compound and isolated runs |

---

## Reuse from Existing Codebase

| What | Source | Used in |
|---|---|---|
| `ConformanceLayer` | `conformance/layer.py` | engine.py — re-derivation |
| `.derive_missing_study_days()` | `conformance/layer.py:201` | re-derive all --DY |
| `.assign_epochs()` | `conformance/layer.py:279` | re-derive EPOCH |
| `.apply_cross_domain_repairs()` | `conformance/layer.py:370` | re-derive RFXSTDTC/RFXENDTC/RFENDTC |
| `parse_date()` | `utils/dates.py` | primitives.py — date manipulation |
| `to_iso_date()` / `format_iso_date()` | `utils/dates.py` | primitives.py — format dates |
| `derive_study_day()` | `utils/dates.py` | self-validation checks |
| `DOMAIN_COLUMN_ORDER` | `schema/column_order.py` | writer.py — preserve column order |
| `apply_primitive.py` | main entrypoint | trigger full orchestration run |
| Guard evaluation logic | `catalog.py` (existing) | rule_prioritization.py — reuse same guard evaluation to count eligible rows |

---

## Verification

1. **Unit**: Each of 16 primitives tested on synthetic DataFrames
2. **Round-trip**: inject → read manifest → verify each modified cell matches dirty CSV
3. **V-vector sanity**: Clean data must NOT trigger any I-vector patterns (no false positives)
4. **Re-derivation**: Corrupt RFSTDTC → DY correctly re-derived (or blanked) → manifest records delta
5. **Cross-domain**: SD0066 → ARMCD not in TA → manifest records it → validator should flag
6. **Death cascade**: Break one link → only that rule fires (not whole cascade)
7. **Domain skip**: Rule targeting CO → auto-skipped → logged in report
8. **Density cap**: With cap=3, no subject has >3 violations in manifest
9. **Determinism**: Same seed → identical manifest.json
10. **Isolated mode**: One folder per rule, one error per manifest
11. **Scoring**: `score` command produces per-rule precision/recall table
12. **Prioritization — sort order**: `prioritize_rules()` returns rules in ascending eligible-row order
13. **Prioritization — edge cases**: No domain loaded → count=0, rule sorted first, skipped correctly. No guard → count=len(df). Tied counts → sorted by rule_id deterministically.
14. **Prioritization — starvation prevention**: In compound mode with density_cap=3, a rule with 4 eligible rows always injects successfully even when 55-row rules are also in the run.
15. **`--no-prioritize`**: Disabling the flag produces catalog order (verify by inspecting report.txt priority log).

---

## Critical Files

**New (10 + 1 config):**
- `injection/__init__.py`, `engine.py`, `reader.py`, `writer.py`, `rule_parser.py`,
  `catalog.py`, `rule_prioritization.py`, `primitives.py`, `manifest.py`, `config.py`
- `config/injection/defaults.yaml`

**Modified (1):**
- `apply_primitive.py` — run full orchestration directly through `InjectionEngine`
