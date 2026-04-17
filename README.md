# SDTM Error Injection Test Harness — Implementation Progress

## Overview

A Python tool that injects **472 known SDTM errors** into clean CSV datasets to create labeled test data for validator testing. Generates dirty CSVs + ground-truth manifest.json for measuring validator precision/recall.

## Architecture

```
injection/
├── __init__.py              # Package exports
├── config.py                # YAML config loader (profiles, rates, skip list)
├── reader.py                # Load CSVs as dtype=str (no auto-NaN)
├── writer.py                # Write clean/, dirty/, manifest.json, report.txt
├── manifest.py              # MutationRecord, InjectionManifest, score_validator()
├── rule_parser.py           # Parse rule definitions, GuardExpression
├── catalog.py               # RuleCatalog, RULE_PRIMITIVE_MAP (472 rules)
├── rule_prioritization.py   # Sort rules by eligible row count (fewest first)
├── primitives.py            # 16 reusable mutation functions
└── engine.py                # (Phase 5) Orchestrate pipeline

config/injection/
└── defaults.yaml            # Profiles, category rates, density cap

tests/
├── test_phase1.py           # Config, reader, writer, manifest
├── test_phase2.py           # Rule catalog, parser, resolution
├── test_phase3.py           # All 16 primitives on synthetic data
└── test_phase4.py           # Rule prioritization, tie-breaking

scripts/
├── verify_phase1.py         # Quick sanity check for Phase 1
└── verify_phase2.py         # Quick sanity check for Phase 2
```

---

## Implementation Status

### ✅ **Phase 1: Foundation Setup** (COMPLETE)
- [x] Config loader (YAML, profiles, category rates)
- [x] CSV reader (dtype=str, keep_default_na=False)
- [x] Manifest data structures (MutationRecord, InjectionManifest)
- [x] Output writer (clean/, dirty/, JSON, report)
- [x] Unit tests for all Phase 1 modules

**Key files:**
- `injection/config.py` — 100 lines
- `injection/reader.py` — 90 lines
- `injection/manifest.py` — 350 lines (includes score_validator)
- `injection/writer.py` — 180 lines
- `tests/test_phase1.py` — 350 lines

---

### ✅ **Phase 2: Rule Catalog & Parsing** (COMPLETE)
- [x] RuleSpec dataclass
- [x] GuardExpression with evaluation logic
- [x] RuleCatalog with 472 injectable rules
- [x] RULE_PRIMITIVE_MAP: static dict mapping rule_id → (primitive, params)
- [x] Rule resolution: profile → categories → explicit rules
- [x] Domain filtering and non-injectable rule handling
- [x] Unit tests for catalog, parser, guard evaluation

**Key files:**
- `injection/rule_parser.py` — 180 lines (RuleSpec, GuardExpression, RuleParser)
- `injection/catalog.py` — 450 lines (RuleCatalog, RULE_PRIMITIVE_MAP)
- `tests/test_phase2.py` — 350 lines
- `config/injection/defaults.yaml` — 100 lines

**Coverage:**
- 472 injectable rules (out of 479 total)
- 5 non-injectable rules (SD1071, SD9999, SD0062, SD1119, SD1368)
- 25 categories (all mapped)
- 16 primitive types (one per rule mapping)

---

### ✅ **Phase 3: Mutation Primitives** (COMPLETE)
All 16 data-driven primitives implemented with comprehensive error handling:

1. **blank_field** — Clear required field (87 rules)
2. **set_invalid_value** — Forbidden literal (0, -1, >20chars, etc.) (82 rules)
3. **mismatch_pair** — Break paired field relationship (57 rules)
4. **delete_row** — Delete rows matching filter (50 rules: TS PARMCD)
5. **cross_domain_mismatch** — Value in A conflicts with B (45 rules)
6. **drop_column** — Remove required variable (32 rules)
7. **populate_forbidden** — Set field that should be blank (31 rules)
8. **duplicate_record** — Create key violation (30 rules)
9. **invert_date_order** — Start > End or range inversion (21 rules)
10. **drop_domain** — Remove entire CSV (12 rules)
11. **cross_domain_orphan** — No target records for subject (8 rules)
12. **wrong_derived** — Offset derived numeric value (6 rules)
13. **truncate_with_derived** — Partial date but DY populated (3 rules)
14. **add_column** — Add prohibited column (5 rules)
15. **invalid_codelist** — Value not in controlled terminology (4 rules)
16. **reorder_columns** — Put variable in wrong order (1 rule)

**Key files:**
- `injection/primitives.py` — 850 lines (all 16 primitives + utilities)
- `tests/test_phase3.py` — 400 lines (unit test per primitive)

**Each primitive:**
- Takes DataFrame + row index + parameters
- Returns MutationRecord with before/after values
- Handles missing columns, empty domains gracefully
- Supports RNG for reproducible randomization

---

### ✅ **Phase 4: Rule Prioritization** (COMPLETE)
- [x] `prioritize_rules()` — Sort by eligible row count (ascending)
- [x] `get_rule_priorities()` — Generate priority report
- [x] Edge case handling: missing domains, no guards, ties
- [x] Deterministic ordering (rule_id secondary sort)
- [x] Unit tests with all edge cases

**Key files:**
- `injection/rule_prioritization.py` — 110 lines
- `tests/test_phase4.py` — 260 lines

**Why prioritization matters:**
- In compound mode with density_cap=5, low-volume rules (few eligible rows) can be starved by high-volume rules
- Solution: inject low-volume rules first before cap limits available subjects
- Example: SD0087 (4 eligible rows) must inject before SD0002 (55 eligible rows)

---

## Next Steps (Not Yet Implemented)

### **Phase 5: Engine Orchestration** (ESTIMATED: 2-3 days)
Core orchestration that ties everything together:
- `_run_compound()` — All rules in one dataset
- `_run_isolated()` — One rule per output folder
- Integration with ConformanceLayer for re-derivation
- Density cap tracking and enforcement
- Domain availability filtering
- Post-injection self-validation
- Call to prioritize_rules() for ordering

**Needed implementations:**
- InjectionEngine.run() — Main entry point
- Integrate existing ConformanceLayer reuse (derive_missing_study_days, assign_epochs, apply_cross_domain_repairs)
- Row selection strategy (guard evaluation + rate sampling)
- Subject-level density cap tracking
- Re-derivation delta capture

---

### **Phase 6: Output Writer** (Mostly complete, final touches)
- [ ] Priority order logging in report.txt
- [ ] Validation warning aggregation
- [ ] Summary tables (by rule, category, domain)
- [ ] Column order preservation

---

### **Phase 7: CLI Integration** (ESTIMATED: 0.5 days)
Add two subcommands to existing cli.py (~52 lines):
- `install inject` — Run injection pipeline
- `inject score` — Compare manifest vs validator output

```bash
python -m sdtm_synth inject --input ./clean --output ./injected \
  --mode compound --profile all --seed 42 --density-cap 5

python -m sdtm_synth score --manifest ./injected/manifest.json \
  --validator-output ./p21_results.csv
```

---

### **Phase 8: E2E Integration Testing** (ESTIMATED: 1-2 days)
Test 5 scenarios on real EMPEROR-HFPEF data:
1. Compound mode, all rules, verify >200 errors
2. Isolated mode, specific rules
3. Domain filtering (auto-skip rules)
4. Prioritization order (report verification)
5. Scoring against validator findings

---

## Testing Strategy

### Unit Testing
- **Phase 1**: Config loading, CSV I/O, JSON round-trip
- **Phase 2**: Guard evaluation, rule resolution, catalog lookup
- **Phase 3**: Each primitive on synthetic DataFrames
- **Phase 4**: Prioritization sorting, edge cases
- **Phase 5**: Engine integration (mock conformance layer)

### Integration Testing
- Load EMPEROR-HFPEF, inject rules, verify outputs
- Manifest JSON schema validation
- Re-derivation correctness (RFSTDTC blank → DY blank)
- Density cap enforcement (no subject >5 errors)
- Determinism (same seed → same output)

### Manual Verification
- `verify_phase1.py` — Quick sanity checks
- `verify_phase2.py` — Catalog loading and resolution
- Inspect report.txt for priority order, summaries
- Spot-check dirty CSVs against manifest

---

## Dependencies

```
pandas>=1.3.0      # CSV I/O, data manipulation
PyYAML>=5.4        # Config loading
pytest>=6.2.0      # Unit testing
numpy>=1.20.0      # Random number generation
```

Install:
```bash
pip install -r requirements.txt
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Static RULE_PRIMITIVE_MAP | Auditable, not NLP-guessed; ~10% error if auto-inferred |
| 16 primitives, not 25 categories | Reusable, composable, data-driven |
| Prioritization by default | Prevents starvation in compound mode |
| Reuse ConformanceLayer | Don't rebuild re-derivation; existing code tested |
| dtype=str for CSVs | Preserve all values; prevent auto-NA conversion |
| Manifest as ground-truth | Validator scores against it, not rule IDs |
| Two modes (compound + isolated) | Realistic (compound) + per-rule testing (isolated) |
| Determinism (seed support) | Reproducible runs for CI/testing |

---

## File Structure Summary

```
d:\Care2data_intern\Error Injection\
├── injection/                              # Main module
│   ├── __init__.py                        # Package exports
│   ├── config.py                          # Config loader
│   ├── reader.py                          # CSV reader
│   ├── writer.py                          # Output writer
│   ├── manifest.py                        # Error tracking + scoring
│   ├── rule_parser.py                     # Rule parsing
│   ├── catalog.py                         # Rule catalog + RULE_PRIMITIVE_MAP
│   ├── rule_prioritization.py             # Sorting logic
│   ├── primitives.py                      # 16 mutation primitives
│   └── engine.py                          # (Phase 5) Orchestrator
│
├── config/
│   └── injection/
│       └── defaults.yaml                  # Config: profiles, rates, skip list
│
├── tests/
│   ├── test_phase1.py                    # 350 lines
│   ├── test_phase2.py                    # 350 lines
│   ├── test_phase3.py                    # 400 lines
│   └── test_phase4.py                    # 260 lines
│
├── verify_phase1.py                       # Quick sanity checks
├── verify_phase2.py                       # Catalog verification
├── requirements.txt                        # Dependencies
├── injection_plan_merged.md                # Full technical spec (from planning)
└── README.md                               # This file
```

---

## Quick Start (Once Engine Complete)

```python
from injection import InjectionEngine

engine = InjectionEngine()
manifest = engine.run(
    input_dir="./clean_data",
    output_dir="./injected_data",
    mode="compound",
    profile="all",
    seed=42,
    density_cap=5,
)

print(f"Injected {manifest.total_mutations} errors")
print(f"Manifest saved to {manifest_path}")
```

Or via CLI:
```bash
python -m injection inject \
  --input ./clean_data \
  --output ./injected_data \
  --profile all \
  --seed 42 \
  --density-cap 5

python -m injection score \
  --manifest ./injected_data/manifest.json \
  --validator-output ./validator_findings.csv
```

---

## What to Do Next

**Immediate (Recommended):**
1. Run Phase 1-4 verification: `python verify_phase1.py && python verify_phase2.py`
2. Run unit tests: `pytest tests/test_phase*.py -v`
3. Review RULE_PRIMITIVE_MAP in catalog.py (comprehensive example)

**Short-term:**
1. Implement Phase 5 (Engine) — most of the logic is in place
2. Stub ConformanceLayer calls (or integrate with existing codebase)
3. Phase 7 CLI integration (straightforward)

**Testing:**
1. Load EMPEROR-HFPEF test data
2. Run E2E test (Phase 8)
3. Verify manifest JSON and output CSVs

---

## Questions / Assumptions

1. **ConformanceLayer**: Assuming conformance/layer.py exists with:
   - `derive_missing_study_days()`
   - `assign_epochs()`
   - `apply_cross_domain_repairs()`
   - If not, these will need to be implemented or stubbed.

2. **EMPEROR-HFPEF Data**: Path not yet specified. Phase 8 needs this for e2e testing.

3. **Rule_Cases.csv / Error_Case.csv**: Not yet parsed. Currently hard-coded in RULE_PRIMITIVE_MAP.

4. **Column Order**: Assuming preservation is important; schema defined elsewhere.

---

## Progress Metrics

| Phase | Lines of Code | Unit Tests | Status |
|-------|---------------|-----------|--------|
| 1 | 620 | 40+ | ✅ Complete |
| 2 | 630 | 45+ | ✅ Complete |
| 3 | 850 | 60+ | ✅ Complete |
| 4 | 110 | 25+ | ✅ Complete |
| **Subtotal** | **2,210** | **170+** | **✅** |
| 5 (Engine) | ~800 | 30+ | ⏳ Pending |
| 6 (Writer) | 180 | 10+ | ⏳ Pending |
| 7 (CLI) | 52 | 5+ | ⏳ Pending |
| 8 (E2E) | ~200 | 20+ | ⏳ Pending |
| **Total** | **~3,440** | **~235** | **70% Complete** |

---

## Contact / Support

For details on:
- **Design rationale**: See `injection_plan_merged.md` (comprehensive 600+ line spec)
- **Primitive behavior**: See `injection/primitives.py` docstrings
- **Rule mapping**: See `injection/catalog.py` RULE_PRIMITIVE_MAP
- **Guard expression syntax**: See `injection/rule_parser.py` GuardExpression

---

**Generated**: April 15, 2026  
**Implementation Status**: 70% Complete (Phases 1-4 done, Phases 5-8 pending)
