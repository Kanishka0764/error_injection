# Error Injection System - Architecture & Implementation

## Overview

The system has been fully modified to require **profile and category as mandatory command-line inputs**. Only rules belonging to the specified profile and category will be executed for error injection.

## Command Line Interface

### Usage
```bash
python apply_primitive.py --profile <PROFILE> --category <CATEGORY> [options]
```

### Required Arguments
- `--profile`: Profile name (e.g., `all`, `dates`, `dm`, `findings`, `cross_domain`, `ts`, `structural`, `death`)
- `--category`: Category within the selected profile (e.g., `dm_date`, `date_cross_domain`, `age_arm`)

### Optional Arguments
- `--input-dir`: Input dataset directory
- `--output-dir`: Output directory for results
- `--mode`: `compound` (default) or `isolated`
- `--seed`: Random seed for reproducibility
- `--rate`: Injection rate (0.0-1.0)
- `--density-cap`: Maximum errors per subject

## System Architecture

### 1. **apply_primitive.py** - Entry Point
**File**: `apply_primitive.py`

**Responsibilities**:
- Parse command-line arguments (profile, category, options)
- Validate profile and category against available options
- Create output directory
- Initialize and run InjectionEngine
- Display results summary

**Key Code**:
```python
parser.add_argument("--profile", type=str, required=True)
parser.add_argument("--category", type=str, required=True)

# Validate inputs
if args.profile not in config.profiles:
    # Error with available profiles
if args.category not in profile_categories:
    # Error with available categories

# Run engine with selected profile and category
manifest = engine.run(
    input_dir=...,
    output_dir=...,
    profile=args.profile,
    categories=[args.category],  # Pass as list
    ...
)
```

### 2. **InjectionEngine** - Orchestration
**File**: `injection/engine.py`

**Responsibilities**:
- Load datasets from input directory
- Resolve rules based on profile, category, and domain
- Execute rule prioritization if enabled
- Apply error injection using primitives
- Generate manifest and reports

**Key Method - run()**:
```python
def run(self, ..., profile: str = "all", categories: Optional[List[str]] = None, ...):
    # Load datasets
    datasets = load_datasets(input_dir)
    
    # Resolve rules using catalog
    resolved_rules = self.catalog.resolve(
        profile=profile,
        categories=categories,  # Critical: pass categories here
        rules=rules,
        exclude_rules=exclude_rules,
        domains=domains,
    )
    
    # Filter by available domains
    active_rules, skipped = self._filter_by_available_domains(resolved_rules, datasets)
    
    # Execute injection
    if mode == "compound":
        # Inject all rules together
    else:
        # Inject rules in isolation
```

### 3. **RuleCatalog** - Rule Selection & Filtering
**File**: `injection/catalog.py`

**Responsibilities**:
- Store all 472 injectable rules with their properties
- Resolve rules based on profile, category, domain, and guards
- Generate RuleSpec objects for matched rules

**Key Data Structure - RULE_PRIMITIVE_MAP**:
```python
RULE_PRIMITIVE_MAP = {
    "SD0087": {
        "primitive": "blank_field",
        "params": {"field": "RFSTDTC"},
        "domain": "DM",
        "guard": "ARMCD not in (SCRNFAIL,NOTASSGN,)",
        "category": "dm_date",  # <-- Category field for each rule
    },
    # ... 472 rules total
}
```

**Key Method - resolve()**:
```python
def resolve(self, profile="all", categories=None, rules=None, 
            exclude_rules=None, domains=None) -> List[RuleSpec]:
    
    rule_ids = set()
    
    # Start with rules if explicit, otherwise use all
    if rules:
        rule_ids.update(rules)
    else:
        rule_ids.update(self.rule_map.keys())
    
    # Remove excluded rules
    if exclude_rules:
        rule_ids.difference_update(exclude_rules)
    
    # CRITICAL: Filter by categories
    if categories:
        categories_set = set(categories)
        filtered = set()
        for rule_id in rule_ids:
            entry = self.rule_map.get(rule_id)
            rule_category = entry.get("category", "")
            if rule_category in categories_set:
                filtered.add(rule_id)
        rule_ids = filtered
    
    # Filter by domains if specified
    if domains:
        domains_set = set(d.upper() for d in domains)
        filtered = set()
        for rule_id in rule_ids:
            entry = self.rule_map.get(rule_id)
            rule_domains = set(entry.get("domain_expanded", [entry.get("domain", "--")]))
            if "--" in rule_domains or rule_domains & domains_set:
                filtered.add(rule_id)
        rule_ids = filtered
    
    # Build RuleSpec objects
    specs = []
    for rule_id in sorted(rule_ids):
        spec = self.get_spec(rule_id)
        if spec:
            specs.append(spec)
    
    return specs
```

### 4. **Config** - Profile & Category Definitions
**File**: `config/injection/defaults.yaml`

**Defines**:
- Available profiles and their categories
- Default injection parameters (rate, seed, density_cap)
- Category-specific injection rates
- Rules to skip by default

**Example**:
```yaml
profiles:
  all:
    categories: 
      - dm_date
      - date_cross_domain
      - age_arm
      - ...

  cross_domain:
    categories: [dm_cross_domain, date_cross_domain]

  dm:
    categories: [dm_date, age_arm, dm_cross_domain]

defaults:
  rate: 0.05
  seed: 42
  density_cap: 5
  prioritize_rules: true

category_rates:
  dm_date: 0.10
  date_cross_domain: 0.05
  age_arm: 0.10
  ...
```

## Data Flow

### Rule Selection Pipeline

```
User Input (--profile, --category)
    ↓
apply_primitive.py validates against Config
    ↓
InjectionEngine.run() receives (profile, categories)
    ↓
RuleCatalog.resolve(profile=X, categories=[Y]) called
    ↓
Category Filtering:
  - For each rule in RULE_PRIMITIVE_MAP
  - Check if rule.category matches specified category
  - Only return matching RuleSpecs
    ↓
Domain Filtering:
  - Filter resolved rules by available datasets
  - Only keep rules applicable to loaded domains
    ↓
Guard Expression Evaluation:
  - At runtime during injection
  - Only execute rule if guard condition is true
    ↓
Error Injection:
  - Apply selected rules to matching records
  - Track mutations in manifest
```

## Category Mapping

Each of the 472 rules belongs to exactly one category:

| Category | Rule Count | Examples |
|----------|-----------|----------|
| `dm_date` | 18 | SD0087, SD0088, SD1002, ... |
| `date_cross_domain` | 7 | SD0013, SD0038, SD1085, ... |
| `age_arm` | 26 | SD0084, SD0093, SD1001, ... |
| `dm_cross_domain` | 15 | SD0066, SD0069, SD0070, ... |
| `ae_events` | 9 | SD1254, SD1255, SD1347, ... |
| `findings_individual` | 36 | SD0047, SD0040, SD1137, ... |
| `general_obs_1` | 35 | SD0002, SD0003, SD0004, ... |
| `general_obs_2` | 56 | SD0055, SD0056, SD0057, ... |
| `interventions` | 67 | SD0006, SD0009, SD0014, ... |
| `trial_design` | 44 | SD0067, SD0068, SD0072, ... |
| `mandatory_domain` | 12 | SD1020, SD1111, SD1109, ... |
| `mandatory_variable` | 33 | SD1083, SD1129, SD2270, ... |
| `mandatory_variable_2` | 6 | SD0058, SD1073, SD1074, ... |
| `ts_domain` | 44 | SD2201, SD2202, SD2203, ... |
| `ts_parmcd_req` | 9 | SD1214, SD1216, SD1218, ... |
| `ts_parmcd_limit` | 24 | SD1215, SD1217, SD1219, ... |
| `ts_tsval` | 28 | SD2252, SD2254, SD2255, ... |
| `special_purpose` | 3 | CT2001, CT2002, CT2003 |
| `all_domains_1` | 3 | SD1348, SD1350, SD1351 |
| `all_domains_2` | 2 | SD1355, SD1356 |
| `custom_split` | 3 | SD0019, SD0020, SD0021 |
| `findings_chain_1` | 1 | SD1263 |
| `findings_chain_2` | 3 | SD1336, SD1337, SD1338 |
| **TOTAL** | **472** | |

## Profile-to-Category Mapping

```
Profile: all
└─ All 23 categories (472 rules)

Profile: dates
├─ dm_date (18 rules)
└─ date_cross_domain (7 rules)

Profile: dm
├─ dm_date (18 rules)
├─ age_arm (26 rules)
└─ dm_cross_domain (15 rules)

Profile: findings
├─ findings_chain_1 (1 rule)
├─ findings_chain_2 (3 rules)
└─ findings_individual (36 rules)

Profile: cross_domain
├─ dm_cross_domain (15 rules)
└─ date_cross_domain (7 rules)

Profile: ts
├─ ts_domain (44 rules)
├─ ts_parmcd_req (9 rules)
├─ ts_parmcd_limit (24 rules)
└─ ts_tsval (28 rules)

Profile: structural
├─ mandatory_domain (12 rules)
├─ mandatory_variable (33 rules)
├─ mandatory_variable_2 (6 rules)
├─ all_domains_1 (3 rules)
└─ all_domains_2 (2 rules)

Profile: death
└─ ae_events (9 rules)
```

## Example Execution Flows

### Example 1: Cross-Domain Date Mismatches
```bash
python apply_primitive.py --profile cross_domain --category date_cross_domain
```

**Flow**:
1. apply_primitive.py validates: profile="cross_domain" ✓, category="date_cross_domain" ✓
2. InjectionEngine.run() called with profile="cross_domain", categories=["date_cross_domain"]
3. RuleCatalog.resolve() filters:
   - Includes all rules with category="date_cross_domain": [SD0013, SD0038, SD1085, SD1086, SD1202, SD1203, SD1204]
   - Filters by available domains (e.g., removes rules for non-existent domains)
   - Returns 7 RuleSpec objects
4. Injection Engine applies 7 rules to datasets, tracking each mutation
5. Output files generated in output directory

### Example 2: Demographics Date Issues
```bash
python apply_primitive.py --profile dm --category dm_date --rate 0.10
```

**Flow**:
1. Validation: profile="dm" ✓, category="dm_date" ✓
2. InjectionEngine.run() with profile="dm", categories=["dm_date"], rate=0.10
3. RuleCatalog.resolve() returns rules with category="dm_date" (18 rules)
4. Domain filter keeps rules applicable to loaded datasets
5. Rules: [SD0087, SD0088, SD1002, SD1208, SD1258, SD1342, SD1343, SD2004, SD2005, SD1209, SD1210, SD1213, SD1259, SD1334, SD1335, SD1366, SD1376, SD2023]
6. Injection at 10% rate (overrides default 5%)

### Example 3: TS Parameter Validation
```bash
python apply_primitive.py --profile ts --category ts_parmcd_req --seed 42
```

**Flow**:
1. Validation: profile="ts" ✓, category="ts_parmcd_req" ✓
2. InjectionEngine.run() with profile="ts", categories=["ts_parmcd_req"], seed=42
3. RuleCatalog.resolve() returns 9 rules for TS parameter code duplicates
4. Reproducible results with fixed seed
5. Outputs: manifest.json, report.txt, clean/, dirty/

## Key Features

✅ **Required Profile & Category**: Must specify both to proceed
✅ **Input Validation**: Checks profile exists and category belongs to profile
✅ **Automatic Filtering**: Only selected rules execute
✅ **Category-Based Rates**: Different injection rates per category
✅ **Domain Awareness**: Rules filtered by available datasets
✅ **Guard Expressions**: Run-time conditions for rule application
✅ **Reproducibility**: Seed parameter for consistent results
✅ **Comprehensive Tracking**: All mutations recorded in manifest
✅ **Detailed Reporting**: Rules tried, skipped, and injected with reasons

## Validation Checks

The system validates at multiple levels:

### 1. Command-Line Validation (apply_primitive.py)
```python
if args.profile not in config.profiles:
    print("ERROR: Profile not found")
    print(f"Available profiles: {available}")
if args.category not in profile_categories:
    print("ERROR: Category not in profile")
    print(f"Available categories: {profile_categories}")
```

### 2. Rule Filtering Validation (RuleCatalog.resolve)
```python
if categories:
    # Only include rules matching specified categories
    if rule_category in categories_set:
        filtered.add(rule_id)
```

### 3. Domain Availability Check (InjectionEngine._filter_by_available_domains)
```python
# Check if rule's domain exists in loaded datasets
if domain in datasets:
    # Proceed with rule
```

### 4. Guard Expression Validation (During Injection)
```python
# Check if guard condition is met for each record
if spec.guard_expression:
    if spec.guard_expression.evaluate(record):
        # Apply error to record
```

## Output Structure

```
output_dir/
├── clean/                    # Original datasets (unchanged)
│   ├── DM.csv
│   ├── AE.csv
│   └── ...
├── dirty/                    # Datasets with injected errors
│   ├── DM.csv               # Modified
│   ├── AE.csv               # Modified
│   └── ...
├── manifest.json            # Detailed mutation record
├── report.txt               # Human-readable summary
└── catalog_debug.py         # Debug copy of rule catalog
```

## Summary

The codebase has been fully refactored to:
1. **Require profile and category as command-line inputs**
2. **Validate inputs against configuration**
3. **Automatically filter rules by category in RuleCatalog.resolve()**
4. **Only execute rules matching the specified profile and category**
5. **Track and report all executed mutations**

Users can now precisely control which error types are injected by specifying profile and category combinations.
