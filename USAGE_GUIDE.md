# Error Injection System - Usage Guide

## Overview
The error injection system now requires **both profile and category** as mandatory inputs. Error injection will only occur on rules that belong to the specified profile and category combination.

## Command Syntax

```bash
python apply_primitive.py --profile <PROFILE> --category <CATEGORY> [optional arguments]
```

### Required Arguments

- **`--profile`** (required): The injection profile name
  - Examples: `all`, `dates`, `dm`, `findings`, `cross_domain`, `ts`, `structural`, `death`

- **`--category`** (required): The category name within the selected profile
  - Category must belong to the selected profile
  - See available categories below

### Optional Arguments

- **`--input-dir`**: Input dataset directory (default: `D:\Care2data_intern\sdtm_synth_package(39)\output_tj301_p21`)
- **`--output-dir`**: Output directory for results (default: `D:\Care2data_intern\op_for_errror_inj`)
- **`--mode`**: Injection mode - `compound` or `isolated` (default: `compound`)
- **`--seed`**: Random seed for reproducibility (default: from config)
- **`--rate`**: Injection rate 0.0-1.0 (default: from config)
- **`--density-cap`**: Maximum errors per subject (default: from config)

## Available Profiles and Categories

### Profile: `all`
All available categories. Select specific category for focused injection.

### Profile: `dates`
Categories: `dm_date`, `date_cross_domain`

### Profile: `dm`
Categories: `dm_date`, `age_arm`, `dm_cross_domain`

### Profile: `findings`
Categories: `findings_chain_1`, `findings_chain_2`, `findings_individual`

### Profile: `cross_domain`
Categories: `dm_cross_domain`, `date_cross_domain`

### Profile: `ts`
Categories: `ts_domain`, `ts_parmcd_req`, `ts_parmcd_limit`, `ts_tsval`

### Profile: `structural`
Categories: `mandatory_domain`, `mandatory_variable`, `mandatory_variable_2`, `all_domains_1`, `all_domains_2`

### Profile: `death`
Categories: `ae_events`

## Examples

### Example 1: Inject date errors in DM domain
```bash
python apply_primitive.py --profile dm --category dm_date
```

### Example 2: Inject cross-domain date mismatches
```bash
python apply_primitive.py --profile cross_domain --category date_cross_domain
```

### Example 3: Inject findings errors with custom rate
```bash
python apply_primitive.py --profile findings --category findings_individual --rate 0.10
```

### Example 4: Inject TS domain errors with specific seed
```bash
python apply_primitive.py --profile ts --category ts_parmcd_req --seed 123 --density-cap 10
```

### Example 5: Age/arm errors with custom output directory
```bash
python apply_primitive.py --profile dm --category age_arm --output-dir D:\custom_output
```

### Example 6: Trial design errors in isolated mode
```bash
python apply_primitive.py --profile all --category trial_design --mode isolated
```

## Output Files

For each run, the following files are generated in `--output-dir`:

1. **`clean/`** - Original clean datasets (unchanged)
2. **`dirty/`** - Datasets with injected errors
3. **`manifest.json`** - Detailed record of all injected mutations
4. **`report.txt`** - Human-readable summary report
5. **`catalog.py`** - Debug copy of rule catalog used

## How Rule Selection Works

1. **Profile Selection**: The profile determines which categories are available
2. **Category Selection**: Only rules belonging to the selected category are eligible
3. **Rule Filtering**: Rules are filtered by:
   - Profile match (from config)
   - Category match (from embedded category mappings)
   - Domain availability (in input datasets)
   - Guard expressions (rule-specific conditions)
4. **Error Injection**: Only filtered rules have errors injected into the datasets

## Error Handling

The system validates inputs and provides helpful error messages:

- **Invalid Profile**: Shows available profiles
- **Invalid Category**: Shows available categories for the selected profile
- **Missing Datasets**: Shows which domains were not found
- **Guide Errors**: Shows rules that could not be applied due to guard conditions

## Category Descriptions

| Category | Purpose |
|----------|---------|
| `dm_date` | Reference date and study timing errors in Demographics |
| `date_cross_domain` | Date mismatches across domains |
| `age_arm` | Demographics: age, arm assignments |
| `dm_cross_domain` | Demographics cross-domain mismatches |
| `ae_events` | Death cascade rules from AE domain |
| `findings_individual` | Single finding value errors |
| `findings_chain_1` | Chained finding errors (type 1) |
| `findings_chain_2` | Chained finding errors (type 2) |
| `general_obs_1` | General observation timing/visit errors |
| `general_obs_2` | General observation structural/variable errors |
| `interventions` | Intervention and event errors |
| `trial_design` | Trial design and special-purpose domain errors |
| `mandatory_domain` | Domain deletion errors |
| `mandatory_variable` | Variable deletion errors |
| `mandatory_variable_2` | Variable addition errors |
| `ts_domain` | TS domain deletion errors |
| `ts_parmcd_req` | TS parameter code requirement violations |
| `ts_parmcd_limit` | TS parameter value errors |
| `ts_tsval` | TS value and validation errors |
| `special_purpose` | Codelist validation errors |
| `all_domains_1` | PP domain errors |
| `all_domains_2` | TM domain errors |
| `custom_split` | DI/TS parameter label errors |

## Troubleshooting

**Q: How do I see what rules will be injected?**
A: Check the `report.txt` file in the output directory for a detailed list.

**Q: Can I inject multiple categories at once?**
A: Currently, one category per run is required. Run the script multiple times with different categories to inject multiple category types.

**Q: Why didn't all eligible rules get injected?**
A: Rules may be skipped due to:
- Guard expression conditions not met
- No data in target domain
- Subject density cap reached
- Random selection (injection rate < 1.0)

**Q: How do I make results reproducible?**
A: Use the `--seed` argument with the same value for reproducible random selections.
