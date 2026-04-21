# Quick Reference - Command Examples

## Basic Syntax
```bash
python apply_primitive.py --profile <PROFILE> --category <CATEGORY>
```

## Common Examples

### 1. DM Domain Date Errors
```bash
python apply_primitive.py --profile dm --category dm_date
```
- Targets: Reference date blanking, date inversion in Demographics
- Rules: 18
- Default Rate: 10%

### 2. Cross-Domain Date Mismatches
```bash
python apply_primitive.py --profile cross_domain --category date_cross_domain
```
- Targets: Date comparisons across domains
- Rules: 7
- Default Rate: 5%

### 3. Demographics Age/Arm Issues
```bash
python apply_primitive.py --profile dm --category age_arm
```
- Targets: Age unit blanking, arm/armcd mismatches
- Rules: 26
- Default Rate: 10%

### 4. Findings Individual Value Errors
```bash
python apply_primitive.py --profile findings --category findings_individual
```
- Targets: Single finding mismatches (value vs unit, etc.)
- Rules: 36
- Default Rate: 3%

### 5. General Observations - Timing
```bash
python apply_primitive.py --profile all --category general_obs_1
```
- Targets: Date/time/visit errors
- Rules: 35
- Default Rate: 5%

### 6. General Observations - Structure
```bash
python apply_primitive.py --profile all --category general_obs_2
```
- Targets: Field blanking, invalid values, duplicates
- Rules: 56
- Default Rate: 5%

### 7. Intervention & Event Errors
```bash
python apply_primitive.py --profile all --category interventions
```
- Targets: AE, DS, EX domain errors
- Rules: 67
- Default Rate: 5%

### 8. Trial Design Errors
```bash
python apply_primitive.py --profile all --category trial_design
```
- Targets: TA, TE, TI domain errors
- Rules: 44
- Default Rate: 5%

### 9. Mandatory Domain Violations
```bash
python apply_primitive.py --profile structural --category mandatory_domain
```
- Targets: Entire domain deletion
- Rules: 12
- Default Rate: 100%

### 10. Mandatory Variable Errors
```bash
python apply_primitive.py --profile structural --category mandatory_variable
```
- Targets: Required field deletion
- Rules: 33
- Default Rate: 5%

### 11. TS Domain Errors
```bash
python apply_primitive.py --profile ts --category ts_domain
```
- Targets: Trial summary row deletion
- Rules: 44
- Default Rate: 5%

### 12. TS Parameter Code Errors
```bash
python apply_primitive.py --profile ts --category ts_parmcd_req
```
- Targets: TS parameter code duplications
- Rules: 9
- Default Rate: 5%

### 13. Death Cascade Rules
```bash
python apply_primitive.py --profile death --category ae_events
```
- Targets: AE, DS, SS death-related mismatches
- Rules: 9
- Default Rate: 8%

## With Optional Arguments

### Custom Injection Rate
```bash
python apply_primitive.py --profile dm --category dm_date --rate 0.20
```
- Injects DM date errors at 20% (overrides default 10%)

### Custom Output Directory
```bash
python apply_primitive.py --profile dm --category dm_date --output-dir C:\custom_path
```
- Results saved to C:\custom_path

### Reproducible Results
```bash
python apply_primitive.py --profile dm --category dm_date --seed 12345
```
- Same seed produces identical mutation selection

### Isolated Injection Mode
```bash
python apply_primitive.py --profile dm --category dm_date --mode isolated
```
- Each rule injected separately (slower, more detailed tracking)

### Combined Options
```bash
python apply_primitive.py \
  --profile ts \
  --category ts_parmcd_req \
  --rate 0.50 \
  --seed 999 \
  --density-cap 10 \
  --mode compound \
  --output-dir D:\results\ts_test
```
- TS parameter code errors
- 50% injection rate
- Fixed seed for reproducibility
- Max 10 errors per subject
- Compound mode (faster)
- Custom output directory

## Profile Contents

### Profile: all
- All 23 categories, 472 rules
```bash
python apply_primitive.py --profile all --category <CATEGORY>
```

### Profile: dates
- dm_date, date_cross_domain
```bash
python apply_primitive.py --profile dates --category dm_date
python apply_primitive.py --profile dates --category date_cross_domain
```

### Profile: dm
- dm_date, age_arm, dm_cross_domain
```bash
python apply_primitive.py --profile dm --category dm_date
python apply_primitive.py --profile dm --category age_arm
python apply_primitive.py --profile dm --category dm_cross_domain
```

### Profile: findings
- findings_chain_1, findings_chain_2, findings_individual
```bash
python apply_primitive.py --profile findings --category findings_individual
python apply_primitive.py --profile findings --category findings_chain_1
python apply_primitive.py --profile findings --category findings_chain_2
```

### Profile: cross_domain
- dm_cross_domain, date_cross_domain
```bash
python apply_primitive.py --profile cross_domain --category date_cross_domain
python apply_primitive.py --profile cross_domain --category dm_cross_domain
```

### Profile: ts
- ts_domain, ts_parmcd_req, ts_parmcd_limit, ts_tsval
```bash
python apply_primitive.py --profile ts --category ts_domain
python apply_primitive.py --profile ts --category ts_parmcd_req
python apply_primitive.py --profile ts --category ts_parmcd_limit
python apply_primitive.py --profile ts --category ts_tsval
```

### Profile: structural
- mandatory_domain, mandatory_variable, mandatory_variable_2, all_domains_1, all_domains_2
```bash
python apply_primitive.py --profile structural --category mandatory_domain
python apply_primitive.py --profile structural --category mandatory_variable
python apply_primitive.py --profile structural --category mandatory_variable_2
python apply_primitive.py --profile structural --category all_domains_1
python apply_primitive.py --profile structural --category all_domains_2
```

### Profile: death
- ae_events
```bash
python apply_primitive.py --profile death --category ae_events
```

## Error Messages

### Missing Required Argument
```
error: the following arguments are required: --profile, --category
```
**Solution**: Provide both --profile and --category

### Invalid Profile
```
ERROR: Profile 'xyz' not found.
Available profiles: ['all', 'dates', 'dm', ...]
```
**Solution**: Use a profile from the available list

### Invalid Category for Profile
```
ERROR: Category 'abc' not found in profile 'dm'.
Available categories for 'dm': ['dm_date', 'age_arm', 'dm_cross_domain']
```
**Solution**: Use a category that belongs to the selected profile

### Input Directory Not Found
```
ERROR: Input directory not found: ...
```
**Solution**: Verify --input-dir path exists and contains SDTM datasets

### No Data in Target Domain
```
[In report.txt] Rule SD1234 skipped: No concrete target domain available
```
**Solution**: This is normal - rule doesn't apply to your datasets

## Output Files

After each run:
- **manifest.json** - All mutations in JSON format
- **report.txt** - Summary with rules tried/skipped/injected
- **clean/** - Original datasets (for comparison)
- **dirty/** - Modified datasets with errors

## Tips

1. **Start Small**: Test with a single category first
2. **Check Report**: Always review report.txt for rule application details
3. **Use Seeds**: For reproducible testing, specify --seed
4. **Dashboard**: Write scripts to compare clean/ vs dirty/ datasets
5. **Batch**: Run multiple profiles/categories to build comprehensive test set

## Environment Setup

```bash
# Activate virtual environment
cd d:\Care2data_intern\Error Injection
.\error_inj_venv\Scripts\Activate.ps1

# Run a test
python apply_primitive.py --profile dm --category dm_date

# Deactivate when done
deactivate
```
