#!/usr/bin/env python
"""Quick check: Do SCRNFAIL/NOTASSGN values exist in DM?"""

from pathlib import Path
from injection.reader import load_datasets

INPUT_DIR = Path(r"C:\Users\Kanishka R B\Downloads\sdtm_synth_package(39) 1 (1)\output_tj301_p21")
datasets = load_datasets(INPUT_DIR)
dm = datasets.get("DM")

if dm is None:
    print("ERROR: DM not found")
    exit(1)

print("="*80)
print("CHECKING GUARD CONDITION VALUES IN DM")
print("="*80)

# Check ARMCD for SD0011 and SD0053
print("\n1. ARMCD values (for SD0011 and SD0053 guards):")
if "ARMCD" in dm.columns:
    print(f"   Unique ARMCD values: {dm['ARMCD'].unique()}")
    print(f"   SCRNFAIL count: {len(dm[dm['ARMCD'] == 'SCRNFAIL'])}")
    print(f"   NOTASSGN count: {len(dm[dm['ARMCD'] == 'NOTASSGN'])}")
else:
    print("   ERROR: ARMCD not in DM")

# Check ACTARMCD for SD1361 and SD1362
print("\n2. ACTARMCD values (for SD1361 and SD1362 guards):")
if "ACTARMCD" in dm.columns:
    print(f"   Unique ACTARMCD values: {dm['ACTARMCD'].unique()}")
    print(f"   SCRNFAIL count: {len(dm[dm['ACTARMCD'] == 'SCRNFAIL'])}")
    print(f"   NOTASSGN count: {len(dm[dm['ACTARMCD'] == 'NOTASSGN'])}")
else:
    print("   ERROR: ACTARMCD not in DM")

print("\n" + "="*80)
print("CONCLUSION:")
print("="*80)
print("If all counts are 0, the data doesn't have these special values.")
print("That's why the guarded rules aren't executing - there are no eligible rows!")
