#!/usr/bin/env python
"""Run full SDTM error-injection orchestration via InjectionEngine."""

import argparse
from pathlib import Path

from injection.engine import InjectionEngine
from injection.config import Config


INPUT_DIR = Path(r"C:\Users\Kanishka R B\Downloads\sdtm_synth_package(39) 1 (1)\output_tj301_p21")
OUTPUT_DIR = Path(r"C:\Users\Kanishka R B\Downloads\error_inj_op")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run SDTM error injection with required profile and category inputs."
    )
    
    parser.add_argument(
        "--profile",
        type=str,
        required=True,
        help="Profile name (e.g., 'all', 'dates', 'dm', 'findings', 'cross_domain', 'ts', 'structural', 'death')",
    )
    
    parser.add_argument(
        "--category",
        type=str,
        required=True,
        help="Category name from the selected profile (e.g., 'dm_date', 'date_cross_domain', 'age_arm')",
    )
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(INPUT_DIR),
        help=f"Input directory path (default: {INPUT_DIR})",
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help=f"Output directory path (default: {OUTPUT_DIR})",
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        choices=["compound", "isolated"],
        default="compound",
        help="Injection mode: 'compound' or 'isolated' (default: compound)",
    )
    
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility (default: from config)",
    )
    
    parser.add_argument(
        "--rate",
        type=float,
        default=None,
        help="Injection rate 0.0-1.0 (default: from config)",
    )
    
    parser.add_argument(
        "--density-cap",
        type=int,
        default=None,
        help="Maximum errors per subject (default: from config)",
    )
    
    args = parser.parse_args()
    
    # Validate profile and category
    config = Config()
    
    if args.profile not in config.profiles:
        available = list(config.profiles.keys())
        print(f"ERROR: Profile '{args.profile}' not found.")
        print(f"Available profiles: {available}")
        exit(1)
    
    profile_categories = config.profiles[args.profile].get("categories", [])
    if args.category not in profile_categories:
        print(f"ERROR: Category '{args.category}' not found in profile '{args.profile}'.")
        print(f"Available categories for '{args.profile}': {profile_categories}")
        exit(1)
    
    # Create output directory if it doesn't exist
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Run the injection engine with the selected profile and category
    engine = InjectionEngine()
    manifest = engine.run(
        input_dir=Path(args.input_dir),
        output_dir=output_path,
        mode=args.mode,
        profile=args.profile,
        categories=[args.category],
        seed=args.seed,
        rate=args.rate,
        density_cap=args.density_cap,
    )

    print("\n" + "=" * 70)
    print("ERROR INJECTION COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"Profile:                {args.profile}")
    print(f"Category:               {args.category}")
    print(f"Mode:                   {args.mode}")
    print(f"Input directory:        {args.input_dir}")
    print(f"Output directory:       {output_path}")
    print(f"Rules available:        {manifest.rules_available}")
    print(f"Rules injectable:       {manifest.rules_injectable}")
    print(f"Rules injected:         {manifest.rules_injected}")
    print(f"Total mutations:        {manifest.total_mutations}")
    print(f"Manifest file:          {output_path / 'manifest.json'}")
    print(f"Report file:            {output_path / 'report.txt'}")
    print("=" * 70)


if __name__ == "__main__":
    main()