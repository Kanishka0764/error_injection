#!/usr/bin/env python
"""Run full SDTM error-injection orchestration via InjectionEngine."""

from pathlib import Path

from injection.engine import InjectionEngine


INPUT_DIR = Path(r"D:\Care2data_intern\sdtm_synth_package(39)\output_tj301_p21")
OUTPUT_DIR = Path(r"D:\Care2data_intern\op_for_errror_inj")


def main() -> None:
    category = "date_cross_domain"

    engine = InjectionEngine()
    manifest = engine.run(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        mode="compound",
        profile="all",
        categories=[category],
    )

    print(f"Injected mutations: {manifest.total_mutations}")
    print(f"Rules injected: {manifest.rules_injected}")
    print(f"Category: {category}")
    print(f"Output written to: {OUTPUT_DIR}")
    print(f"Manifest: {OUTPUT_DIR / 'manifest.json'}")


if __name__ == "__main__":
    main()