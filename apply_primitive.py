#!/usr/bin/env python
"""
Apply one SDTM rule mutation to real CSV datasets.

Primary mode is rule-driven: provide --rule-id and this script resolves
primitive/domain/default params from RuleCatalog. Optional --params can override
or supplement catalog params.
"""

import argparse
import inspect
import json
from pathlib import Path
from typing import Any, Dict

import numpy as np

from injection import primitives
from injection.catalog import RuleCatalog
from injection.reader import load_datasets
from injection.writer import write_datasets


def parse_params(params_text: str) -> Dict[str, Any]:
    """Parse JSON params payload passed via CLI."""
    if not params_text:
        return {}
    parsed = json.loads(params_text)
    if not isinstance(parsed, dict):
        raise ValueError("--params must be a JSON object")
    return parsed


def _resolve_generic_param(
    raw_value: Any,
    domain: str,
    columns: Any,
) -> Any:
    """Resolve generic -- style vars to domain-specific variables when possible."""
    if not isinstance(raw_value, str) or not raw_value.startswith("--"):
        return raw_value
    return primitives._resolve_prefix(raw_value, domain, list(columns)) or raw_value


def normalize_params(
    primitive_name: str,
    params: Dict[str, Any],
    datasets: Dict[str, Any],
    domain: str,
    row_idx: int,
) -> Dict[str, Any]:
    """Normalize catalog params to primitive signature and resolve generic vars."""
    normalized = dict(params)

    if primitive_name == "cross_domain_mismatch":
        if "source" in normalized and "source_domain" not in normalized:
            normalized["source_domain"] = normalized.pop("source")
        if "target" in normalized and "target_domain" not in normalized:
            normalized["target_domain"] = normalized.pop("target")

    if primitive_name == "cross_domain_orphan":
        if "source" in normalized and "source_domain" not in normalized:
            normalized["source_domain"] = normalized.pop("source")
        if "target" in normalized and "target_domain" not in normalized:
            normalized["target_domain"] = normalized.pop("target")
        if "key" in normalized and "key_field" not in normalized:
            normalized["key_field"] = normalized.pop("key")

        key_field = normalized.get("key_field")
        source_domain = normalized.get("source_domain", domain)
        if "usubjid" not in normalized and key_field and source_domain in datasets:
            src_df = datasets[source_domain]
            if key_field in src_df.columns and len(src_df.index) > row_idx:
                normalized["usubjid"] = src_df.iloc[row_idx][key_field]

    primary_domain = domain
    if primitive_name == "cross_domain_mismatch" and "source_domain" in normalized:
        primary_domain = str(normalized["source_domain"]).upper()
    elif primitive_name == "cross_domain_orphan" and "source_domain" in normalized:
        primary_domain = str(normalized["source_domain"]).upper()

    primary_df = datasets.get(primary_domain)
    if primary_df is not None:
        for key in ["field", "start_field", "end_field", "date_field", "dy_field", "column", "col_a", "col_b"]:
            if key in normalized:
                normalized[key] = _resolve_generic_param(normalized[key], primary_domain, primary_df.columns)

        if "source_field" in normalized:
            src = normalized["source_field"]
            if isinstance(src, str) and "," in src:
                normalized["source_field"] = ",".join(
                    _resolve_generic_param(part.strip(), primary_domain, primary_df.columns) for part in src.split(",")
                )
            else:
                normalized["source_field"] = _resolve_generic_param(src, primary_domain, primary_df.columns)

    target_domain = normalized.get("target_domain")
    target_df = datasets.get(str(target_domain).upper()) if target_domain else None
    if target_df is not None and "target_field" in normalized:
        tgt = normalized["target_field"]
        if isinstance(tgt, str) and "," in tgt:
            normalized["target_field"] = ",".join(
                _resolve_generic_param(part.strip(), str(target_domain).upper(), target_df.columns)
                for part in tgt.split(",")
            )
        else:
            normalized["target_field"] = _resolve_generic_param(tgt, str(target_domain).upper(), target_df.columns)

    return normalized


def build_call_kwargs(
    func: Any,
    datasets: Dict[str, Any],
    domain: str,
    row_idx: int,
    rule_id: str,
    params: Dict[str, Any],
    rng: np.random.Generator,
) -> Dict[str, Any]:
    """Construct kwargs for a primitive based on its function signature."""
    kwargs: Dict[str, Any] = {}
    sig = inspect.signature(func)

    for name, param in sig.parameters.items():
        if name == "df":
            if not domain:
                raise ValueError("--domain is required for primitives that use df")
            if domain not in datasets:
                raise ValueError(f"Domain '{domain}' not found in input datasets")
            df = datasets[domain]
            df.attrs["domain"] = domain
            kwargs[name] = df
        elif name == "datasets":
            kwargs[name] = datasets
        elif name == "row_idx":
            kwargs[name] = row_idx
        elif name == "rule_id":
            kwargs[name] = rule_id
        elif name == "rng":
            kwargs[name] = rng
        elif name in params:
            kwargs[name] = params[name]
        elif param.default is inspect._empty:
            raise ValueError(
                f"Missing required parameter '{name}'. Pass it in --params JSON."
            )

    return kwargs


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply one rule mutation to SDTM datasets")
    parser.add_argument("--input-dir", required=True, help="Folder containing domain CSV files")
    parser.add_argument("--output-dir", help="Folder to write modified CSV files (default: input-dir)")
    parser.add_argument("--primitive", default="", help="Optional override primitive name (normally resolved from --rule-id)")
    parser.add_argument("--domain", default="", help="Optional override domain name (normally resolved from --rule-id)")
    parser.add_argument("--row-idx", type=int, default=0, help="Row index for row-based primitives")
    parser.add_argument("--rule-id", required=True, help="Rule ID to resolve from RuleCatalog")
    parser.add_argument(
        "--params",
        default="{}",
        help="JSON object to override/supplement catalog params, e.g. '{\"field\": \"RFSTDTC\"}'",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed for rng-based primitives")

    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir) if args.output_dir else input_dir

    datasets = load_datasets(input_dir)
    cli_params = parse_params(args.params)
    rng = np.random.default_rng(args.seed)

    catalog = RuleCatalog()
    spec = catalog.get_spec(args.rule_id)
    if spec is None:
        raise ValueError(f"Rule '{args.rule_id}' not found or not injectable")

    primitive_name = args.primitive if args.primitive else spec.primitive
    domain = args.domain.upper() if args.domain else (spec.domain or "")

    if domain == "--":
        raise ValueError(
            "Resolved domain is '--' (generic). Pass --domain with a concrete domain like AE or DM."
        )

    merged_params = dict(spec.params)
    merged_params.update(cli_params)
    params = normalize_params(
        primitive_name=primitive_name,
        params=merged_params,
        datasets=datasets,
        domain=domain,
        row_idx=args.row_idx,
    )

    if not hasattr(primitives, primitive_name):
        raise ValueError(f"Unknown primitive: {primitive_name}")

    func = getattr(primitives, primitive_name)
    kwargs = build_call_kwargs(
        func=func,
        datasets=datasets,
        domain=domain,
        row_idx=args.row_idx,
        rule_id=args.rule_id,
        params=params,
        rng=rng,
    )

    result = func(**kwargs)
    write_datasets(datasets, output_dir)

    if isinstance(result, list):
        payload = [r.to_dict() for r in result]
    else:
        payload = result.to_dict()

    print(json.dumps(payload, indent=2))
    print(f"Rule: {args.rule_id} | Primitive: {primitive_name} | Domain: {domain}")
    print(f"Updated datasets written to: {output_dir}")


if __name__ == "__main__":
    main()