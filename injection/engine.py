"""
Main orchestration engine for SDTM error injection.
Coordinates loading, rule resolution, prioritization, injection, re-derivation, and output.
"""

from __future__ import annotations

import inspect
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple

import numpy as np
import pandas as pd

from injection import primitives
from injection.catalog import RuleCatalog
from injection.config import Config
from injection.manifest import InjectionManifest, MutationRecord
from injection.reader import load_datasets
from injection.rule_parser import RuleSpec
from injection.rule_prioritization import prioritize_rules
from injection.writer import write_datasets, write_manifest, write_report, copy_clean_datasets


class InjectionEngine:
    """Orchestrates the end-to-end error injection pipeline."""

    def __init__(self, config_path: Optional[Path] = None):
        self.config = Config(config_path=config_path)
        self.catalog = RuleCatalog()

    def run(
        self,
        input_dir: str | Path,
        output_dir: str | Path,
        mode: str = "compound",
        profile: str = "all",
        categories: Optional[List[str]] = None,
        rules: Optional[List[str]] = None,
        exclude_rules: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
        seed: Optional[int] = None,
        rate: Optional[float] = None,
        density_cap: Optional[int] = None,
        prioritize: Optional[bool] = None,
    ) -> InjectionManifest:
        """
        Run the full injection pipeline.

        Returns:
            InjectionManifest for the executed run
        """
        mode = mode.lower().strip()
        if mode not in {"compound", "isolated"}:
            raise ValueError("mode must be either 'compound' or 'isolated'")

        input_dir = Path(input_dir)
        output_dir = Path(output_dir)

        run_seed = seed if seed is not None else int(self.config.defaults.get("seed", 42))
        run_rate = rate if rate is not None else float(self.config.defaults.get("rate", 0.05))
        run_density_cap = density_cap if density_cap is not None else int(self.config.defaults.get("density_cap", 5))
        run_prioritize = (
            prioritize
            if prioritize is not None
            else bool(self.config.defaults.get("prioritize_rules", True))
        )

        datasets = load_datasets(input_dir)
        clean_datasets = {domain: df.copy(deep=True) for domain, df in datasets.items()}
        rules_available = len(self.catalog.rule_map)

        resolved_rules = self.catalog.resolve(
            profile=profile,
            categories=categories,
            rules=rules,
            exclude_rules=exclude_rules,
            domains=domains,
            category_rules=self.config.category_rules,
        )

        active_rules, skipped = self._filter_by_available_domains(resolved_rules, datasets)

        if run_prioritize:
            active_rules = prioritize_rules(active_rules, datasets)

        if mode == "compound":
            manifest, warnings = self._run_compound(
                datasets=datasets,
                input_dir=input_dir,
                profile=profile,
                rules_available=rules_available,
                active_rules=active_rules,
                skipped_rules=skipped,
                seed=run_seed,
                rate=run_rate,
                density_cap=run_density_cap,
                prioritize_flag=run_prioritize,
            )

            copy_clean_datasets(clean_datasets, output_dir / "clean")
            write_datasets(datasets, output_dir / "dirty")
            write_manifest(manifest, output_dir / "manifest.json")
            write_report(
                manifest,
                skipped_rules=[(row["rule_id"], row["reason"]) for row in skipped],
                warnings=warnings,
                output_path=output_dir / "report.txt",
            )
            return manifest

        return self._run_isolated(
            clean_datasets=clean_datasets,
            input_dir=input_dir,
            output_dir=output_dir,
            profile=profile,
            rules_available=rules_available,
            active_rules=active_rules,
            skipped_rules=skipped,
            seed=run_seed,
            rate=run_rate,
            density_cap=run_density_cap,
            prioritize_flag=run_prioritize,
        )

    def _run_compound(
        self,
        datasets: Dict[str, pd.DataFrame],
        input_dir: Path,
        profile: str,
        rules_available: int,
        active_rules: List[RuleSpec],
        skipped_rules: List[Dict[str, str]],
        seed: int,
        rate: float,
        density_cap: int,
        prioritize_flag: bool,
    ) -> Tuple[InjectionManifest, List[str]]:
        rng = np.random.default_rng(seed)
        manifest = InjectionManifest(
            generated_at=datetime.utcnow().isoformat(),
            seed=seed,
            mode="compound",
            source_dir=str(input_dir),
            profile=profile,
            rate=rate,
            density_cap=density_cap,
            prioritize_rules=prioritize_flag,
            rules_available=rules_available,
            rules_injectable=len(active_rules),
            rules_skipped=len(skipped_rules),
            rules_injected=0,
            total_mutations=0,
        )

        for row in skipped_rules:
            manifest.add_skipped_rule(row["rule_id"], row["reason"])

        subject_density: Dict[str, int] = {}
        injected_rule_ids = set()
        error_counter = 1
        # Track which (USUBJID, column) pairs have already been injected
        # Maps (usubjid, column) -> rule_id to prevent duplicate column injections
        injected_columns: Dict[Tuple[str, str], str] = {}

        for spec in active_rules:
            target_domains = self._resolve_target_domains(spec, datasets)
            if not target_domains:
                manifest.add_skipped_rule(spec.rule_id, "No concrete target domain available")
                continue

            category_rate = float(self.config.get_rate(spec.category) or rate)
            entry = self.catalog.rule_map.get(spec.rule_id, {})
            co_violations = list(entry.get("co_violations", []))

            for domain in target_domains:
                if domain not in datasets:
                    continue

                df = datasets[domain]
                if df.empty:
                    continue

                row_candidates = self._eligible_rows(df, spec.guard_expression)
                if not row_candidates and self._is_row_level_primitive(spec.primitive):
                    continue

                selected_rows = self._select_rows(
                    df=df,
                    row_candidates=row_candidates,
                    rate=category_rate,
                    density_cap=density_cap,
                    subject_density=subject_density,
                    rng=rng,
                    primitive_name=spec.primitive,
                )

                if self._is_row_level_primitive(spec.primitive) and not selected_rows:
                    continue

                if not self._is_row_level_primitive(spec.primitive):
                    selected_rows = [0]

                for row_idx in selected_rows:
                    call_rows = row_idx if self._is_row_level_primitive(spec.primitive) else 0
                    
                    # Get USUBJID for this row to check column conflicts
                    usubjid = self._get_usubjid(df, row_idx)
                    
                    # Extract columns that will be modified by this rule
                    columns_to_inject = self._get_columns_from_params(
                        spec.primitive,
                        spec.params,
                        datasets,
                        domain,
                    )
                    
                    # Check if any of these columns have already been injected for this USUBJID
                    conflicts = self._check_column_conflicts(
                        usubjid,
                        columns_to_inject,
                        injected_columns,
                    )
                    
                    if conflicts:
                        # Skip this injection - column(s) already injected for this subject
                        manifest.add_skipped_rule(
                            spec.rule_id,
                            f"Column conflict for {usubjid}: {conflicts} already injected"
                        )
                        continue
                    
                    records = self._apply_rule_once(
                        datasets=datasets,
                        domain=domain,
                        row_idx=call_rows,
                        spec=spec,
                        rng=rng,
                    )
                    if not records:
                        continue

                    for record in records:
                        if not isinstance(record, MutationRecord):
                            continue
                        record.category = spec.category or record.category
                        record.expected_co_violations = co_violations
                        record.error_id = self._build_error_id(record, error_counter)
                        error_counter += 1
                        manifest.add_error(record)
                        injected_rule_ids.add(spec.rule_id)
                        
                        # Record the injected columns for this USUBJID
                        for col in record.variables_modified.keys():
                            injected_columns[(record.usubjid, col)] = spec.rule_id

        self._rederive_dependent_fields(datasets, manifest)
        warnings = self._self_validate(datasets, manifest)
        manifest.rules_injected = len(injected_rule_ids)
        manifest.total_mutations = len(manifest.errors)
        return manifest, warnings

    def _run_isolated(
        self,
        clean_datasets: Dict[str, pd.DataFrame],
        input_dir: Path,
        output_dir: Path,
        profile: str,
        rules_available: int,
        active_rules: List[RuleSpec],
        skipped_rules: List[Dict[str, str]],
        seed: int,
        rate: float,
        density_cap: int,
        prioritize_flag: bool,
    ) -> InjectionManifest:
        aggregate_manifest = InjectionManifest(
            generated_at=datetime.utcnow().isoformat(),
            seed=seed,
            mode="isolated",
            source_dir=str(input_dir),
            profile=profile,
            rate=rate,
            density_cap=density_cap,
            prioritize_rules=prioritize_flag,
            rules_available=rules_available,
            rules_injectable=len(active_rules),
            rules_skipped=len(skipped_rules),
            rules_injected=0,
            total_mutations=0,
        )

        for row in skipped_rules:
            aggregate_manifest.add_skipped_rule(row["rule_id"], row["reason"])

        for idx, spec in enumerate(active_rules):
            rule_output_dir = output_dir / spec.rule_id
            datasets = {d: df.copy(deep=True) for d, df in clean_datasets.items()}
            manifest, warnings = self._run_compound(
                datasets=datasets,
                input_dir=input_dir,
                profile=profile,
                rules_available=rules_available,
                active_rules=[spec],
                skipped_rules=[],
                seed=seed + idx,
                rate=rate,
                density_cap=density_cap,
                prioritize_flag=False,
            )

            copy_clean_datasets(clean_datasets, rule_output_dir / "clean")
            write_datasets(datasets, rule_output_dir / "dirty")
            write_manifest(manifest, rule_output_dir / "manifest.json")
            write_report(
                manifest,
                warnings=warnings,
                output_path=rule_output_dir / "report.txt",
            )

            if manifest.total_mutations > 0:
                aggregate_manifest.rules_injected += 1
            aggregate_manifest.total_mutations += manifest.total_mutations
            aggregate_manifest.errors.extend(manifest.errors)

        return aggregate_manifest

    def _filter_by_available_domains(
        self,
        specs: List[RuleSpec],
        datasets: Dict[str, pd.DataFrame],
    ) -> Tuple[List[RuleSpec], List[Dict[str, str]]]:
        available = set(datasets.keys())
        active = []
        skipped = []
        for spec in specs:
            targets = self._resolve_target_domains(spec, datasets)
            if targets:
                active.append(spec)
            else:
                skipped.append({"rule_id": spec.rule_id, "reason": "Target domain not available"})
        return active, skipped

    @staticmethod
    def _resolve_target_domains(spec: RuleSpec, datasets: Dict[str, pd.DataFrame]) -> List[str]:
        if spec.domain and spec.domain != "--":
            return [spec.domain.upper()] if spec.domain.upper() in datasets else []

        explicit = [d.upper() for d in (spec.domain_expanded or []) if d and d != "--"]
        if explicit:
            return [d for d in explicit if d in datasets]

        return sorted(list(datasets.keys()))

    @staticmethod
    def _eligible_rows(df: pd.DataFrame, guard_expression: Any) -> List[int]:
        if df.empty:
            return []
        if guard_expression is None:
            return list(range(len(df)))
        eligible = []
        for idx in range(len(df)):
            try:
                if guard_expression.evaluate(df, idx):
                    eligible.append(idx)
            except Exception:
                eligible.append(idx)
        return eligible

    def _select_rows(
        self,
        df: pd.DataFrame,
        row_candidates: List[int],
        rate: float,
        density_cap: int,
        subject_density: Dict[str, int],
        rng: np.random.Generator,
        primitive_name: str,
    ) -> List[int]:
        if not self._is_row_level_primitive(primitive_name):
            return [0]
        if not row_candidates:
            return []

        n_target = max(1, int(round(len(row_candidates) * rate)))
        n_target = min(n_target, int(self.config.defaults.get("max_errors_per_rule", 50)), len(row_candidates))

        shuffled = list(row_candidates)
        rng.shuffle(shuffled)
        selected: List[int] = []

        for row_idx in shuffled:
            sid = self._get_subject_id(df, row_idx)
            if sid and density_cap > 0 and subject_density.get(sid, 0) >= density_cap:
                continue
            selected.append(row_idx)
            if sid:
                subject_density[sid] = subject_density.get(sid, 0) + 1
            if len(selected) >= n_target:
                break

        return selected

    def _apply_rule_once(
        self,
        datasets: Dict[str, pd.DataFrame],
        domain: str,
        row_idx: int,
        spec: RuleSpec,
        rng: np.random.Generator,
    ) -> List[MutationRecord]:
        if not hasattr(primitives, spec.primitive):
            return []

        func = getattr(primitives, spec.primitive)
        params = self._normalize_params(spec.primitive, dict(spec.params), datasets, domain, row_idx)

        try:
            kwargs = self._build_call_kwargs(
                func=func,
                datasets=datasets,
                domain=domain,
                row_idx=row_idx,
                rule_id=spec.rule_id,
                params=params,
                rng=rng,
            )
            result = func(**kwargs)
        except Exception:
            return []

        if isinstance(result, list):
            return [r for r in result if isinstance(r, MutationRecord)]
        if isinstance(result, MutationRecord):
            return [result]
        return []

    @staticmethod
    def _resolve_generic_param(raw_value: Any, domain: str, columns: List[str]) -> Any:
        if not isinstance(raw_value, str) or not raw_value.startswith("--"):
            return raw_value
        return primitives._resolve_prefix(raw_value, domain, columns) or raw_value

    def _normalize_params(
        self,
        primitive_name: str,
        params: Dict[str, Any],
        datasets: Dict[str, pd.DataFrame],
        domain: str,
        row_idx: int,
    ) -> Dict[str, Any]:
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
            source_domain = str(normalized.get("source_domain", domain)).upper()
            if "usubjid" not in normalized and key_field and source_domain in datasets:
                src_df = datasets[source_domain]
                if key_field in src_df.columns and len(src_df.index) > row_idx:
                    normalized["usubjid"] = src_df.iloc[row_idx][key_field]

        primary_domain = domain
        if primitive_name in {"cross_domain_mismatch", "cross_domain_orphan"} and "source_domain" in normalized:
            primary_domain = str(normalized["source_domain"]).upper()

        primary_df = datasets.get(primary_domain)
        if primary_df is not None:
            for key in ["field", "start_field", "end_field", "date_field", "dy_field", "column", "col_a", "col_b"]:
                if key in normalized:
                    normalized[key] = self._resolve_generic_param(normalized[key], primary_domain, list(primary_df.columns))

            if "source_field" in normalized:
                src = normalized["source_field"]
                if isinstance(src, str) and "," in src:
                    normalized["source_field"] = ",".join(
                        self._resolve_generic_param(part.strip(), primary_domain, list(primary_df.columns))
                        for part in src.split(",")
                    )
                else:
                    normalized["source_field"] = self._resolve_generic_param(src, primary_domain, list(primary_df.columns))

        target_domain = normalized.get("target_domain")
        target_df = datasets.get(str(target_domain).upper()) if target_domain else None
        if target_df is not None and "target_field" in normalized:
            tgt = normalized["target_field"]
            if isinstance(tgt, str) and "," in tgt:
                normalized["target_field"] = ",".join(
                    self._resolve_generic_param(part.strip(), str(target_domain).upper(), list(target_df.columns))
                    for part in tgt.split(",")
                )
            else:
                normalized["target_field"] = self._resolve_generic_param(
                    tgt,
                    str(target_domain).upper(),
                    list(target_df.columns),
                )

        return normalized

    @staticmethod
    def _build_call_kwargs(
        func: Any,
        datasets: Dict[str, pd.DataFrame],
        domain: str,
        row_idx: int,
        rule_id: str,
        params: Dict[str, Any],
        rng: np.random.Generator,
    ) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        sig = inspect.signature(func)

        for name, param in sig.parameters.items():
            if name == "df":
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
                raise ValueError(f"Missing required parameter '{name}'")

        return kwargs

    @staticmethod
    def _build_error_id(record: MutationRecord, counter: int) -> str:
        domain = (record.domain or "GEN").upper()
        return f"INJ-{record.rule_id}-{domain}-{counter:04d}"

    @staticmethod
    def _get_subject_id(df: pd.DataFrame, row_idx: int) -> str:
        if "USUBJID" in df.columns and row_idx < len(df):
            return str(df.iloc[row_idx]["USUBJID"])
        if len(df.columns) > 0 and row_idx < len(df):
            return str(df.iloc[row_idx][df.columns[0]])
        return ""

    @staticmethod
    def _is_row_level_primitive(primitive_name: str) -> bool:
        non_row_level = {
            "drop_domain",
            "add_column",
            "drop_column",
            "reorder_columns",
            "delete_row",
            "cross_domain_orphan",
        }
        return primitive_name not in non_row_level

    @staticmethod
    def _get_usubjid(df: pd.DataFrame, row_idx: int) -> str:
        """Extract USUBJID from row for tracking purposes."""
        if row_idx < 0 or row_idx >= len(df):
            return ""
        if "USUBJID" in df.columns:
            return str(df.iloc[row_idx]["USUBJID"])
        return ""

    @staticmethod
    def _get_columns_from_params(
        primitive_name: str,
        params: Dict[str, Any],
        datasets: Dict[str, pd.DataFrame],
        domain: str,
    ) -> List[str]:
        """
        Extract column names from rule parameters that will be modified by the primitive.
        
        Args:
            primitive_name: Name of the primitive (e.g., "blank_field")
            params: Parameter dict from RuleSpec
            datasets: Available datasets
            domain: Target domain
            
        Returns:
            List of column names that will be modified
        """
        columns = []
        
        # These parameters directly specify columns to modify
        column_params = ["field", "column_name", "source_field", "target_field"]
        for param in column_params:
            if param in params:
                col = str(params[param])
                if col and col not in {"--", ""}:
                    columns.append(col)
        
        # Handle range-based primitives that modify two columns
        for start_col_param, end_col_param in [
            ("start_field", "end_field"),
            ("date_field", "dy_field"),
            ("col_a", "col_b"),
        ]:
            if start_col_param in params:
                col = str(params[start_col_param])
                if col and col not in {"--", ""}:
                    columns.append(col)
            if end_col_param in params:
                col = str(params[end_col_param])
                if col and col not in {"--", ""}:
                    columns.append(col)
        
        # Handle key_fields for duplicate_record
        if "key_fields" in params:
            key_fields = params["key_fields"]
            if isinstance(key_fields, list):
                columns.extend([str(k) for k in key_fields if k])
            elif isinstance(key_fields, str):
                columns.extend([k.strip() for k in key_fields.split(",") if k.strip()])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_columns = []
        for col in columns:
            if col not in seen:
                seen.add(col)
                unique_columns.append(col)
        
        return unique_columns

    @staticmethod
    def _check_column_conflicts(
        usubjid: str,
        columns_to_inject: List[str],
        injected_columns: Dict[Tuple[str, str], str],
    ) -> List[str]:
        """
        Check if any of the columns to be injected have already been injected for this USUBJID.
        
        Args:
            usubjid: Subject ID
            columns_to_inject: List of column names to be modified
            injected_columns: Tracking dict mapping (usubjid, column) -> rule_id
            
        Returns:
            List of conflicting columns (empty if no conflicts)
        """
        conflicts = []
        for col in columns_to_inject:
            if (usubjid, col) in injected_columns:
                conflicts.append(f"{col}(by {injected_columns[(usubjid, col)]})")
        return conflicts

    def _rederive_dependent_fields(
        self,
        datasets: Dict[str, pd.DataFrame],
        manifest: InjectionManifest,
    ) -> None:
        """Re-derive DY-like fields from matching DTC-like fields using DM.RFSTDTC."""
        dm = datasets.get("DM")
        if dm is None or dm.empty or "USUBJID" not in dm.columns or "RFSTDTC" not in dm.columns:
            return

        rfstdtc_by_subject = {
            str(row["USUBJID"]): str(row["RFSTDTC"])
            for _, row in dm[["USUBJID", "RFSTDTC"]].iterrows()
        }

        row_to_record: Dict[Tuple[str, int], MutationRecord] = {}
        for record in manifest.errors:
            row_to_record[(record.domain, int(record.row_index))] = record

        for domain, df in datasets.items():
            if df.empty or "USUBJID" not in df.columns:
                continue

            dy_columns = [c for c in df.columns if c.endswith("DY")]
            for row_idx in range(len(df)):
                usubjid = str(df.iloc[row_idx]["USUBJID"])
                rfstdtc = rfstdtc_by_subject.get(usubjid, "")
                if not rfstdtc:
                    continue

                for dy_col in dy_columns:
                    dtc_col = dy_col[:-2] + "DTC"
                    if dtc_col not in df.columns:
                        continue

                    dtc_value = str(df.iloc[row_idx][dtc_col])
                    new_day = primitives._derive_study_day(dtc_value, rfstdtc)
                    new_value = "" if new_day is None else str(new_day)
                    old_value = str(df.iloc[row_idx][dy_col])

                    if old_value == new_value:
                        continue

                    df.loc[df.index[row_idx], dy_col] = new_value

                    key = (domain, row_idx)
                    if key in row_to_record:
                        row_to_record[key].re_derived[dy_col] = {
                            "original": old_value,
                            "new": new_value,
                        }

    def _self_validate(
        self,
        datasets: Dict[str, pd.DataFrame],
        manifest: InjectionManifest,
    ) -> List[str]:
        """Run lightweight consistency checks after injection."""
        warnings: List[str] = []

        if not manifest.errors:
            warnings.append("No mutations were recorded in manifest")
            return warnings

        for record in manifest.errors:
            domain = record.domain
            df = datasets.get(domain)

            if df is None and not any(v.get("injected") == "ABSENT" for v in record.variables_modified.values()):
                warnings.append(f"{record.error_id}: domain {domain} not present after mutation")
                continue

            if df is not None and record.row_index >= 0 and record.row_index >= len(df):
                warnings.append(f"{record.error_id}: row_index {record.row_index} out of bounds for {domain}")

            for field, delta in record.variables_modified.items():
                injected = str(delta.get("injected", ""))
                if injected in {"DELETED", "ABSENT", "PRESENT", "DUPLICATE"}:
                    continue
                if df is None or record.row_index < 0 or field not in df.columns or record.row_index >= len(df):
                    continue

                current = str(df.iloc[record.row_index][field])
                if current != injected:
                    warnings.append(
                        f"{record.error_id}: expected {domain}.{field}='{injected}', found '{current}'"
                    )

        return warnings
