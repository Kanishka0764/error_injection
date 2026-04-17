"""
16 Mutation Primitives for SDTM error injection.
Data-driven, reusable functions that transform datasets.
Each primitive returns a MutationRecord documenting the change.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

from injection.manifest import MutationRecord


@dataclass
class GuardExpression:
    """Represents a guard condition for rule eligibility."""

    expression: str  # e.g., "ARMCD in (SCRNFAIL,NOTASSGN,)"

    def evaluate(self, df, row_idx: int) -> bool:
        """
        Evaluate guard expression for a specific row.

        Args:
            df: Pandas DataFrame
            row_idx: Row index to evaluate against

        Returns:
            True if row is eligible, False otherwise
        """
        if not self.expression or self.expression.strip() == "":
            return True  # No guard = all rows eligible

        # Parse simple guard expressions
        # Format: FIELD in (VAL1,VAL2), FIELD == VALUE, FIELD != VALUE, etc.
        parts = self.expression.split()

        if len(parts) < 2:
            return True

        field = parts[0]
        operator = parts[1] if len(parts) > 1 else None

        if field not in df.columns:
            return True  # Field doesn't exist = skip

        value = df[field].iloc[row_idx]

        if operator == "in":
            # Extract values from parentheses: in (A,B,C)
            rest = " ".join(parts[2:])
            if not rest.startswith("(") or not rest.endswith(")"):
                return True
            values_str = rest[1:-1]  # Remove parens
            valid_values = [v.strip() for v in values_str.split(",")]
            return value in valid_values

        elif operator == "not":
            if len(parts) >= 3 and parts[2] == "in":
                # not in (A,B,C)
                rest = " ".join(parts[3:])
                if not rest.startswith("(") or not rest.endswith(")"):
                    return True
                values_str = rest[1:-1]
                valid_values = [v.strip() for v in values_str.split(",")]
                return value not in valid_values
            elif len(parts) >= 2 and parts[2] == "":
                # Field != "" (not blank)
                return value != ""

        elif operator == "==":
            expected = " ".join(parts[2:])
            return value == expected

        elif operator == "!=":
            if len(parts) >= 3:
                expected = " ".join(parts[2:])
                return value != expected
            else:
                # != with no value means not blank
                return value != ""

        return True


def _resolve_prefix(generic_var: str, domain: str, columns: List[str]) -> Optional[str]:
    """
    Resolve generic prefix variable to domain-specific variable.

    Args:
        generic_var: e.g., "--STDTC", "--DY"
        domain: Domain name, e.g., "AE", "DM"
        columns: List of column names in domain

    Returns:
        Resolved variable name, or None if not found

    Examples:
        "--STDTC" + "AE" → "AESTDTC"
        "--DY" + "LB" → "LBDY"
        "--STDTC" + "DM" → "RFSTDTC" (special case)
    """
    if not generic_var.startswith("--"):
        return generic_var

    suffix = generic_var[2:]  # Remove "--"

    # Special cases for DM domain
    if domain == "DM":
        if suffix == "STDTC":
            return "RFSTDTC"
        elif suffix == "ENDTC":
            return "RFENDTC"

    # Normal case: prefix = domain + suffix
    resolved = domain + suffix
    if resolved in columns:
        return resolved

    return None


def _parse_iso_date(date_str: str) -> Optional[datetime]:
    """Parse ISO date string (YYYY-MM-DD or YYYY-MM)."""
    if not date_str or not isinstance(date_str, str):
        return None
    try:
        if len(date_str) == 10:  # YYYY-MM-DD
            return datetime.strptime(date_str, "%Y-%m-%d")
        elif len(date_str) == 7:  # YYYY-MM
            return datetime.strptime(date_str, "%Y-%m")
        else:
            return None
    except ValueError:
        return None


def _format_iso_date(dt: datetime, partial: bool = False) -> str:
    """Format datetime to ISO date string."""
    if partial:
        return dt.strftime("%Y-%m")
    return dt.strftime("%Y-%m-%d")


def _derive_study_day(dtc_str: str, rfstdtc_str: str) -> Optional[int]:
    """
    Derive study day from DTC and RFSTDTC.

    Returns:
        Study day as integer, or None if can't derive
    """
    dtc = _parse_iso_date(dtc_str)
    rfstdtc = _parse_iso_date(rfstdtc_str)

    if not dtc or not rfstdtc:
        return None

    delta = dtc - rfstdtc
    return delta.days + 1


# ============================================================================
# 16 MUTATION PRIMITIVES
# ============================================================================


def blank_field(
    df: pd.DataFrame,
    row_idx: int,
    field: str,
    rule_id: str,
) -> MutationRecord:
    """
    Clear a required field (set to empty string).

    Args:
        df: DataFrame to mutate (modified in-place)
        row_idx: Row index to blank
        field: Column name
        rule_id: Rule ID for manifest

    Returns:
        MutationRecord documenting the change
    """
    original = str(df[field].iloc[row_idx]) if field in df.columns else ""
    
    if field in df.columns:
        df.loc[df.index[row_idx], field] = ""

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Field {field} is blank",
        category="",
        primitive="blank_field",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={field: {"original": original, "injected": ""}},
    )


def populate_forbidden(
    df: pd.DataFrame,
    row_idx: int,
    field: str,
    value: str,
    rule_id: str,
) -> MutationRecord:
    """
    Set a field that should be blank/absent to a forbidden value.

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        field: Column name
        value: Value to populate with
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    original = str(df[field].iloc[row_idx]) if field in df.columns else ""

    if field not in df.columns:
        df[field] = ""

    df.loc[df.index[row_idx], field] = value

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Field {field} populated with forbidden value",
        category="",
        primitive="populate_forbidden",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={field: {"original": original, "injected": value}},
    )


def set_invalid_value(
    df: pd.DataFrame,
    row_idx: int,
    field: str,
    value: str,
    rule_id: str,
) -> MutationRecord:
    """
    Set field to forbidden/invalid value (0, -1, non-ASCII, oversized, etc.).

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        field: Column name
        value: Invalid value to set
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    original = str(df[field].iloc[row_idx]) if field in df.columns else ""

    if field not in df.columns:
        df[field] = ""

    df.loc[df.index[row_idx], field] = value

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Field {field} set to invalid value",
        category="",
        primitive="set_invalid_value",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={field: {"original": original, "injected": value}},
    )


def mismatch_pair(
    df: pd.DataFrame,
    row_idx: int,
    field_a: str,
    field_b: str,
    rule_id: str,
    rng: np.random.Generator,
) -> MutationRecord:
    """
    Break paired field relationship. E.g., ARMCD/ARM must match.

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        field_a: Primary field (read value)
        field_b: Secondary field (corrupt value)
        rule_id: Rule ID
        rng: Random number generator

    Returns:
        MutationRecord
    """
    original_a = str(df[field_a].iloc[row_idx]) if field_a in df.columns else ""
    original_b = str(df[field_b].iloc[row_idx]) if field_b in df.columns else ""

    # Set field_b to a mismatched value
    mismatched = original_a + "_MISMATCH" if original_a else "INVALID"

    if field_b not in df.columns:
        df[field_b] = ""

    df.loc[df.index[row_idx], field_b] = mismatched

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Fields {field_a}/{field_b} are mismatched",
        category="",
        primitive="mismatch_pair",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={
            field_a: {"original": original_a, "injected": original_a},
            field_b: {"original": original_b, "injected": mismatched},
        },
    )


def invert_date_order(
    df: pd.DataFrame,
    row_idx: int,
    start_field: str,
    end_field: str,
    rule_id: str,
    rng: np.random.Generator,
) -> MutationRecord:
    """
    Make start date > end date (or numeric range inversion).

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        start_field: Start field name
        end_field: End field name
        rule_id: Rule ID
        rng: Random number generator

    Returns:
        MutationRecord
    """
    original_start = str(df[start_field].iloc[row_idx]) if start_field in df.columns else ""
    original_end = str(df[end_field].iloc[row_idx]) if end_field in df.columns else ""

    # Try to parse as dates
    end_date = _parse_iso_date(original_end)
    if end_date:
        # Set start to end + random days (1-30)
        offset_days = rng.integers(1, 31)
        new_start = end_date + timedelta(days=offset_days)
        injected_start = _format_iso_date(new_start)
    else:
        # Try numeric inversion
        try:
            end_num = float(original_end)
            injected_start = str(end_num + rng.integers(1, 10))
        except (ValueError, TypeError):
            injected_start = original_start

    if start_field in df.columns:
        df.loc[df.index[row_idx], start_field] = injected_start

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Start {start_field} is after end {end_field}",
        category="",
        primitive="invert_date_order",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={
            start_field: {"original": original_start, "injected": injected_start}
        },
    )


def wrong_derived(
    df: pd.DataFrame,
    row_idx: int,
    field: str,
    offset_range: Tuple[int, int],
    rule_id: str,
    rng: np.random.Generator,
) -> MutationRecord:
    """
    Offset a derived numeric value by ±N.

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        field: Column name (e.g., "--DY")
        offset_range: Tuple of (min_offset, max_offset)
        rule_id: Rule ID
        rng: Random number generator

    Returns:
        MutationRecord
    """
    original = str(df[field].iloc[row_idx]) if field in df.columns else "0"

    try:
        original_val = int(original)
    except (ValueError, TypeError):
        original_val = 0

    offset = rng.integers(offset_range[0], offset_range[1] + 1)
    if offset == 0:
        offset = 1  # Don't set to same value
    
    injected_val = original_val + offset
    injected = str(injected_val)

    if field in df.columns:
        df.loc[df.index[row_idx], field] = injected

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Derived field {field} is incorrect",
        category="",
        primitive="wrong_derived",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={field: {"original": original, "injected": injected}},
    )


def truncate_with_derived(
    df: pd.DataFrame,
    row_idx: int,
    date_field: str,
    dy_field: str,
    rule_id: str,
    rng: np.random.Generator,
) -> MutationRecord:
    """
    Truncate date to partial (YYYY-MM) but leave DY populated.
    Inconsistency: DY can't be derived from partial date.

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        date_field: Date field to truncate
        dy_field: Derived day field (stays populated)
        rule_id: Rule ID
        rng: Random number generator

    Returns:
        MutationRecord
    """
    original_date = str(df[date_field].iloc[row_idx]) if date_field in df.columns else ""
    original_dy = str(df[dy_field].iloc[row_idx]) if dy_field in df.columns else ""

    # Truncate date to YYYY-MM
    if len(original_date) >= 7:
        injected_date = original_date[:7]  # YYYY-MM
    else:
        injected_date = original_date

    if date_field in df.columns:
        df.loc[df.index[row_idx], date_field] = injected_date

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Date {date_field} is partial but {dy_field} is populated",
        category="",
        primitive="truncate_with_derived",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={
            date_field: {"original": original_date, "injected": injected_date}
        },
    )


def drop_column(
    df: pd.DataFrame,
    column: str,
    rule_id: str,
) -> MutationRecord:
    """
    Remove a required column from domain DataFrame.

    Args:
        df: DataFrame to mutate
        column: Column name to drop
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    if column in df.columns:
        df.drop(columns=[column], inplace=True)

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Required variable {column} is missing",
        category="",
        primitive="drop_column",
        domain=df.attrs.get("domain", ""),
        usubjid="",
        row_index=-1,
        variables_modified={column: {"original": "PRESENT", "injected": "ABSENT"}},
    )


def drop_domain(
    datasets: Dict[str, pd.DataFrame],
    domain: str,
    rule_id: str,
) -> MutationRecord:
    """
    Remove entire domain DataFrame.

    Args:
        datasets: Dict of DataFrames
        domain: Domain to drop
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    if domain in datasets:
        del datasets[domain]

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Required domain {domain} is missing",
        category="",
        primitive="drop_domain",
        domain=domain,
        usubjid="",
        row_index=-1,
        variables_modified={domain: {"original": "PRESENT", "injected": "ABSENT"}},
    )


def duplicate_record(
    df: pd.DataFrame,
    row_idx: int,
    key_fields: List[str],
    rule_id: str,
    filter_field: Optional[str] = None,
    filter_value: Optional[str] = None,
) -> MutationRecord:
    """
    Duplicate a row to create key violation.

    Args:
        df: DataFrame to mutate
        row_idx: Row to duplicate
        key_fields: Field names that form the primary key
        rule_id: Rule ID
        filter_field: Optional field to filter (for TS rules)
        filter_value: Optional value to filter by

    Returns:
        MutationRecord
    """
    # Find a row that matches filter (if provided)
    if filter_field and filter_field in df.columns:
        matching = df[df[filter_field] == filter_value]
        if not matching.empty:
            row_idx = matching.index[0]

    # Duplicate the row
    row = df.iloc[row_idx].copy()
    new_row_idx = len(df)
    df.loc[new_row_idx] = row

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Duplicate record with key {key_fields}",
        category="",
        primitive="duplicate_record",
        domain=df.attrs.get("domain", ""),
        usubjid=row.get("USUBJID", "") if "USUBJID" in row else "",
        row_index=row_idx,
        variables_modified={str(k): {"original": "UNIQUE", "injected": "DUPLICATE"} for k in key_fields},
    )


def delete_row(
    df: pd.DataFrame,
    filter_field: Optional[str],
    filter_value: Optional[str],
    rule_id: str,
) -> List[MutationRecord]:
    """
    Delete row(s) matching filter criteria.
    Primary use: TS 'Missing PARMCD' rules.

    Args:
        df: DataFrame to mutate
        filter_field: Field to filter on (e.g., "TSPARMCD")
        filter_value: Value to match (e.g., "ADDON")
        rule_id: Rule ID

    Returns:
        List of MutationRecords (one per deleted row)
    """
    records = []

    if filter_field and filter_field in df.columns:
        # Delete rows matching filter
        matching = df[df[filter_field] == filter_value]
        for idx in matching.index:
            deleted_row = df.loc[idx].to_dict()
            records.append(
                MutationRecord(
                    error_id="",
                    rule_id=rule_id,
                    rule_message=f"Row with {filter_field}={filter_value} deleted",
                    category="",
                    primitive="delete_row",
                    domain=df.attrs.get("domain", ""),
                    usubjid=deleted_row.get("USUBJID", ""),
                    row_index=idx,
                    variables_modified={k: {"original": str(v), "injected": "DELETED"} for k, v in deleted_row.items()},
                )
            )
        df.drop(matching.index, inplace=True)
    else:
        # No filter = delete all rows
        for idx in df.index:
            records.append(
                MutationRecord(
                    error_id="",
                    rule_id=rule_id,
                    rule_message="All rows deleted",
                    category="",
                    primitive="delete_row",
                    domain=df.attrs.get("domain", ""),
                    usubjid="",
                    row_index=idx,
                    variables_modified={"ALL": {"original": "PRESENT", "injected": "DELETED"}},
                )
            )
        df.drop(df.index, inplace=True)

    return records


def add_column(
    df: pd.DataFrame,
    column_name: str,
    fill_value: str,
    rule_id: str,
) -> MutationRecord:
    """
    Add a column that shouldn't exist in the domain.

    Args:
        df: DataFrame to mutate
        column_name: Name of column to add
        fill_value: Value to fill column with
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    if column_name not in df.columns:
        df[column_name] = fill_value

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Unexpected variable {column_name} present",
        category="",
        primitive="add_column",
        domain=df.attrs.get("domain", ""),
        usubjid="",
        row_index=-1,
        variables_modified={column_name: {"original": "ABSENT", "injected": "PRESENT"}},
    )


def reorder_columns(
    df: pd.DataFrame,
    col_a: str,
    col_b: str,
    rule_id: str,
) -> MutationRecord:
    """
    Swap two column positions to create wrong-order violation.

    Args:
        df: DataFrame to mutate
        col_a: First column name
        col_b: Second column name
        rule_id: Rule ID

    Returns:
        MutationRecord
    """
    cols = list(df.columns)
    if col_a in cols and col_b in cols:
        idx_a = cols.index(col_a)
        idx_b = cols.index(col_b)
        cols[idx_a], cols[idx_b] = cols[idx_b], cols[idx_a]
        df.reindex(columns=cols, copy=False)  # Reorder columns

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"Variables {col_a} and {col_b} are in wrong order",
        category="",
        primitive="reorder_columns",
        domain=df.attrs.get("domain", ""),
        usubjid="",
        row_index=-1,
        variables_modified={col_a: {"original": f"pos{idx_a}", "injected": f"pos{idx_b}"},
                           col_b: {"original": f"pos{idx_b}", "injected": f"pos{idx_a}"}},
    )


def cross_domain_mismatch(
    datasets: Dict[str, pd.DataFrame],
    source_domain: str,
    source_row_idx: int,
    source_field: str,
    target_domain: str,
    target_field: str,
    rule_id: str,
    mismatch_type: str = "not_in_set",
    rng: Optional[np.random.Generator] = None,
) -> MutationRecord:
    """
    Create cross-domain mismatch. Value in domain A conflicts with domain B.

    Args:
        datasets: Dict of DataFrames
        source_domain: Source domain
        source_row_idx: Row index in source domain
        source_field: Field to corrupt
        target_domain: Target domain for lookup
        target_field: Field to search in target
        rule_id: Rule ID
        mismatch_type: Type of mismatch ('not_in_set', 'not_equal', etc.)
        rng: Random number generator

    Returns:
        MutationRecord
    """
    if rng is None:
        rng = np.random.default_rng()

    source_df = datasets.get(source_domain)
    target_df = datasets.get(target_domain)

    if source_df is None or target_df is None:
        return MutationRecord(
            error_id="",
            rule_id=rule_id,
            rule_message="Cross-domain mismatch (domain not available)",
            category="",
            primitive="cross_domain_mismatch",
            domain=source_domain,
            usubjid="",
            row_index=source_row_idx,
        )

    original = str(source_df[source_field].iloc[source_row_idx]) if source_field in source_df.columns else ""

    # Set to invalid value not in target
    if target_field in target_df.columns:
        valid_values = set(target_df[target_field].dropna().astype(str).unique())
        # Pick a value NOT in valid set
        injected = original + "_INVALID" if original else "UNKNOWN"
    else:
        injected = "INVALID"

    if source_field in source_df.columns:
        source_df.loc[source_df.index[source_row_idx], source_field] = injected

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"{source_field} value not in {target_domain}.{target_field}",
        category="",
        primitive="cross_domain_mismatch",
        domain=source_domain,
        usubjid=source_df[source_df.columns[0]].iloc[source_row_idx] if len(source_df.columns) > 0 else "",
        row_index=source_row_idx,
        variables_modified={source_field: {"original": original, "injected": injected}},
    )


def cross_domain_orphan(
    datasets: Dict[str, pd.DataFrame],
    source_domain: str,
    target_domain: str,
    key_field: str,
    usubjid: str,
    rule_id: str,
) -> List[MutationRecord]:
    """
    Remove all records in target domain for a subject.

    Args:
        datasets: Dict of DataFrames
        source_domain: Source domain (e.g., "DM")
        target_domain: Target domain to delete from (e.g., "DS")
        key_field: Key field name (e.g., "USUBJID")
        usubjid: Subject ID value
        rule_id: Rule ID

    Returns:
        List of MutationRecords (one per deleted row)
    """
    records = []
    target_df = datasets.get(target_domain)

    if target_df is None:
        return records

    if key_field in target_df.columns:
        matching = target_df[target_df[key_field] == usubjid]
        for idx in matching.index:
            records.append(
                MutationRecord(
                    error_id="",
                    rule_id=rule_id,
                    rule_message=f"No {target_domain} records for subject {usubjid}",
                    category="",
                    primitive="cross_domain_orphan",
                    domain=target_domain,
                    usubjid=usubjid,
                    row_index=idx,
                    variables_modified={key_field: {"original": usubjid, "injected": "DELETED"}},
                )
            )
        target_df.drop(matching.index, inplace=True)

    return records


def invalid_codelist(
    df: pd.DataFrame,
    row_idx: int,
    field: str,
    valid_values: List[str],
    rule_id: str,
    rng: np.random.Generator,
) -> MutationRecord:
    """
    Use value not in controlled terminology list.

    Args:
        df: DataFrame to mutate
        row_idx: Row index
        field: Column name
        valid_values: List of valid values
        rule_id: Rule ID
        rng: Random number generator

    Returns:
        MutationRecord
    """
    original = str(df[field].iloc[row_idx]) if field in df.columns else ""

    # Pick an invalid value not in codelist
    injected = "INVALID_" + str(rng.integers(1000, 9999))

    if field in df.columns:
        df.loc[df.index[row_idx], field] = injected

    return MutationRecord(
        error_id="",
        rule_id=rule_id,
        rule_message=f"{field} value not in controlled terminology",
        category="",
        primitive="invalid_codelist",
        domain=df.attrs.get("domain", ""),
        usubjid=df[df.columns[0]].iloc[row_idx] if len(df.columns) > 0 else "",
        row_index=row_idx,
        variables_modified={field: {"original": original, "injected": injected}},
    )
