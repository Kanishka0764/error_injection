"""
CSV reader for SDTM data.
Loads datasets as Dict[domain_name, DataFrame] with dtype=str to preserve values.
"""

from pathlib import Path
from typing import Dict, Optional
import pandas as pd


def load_datasets(input_dir: Path) -> Dict[str, pd.DataFrame]:
    """
    Load all CSV files from input directory as SDTM domains.

    Key behaviors:
    - dtype=str: All values read as strings (no auto-NA conversion)
    - keep_default_na=False: Don't convert "", "NA", etc. to NaN
    - Strips leading/trailing whitespace from column names
    - Domain = filename without extension (e.g., DM, AE, VS)

    Args:
        input_dir: Directory containing CSV files

    Returns:
        Dict mapping domain name to DataFrame

    Raises:
        FileNotFoundError: If directory not found
        ValueError: If no CSV files found
    """
    input_dir = Path(input_dir)
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    datasets = {}
    csv_files = list(input_dir.glob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in {input_dir}")

    for csv_file in csv_files:
        domain = csv_file.stem.upper()  # Remove extension, uppercase
        try:
            df = pd.read_csv(
                csv_file,
                dtype=str,
                keep_default_na=False,
                na_values=[""],  # Only treat empty string as NaN for now
                skipinitialspace=True,
            )
            # Strip whitespace from column names
            df.columns = df.columns.str.strip()
            datasets[domain] = df
        except Exception as e:
            raise ValueError(f"Failed to read {csv_file}: {e}") from e

    return datasets


def load_dataset(csv_path: Path) -> pd.DataFrame:
    """
    Load a single CSV file.

    Args:
        csv_path: Path to CSV file

    Returns:
        DataFrame with dtype=str
    """
    df = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_values=[""],
        skipinitialspace=True,
    )
    df.columns = df.columns.str.strip()
    return df


def validate_domains(datasets: Dict[str, pd.DataFrame], expected_domains: Optional[list] = None) -> Dict[str, bool]:
    """
    Validate loaded datasets.

    Args:
        datasets: Dict of DataFrames
        expected_domains: Optional list of required domains

    Returns:
        Dict mapping domain → bool (is_present)
    """
    if not datasets:
        raise ValueError("No datasets loaded")

    status = {}
    if expected_domains:
        for domain in expected_domains:
            status[domain] = domain in datasets
    else:
        for domain in datasets:
            status[domain] = True

    return status
