"""
Configuration loader for injection pipeline.
Loads defaults.yaml, resolves profiles, and manages rule selection.
"""

from pathlib import Path
from typing import Optional, List, Dict, Any
import yaml


class Config:
    """Load and manage injection configuration."""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize config from YAML file.

        Args:
            config_path: Path to defaults.yaml. If None, uses default location.
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "injection" / "defaults.yaml"

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path) as f:
            self.data = yaml.safe_load(f)

        self.profiles = self.data.get("profiles", {})
        self.defaults = self.data.get("defaults", {})
        self.category_rates = self.data.get("category_rates", {})
        self.skip_by_default = self.data.get("skip_by_default", [])

    def resolve_profile(self, profile_name: str) -> List[str]:
        """
        Resolve profile name to list of categories.

        Args:
            profile_name: Profile name (e.g., 'all', 'dates', 'dm')

        Returns:
            List of category names

        Raises:
            ValueError: If profile not found
        """
        if profile_name not in self.profiles:
            raise ValueError(
                f"Unknown profile: {profile_name}. Available: {list(self.profiles.keys())}"
            )
        return self.profiles[profile_name].get("categories", [])

    def get_rate(self, category: Optional[str] = None) -> float:
        """
        Get injection rate for a category or default.

        Args:
            category: Category name. If None, returns default rate.

        Returns:
            Injection rate (0.0 to 1.0)
        """
        if category:
            return self.category_rates.get(category, self.defaults.get("rate", 0.05))
        return self.defaults.get("rate", 0.05)

    def resolve_rules(
        self,
        profile: Optional[str] = None,
        categories: Optional[List[str]] = None,
        rules: Optional[List[str]] = None,
        exclude_rules: Optional[List[str]] = None,
    ) -> tuple[List[str], List[str]]:
        """
        Resolve rule list from profile → categories → explicit rules.

        Args:
            profile: Profile name ('all', 'dates', etc.)
            categories: Override categories from profile
            rules: Explicit rule IDs to include
            exclude_rules: Rule IDs to exclude

        Returns:
            Tuple of (selected_rules, excluded_rules)
        """
        profile = profile or "all"
        categories = categories or self.resolve_profile(profile)

        # For now, these are placeholders - actual categories→rules mapping
        # is in catalog.py
        selected = rules or []
        excluded = exclude_rules or self.skip_by_default

        return selected, excluded

    def to_dict(self) -> Dict[str, Any]:
        """Export config as dictionary."""
        return {
            "profiles": self.profiles,
            "defaults": self.defaults,
            "category_rates": self.category_rates,
            "skip_by_default": self.skip_by_default,
        }
