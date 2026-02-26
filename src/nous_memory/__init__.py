"""nous-memory package."""

__version__ = "0.1.0"

from .cli import main
from .core import resolve_db_path

__all__ = ["__version__", "main", "resolve_db_path"]
