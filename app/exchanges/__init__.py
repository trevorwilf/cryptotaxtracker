"""
Exchange plugin system.

Each exchange plugin subclasses BaseExchange and registers itself via the
@register decorator. The main app uses get_exchange() to instantiate a
configured plugin by name.
"""
import importlib
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime

logger = logging.getLogger("tax-collector.exchanges")

# Registry of exchange classes, keyed by name
_registry: dict[str, type] = {}


class BaseExchange(ABC):
    """Base class all exchange plugins must inherit from."""

    name: str = ""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    @abstractmethod
    async def fetch_trades(self, since: datetime | None = None) -> list[dict]:
        ...

    @abstractmethod
    async def fetch_orders(self, since: datetime | None = None) -> list[dict]:
        ...

    @abstractmethod
    async def fetch_deposits(self, since: datetime | None = None) -> list[dict]:
        ...

    @abstractmethod
    async def fetch_withdrawals(self, since: datetime | None = None) -> list[dict]:
        ...

    @abstractmethod
    async def fetch_pool_activity(self, since: datetime | None = None) -> list[dict]:
        ...


def register(cls):
    """Decorator that registers an exchange plugin class."""
    if not issubclass(cls, BaseExchange):
        raise TypeError(f"{cls.__name__} must subclass BaseExchange")
    _registry[cls.name] = cls
    logger.debug(f"Registered exchange plugin: {cls.name}")
    return cls


def get_exchange(name: str, settings) -> BaseExchange | None:
    """Instantiate a registered exchange plugin with credentials from settings."""
    # Auto-import exchange modules to trigger @register
    _auto_import()

    cls = _registry.get(name)
    if cls is None:
        return None

    key_attr = f"{name}_api_key"
    secret_attr = f"{name}_api_secret"
    api_key = getattr(settings, key_attr, "") or ""
    api_secret = getattr(settings, secret_attr, "") or ""

    return cls(api_key=api_key, api_secret=api_secret)


def list_exchanges() -> list[str]:
    """Return names of all registered exchange plugins."""
    _auto_import()
    return list(_registry.keys())


_imported = False


def _auto_import():
    """Import all .py files in this package to trigger @register decorators."""
    global _imported
    if _imported:
        return
    _imported = True

    pkg_dir = os.path.dirname(__file__)
    for filename in os.listdir(pkg_dir):
        if filename.endswith(".py") and not filename.startswith("_"):
            module_name = filename[:-3]
            try:
                importlib.import_module(f"exchanges.{module_name}")
            except Exception as e:
                logger.warning(f"Failed to import exchange plugin {module_name}: {e}")
