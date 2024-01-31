import logging

import pluggy

log = logging.getLogger(__name__)

_listener_manager = None


class ListenerManager:
    """Manage listener registration and provides hook property for calling them."""

    def __init__(self):
        from ray.lineage import spec

        self.pm = pluggy.PluginManager("ray")
        self.pm.add_hookspecs(spec)
        self.pm.load_setuptools_entrypoints("ray")

    @property
    def has_listeners(self) -> bool:
        return bool(self.pm.get_plugins())

    @property
    def hook(self):
        """Return hook, on which plugin methods specified in spec can be called."""
        return self.pm.hook


def get_listener_manager() -> ListenerManager:
    """Get singleton listener manager."""
    global _listener_manager
    if not _listener_manager:
        _listener_manager = ListenerManager()
    return _listener_manager
