"""The 1.0 import rename, asserted rather than assumed.

1.0 renamed the import package from ``yserver`` to ``yashserver``. Both halves
of that are load-bearing:

* ``import yashserver`` must work, obviously.
* ``import yserver`` must **fail**. A compatibility shim was considered and
  rejected: the ``yashserver`` distribution shipping a top-level ``yserver``
  package would collide with the separate ``yserver`` distribution on PyPI,
  leaving two distributions owning one import path. Without a test, nothing
  would notice a shim reappearing.

The failing half only means something in a clean environment. An older
``yashserver`` 0.x release installed alongside this one also provides a
top-level ``yserver``, so that test skips when it detects one rather than
reporting a failure the working tree cannot cause.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import yashserver  # noqa: E402


def _foreign_yserver_installed() -> str | None:
    """Path of a ``yserver`` package that came from somewhere else, if any."""

    spec = importlib.util.find_spec("yserver")
    if spec is None or not spec.origin:
        return None
    return spec.origin


class TestImportRename(unittest.TestCase):
    def test_yashserver_imports(self) -> None:
        self.assertTrue(yashserver.__version__)
        self.assertEqual(yashserver.__name__, "yashserver")

    def test_every_submodule_imports_under_the_new_name(self) -> None:
        for name in (
            "core", "tcp", "udp", "http", "websocket", "server",
            "database", "archive", "upload", "sync", "tools", "plugin", "plugins",
        ):
            with self.subTest(module=name):
                module = importlib.import_module(f"yashserver.{name}")
                self.assertEqual(module.__name__, f"yashserver.{name}")

    def test_public_classes_report_the_new_module_path(self) -> None:
        # __module__ feeds pickling, reprs and tracebacks; a stale value here
        # would mean something still believes it lives in yserver.
        self.assertEqual(yashserver.YHttpServer.__module__, "yashserver.http")
        self.assertEqual(yashserver.YTcpServer.__module__, "yashserver.tcp")
        self.assertEqual(yashserver.YUdpServer.__module__, "yashserver.udp")
        self.assertEqual(yashserver.YWebSocketServer.__module__, "yashserver.websocket")
        self.assertEqual(yashserver.TLSConfig.__module__, "yashserver.core")

    def test_no_module_claims_to_live_in_yserver(self) -> None:
        for name, module in list(sys.modules.items()):
            if name.startswith("yashserver"):
                origin = getattr(module, "__file__", "") or ""
                with self.subTest(module=name):
                    self.assertNotIn(
                        f"{'yserver'}{Path('/').as_posix()}",
                        origin.replace("\\", "/").replace("yashserver/", ""),
                    )

    def test_importing_yserver_raises_module_not_found(self) -> None:
        foreign = _foreign_yserver_installed()
        if foreign is not None and "yashserver" not in foreign:
            self.skipTest(
                "a separate 'yserver' package is installed in this environment "
                f"({foreign}); the clean-break assertion is only meaningful "
                "without one"
            )
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module("yserver")

    def test_yserver_submodules_are_gone_too(self) -> None:
        foreign = _foreign_yserver_installed()
        if foreign is not None and "yashserver" not in foreign:
            self.skipTest("a separate 'yserver' package is installed in this environment")
        for name in ("yserver.core", "yserver.http", "yserver.server"):
            with self.subTest(module=name):
                with self.assertRaises(ModuleNotFoundError):
                    importlib.import_module(name)


if __name__ == "__main__":
    unittest.main()
