"""Only the action plugin may import pcbnew at runtime; it hands it to the backend.

Ruff's TID251 rule bans ``import pcbnew`` statements. It cannot see an import
by name, such as ``importlib.import_module("pcbnew")``, so this scan does.
The application layer imports neither pcbnew nor wx, by statement or by name.
"""

import ast
from collections import Counter
from collections.abc import Iterable, Iterator
from pathlib import Path
import re

import pytest

ROOT = Path(__file__).resolve().parents[1]

# The plugin's entry point imports pcbnew for good.
_PERMANENT = ("plugin.py",)
# Its entry in pyproject.toml's TID251 exemptions, with the tests'.
_PERMANENT_EXEMPTIONS = {"plugin.py", "tests/**"}
# Imports by name not yet behind the backend, by module, with their count.
# Fabrication's three go when gerber and drill output move to the backend.
_ALLOWED = {"fabrication.py": 3}


def _names(name: str, module: str) -> bool:
    """Return whether an imported name is ``module`` or one of its submodules."""
    return name == module or name.startswith(module + ".")


# Where import_module and __import__ come from, under another name too.
_IMPORTERS = ("importlib", "builtins")


def _import_functions(tree: ast.AST) -> set[str]:
    """Return the names a module calls ``import_module`` and ``__import__`` by.

    ``from importlib import import_module as load`` makes ``load`` one.
    """
    functions = {"import_module", "__import__"}
    names = set(functions)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _IMPORTERS:
            names.update(
                alias.asname
                for alias in node.names
                if alias.asname and alias.name in functions
            )
    return names


def imports_by_name(source: str, module: str = "pcbnew") -> int:
    """Count ``import_module``/``__import__`` calls naming a module or a submodule.

    The module name may be passed by position or as ``name=``, and the
    function called by another name it was imported as.
    """
    count = 0
    tree = ast.parse(source)
    functions = _import_functions(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        name = (
            function.id
            if isinstance(function, ast.Name)
            else function.attr
            if isinstance(function, ast.Attribute)
            else ""
        )
        argument = (
            node.args[0]
            if node.args
            else next((kw.value for kw in node.keywords if kw.arg == "name"), None)
        )
        if (
            name in functions
            and isinstance(argument, ast.Constant)
            and isinstance(argument.value, str)
            and _names(argument.value, module)
        ):
            count += 1
    return count


def import_statements(source: str, module: str = "pcbnew") -> int:
    """Count ``import pcbnew`` and ``from pcbnew import`` statements, nested too.

    Any other module is counted the same way when named.
    """
    count = 0
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            count += sum(_names(alias.name, module) for alias in node.names)
        elif (
            isinstance(node, ast.ImportFrom)
            and node.level == 0
            and node.module is not None
            and _names(node.module, module)
        ):
            count += 1
    return count


def relative_imports(source: str, package: str) -> set[str]:
    """Return the plugin modules a module of ``package`` imports relatively.

    Imports at any depth count, inside a function too. Each name a ``from``
    import takes may be a module itself, so it is listed as well.
    """
    found = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom) and node.level:
            parts = package.split(".") if package else []
            base = parts[: len(parts) - node.level + 1]
            target = [*base, *(node.module.split(".") if node.module else [])]
            found.add(".".join(target))
            found.update(".".join([*target, alias.name]) for alias in node.names)
    return found - {""}


def _module_path(name: str) -> Path:
    """Return the file of a plugin module or package, named from the root."""
    path = ROOT.joinpath(*name.split("."))
    return path / "__init__.py" if path.is_dir() else path.with_suffix(".py")


def _reached(modules: Iterable[str]) -> set[str]:
    """Return these plugin modules and every one they import, at any depth."""
    reached: set[str] = set()
    pending = list(modules)
    while pending:
        name = pending.pop()
        path = _module_path(name)
        if name in reached or not path.is_file():
            continue
        reached.add(name)
        package = name if path.name == "__init__.py" else name.rpartition(".")[0]
        pending.extend(relative_imports(path.read_text(encoding="utf-8"), package))
    return reached


def _ban_patterns() -> list[str]:
    """Return pyproject.toml's patterns exempt from the pcbnew import ban, as written."""
    text = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    section = text.split("[tool.ruff.lint.per-file-ignores]", 1)[1].split("\n[", 1)[0]
    return re.findall(r'^"([^"]+)"\s*=\s*\[[^\]]*"TID251"', section, flags=re.M)


def _ban_exemptions() -> set[str]:
    """Return the paths exempt from the pcbnew import ban, relative to the root."""
    return {pattern.removeprefix("./") for pattern in _ban_patterns()}


def _runtime_modules(root: Path = ROOT) -> Iterator[tuple[str, Path]]:
    """Yield every plugin module that runs inside KiCad, by relative path.

    The plugin's runtime is what the PCM archive ships: the top-level modules
    and the directories its script copies, without the tests it prunes and
    the root conftest.py, which ships but only pytest runs. Anything else in
    a checkout, such as a local archive build or a virtual environment, is
    not.
    """
    script = (root / "PCM" / "create_pcm_archive.sh").read_text(encoding="utf-8")
    files = script.split("for file in ", 1)[1].split(";", 1)[0].split()
    directories = script.split("for dir in ", 1)[1].split(";", 1)[0].split()
    paths = [
        *(
            path
            for pattern in files
            if pattern.endswith(".py")
            for path in root.glob(pattern)
        ),
        *(path for name in directories for path in (root / name).rglob("*.py")),
    ]
    for path in sorted(paths):
        relative = path.relative_to(root).as_posix()
        if (
            relative.startswith(_PERMANENT)
            or path.name.startswith("test_")
            or path.name == "conftest.py"
            or "__pycache__" in path.parts
        ):
            continue
        yield relative, path


def test_only_allowed_modules_import_pcbnew_by_name() -> None:
    """A new import by name fails here; removing one means shrinking the list."""
    found = Counter(
        {
            relative: count
            for relative, path in _runtime_modules()
            if (count := imports_by_name(path.read_text(encoding="utf-8")))
        }
    )
    assert dict(found) == _ALLOWED


def test_only_modules_that_import_pcbnew_are_exempt_from_the_ban() -> None:
    """An exemption for a module that imports nothing would let a new import in.

    Ruff exempts the permanent importers and, until each moves behind the
    backend, the modules that still import pcbnew; no other module.
    """
    importers = {
        relative
        for relative, path in _runtime_modules()
        if import_statements(path.read_text(encoding="utf-8"))
    }
    exemptions = _ban_exemptions()

    assert exemptions >= _PERMANENT_EXEMPTIONS
    assert exemptions - _PERMANENT_EXEMPTIONS == importers


@pytest.mark.parametrize(
    "source, count",
    [
        ("import pcbnew", 1),
        ("import pcbnew as kicad_pcbnew", 1),
        ("from pcbnew import ToMM, FromMM", 1),
        ("def f():\n    import pcbnew.foo", 1),
        ("from .pcbnew import x", 0),
        ("import pcbnew_tools", 0),
    ],
)
def test_the_scan_finds_import_statements(source: str, count: int) -> None:
    """Statements importing pcbnew count, at any depth; look-alikes do not."""
    assert import_statements(source) == count


@pytest.mark.parametrize(
    "source, count",
    [
        ('import_module("pcbnew")', 1),
        ('importlib.import_module("pcbnew")', 1),
        ('importlib.import_module("pcbnew.something")', 1),
        ('importlib.import_module(name="pcbnew")', 1),
        ('__import__(name="pcbnew")', 1),
        ('import_module(package="pcbnew", name="wx")', 0),
        ('__import__("pcbnew")', 1),
        ('getattr(import_module("pcbnew"), "F_Cu")', 1),
        ('from importlib import import_module as load\nload("pcbnew")', 1),
        ('import builtins\nbuiltins.__import__("pcbnew")', 1),
        ('load("pcbnew")', 0),
        ('import_module("wx")', 0),
        ('import_module("pcbnew_tools")', 0),
        ("import_module(name)", 0),
        ('print("pcbnew")', 0),
    ],
)
def test_the_scan_finds_imports_by_name(source: str, count: int) -> None:
    """Calls naming pcbnew count; other modules, names and strings do not."""
    assert imports_by_name(source) == count


def test_the_scan_covers_the_plugins_runtime() -> None:
    """The scan reads plugin code, the SWIG backend included, not tests or tooling."""
    modules = {relative for relative, _path in _runtime_modules()}
    assert {"mainwindow.py", "fabrication.py", "variant/native.py"} <= modules
    assert {"kicad_backend/records.py", "kicad_backend/swig/read.py"} <= modules
    assert "app/session.py" in modules
    assert not any(
        module.startswith((*_PERMANENT, "tests/", "db_build/")) for module in modules
    )
    assert not any(module.rsplit("/", 1)[-1].startswith("test_") for module in modules)


def test_the_scan_reads_only_what_the_archive_ships(tmp_path: Path) -> None:
    """A local archive build or a virtual environment is not the plugin's runtime."""
    (tmp_path / "PCM").mkdir()
    (tmp_path / "PCM" / "create_pcm_archive.sh").write_bytes(
        (ROOT / "PCM" / "create_pcm_archive.sh").read_bytes()
    )
    for relative in (
        "PCM/archive/plugins/plugin.py",
        "PCM/archive/plugins/mainwindow.py",
        "venv/lib/site.py",
        "env/lib/site.py",
        "mainwindow.py",
        "plugin.py",
        "kicad_backend/swig/read.py",
        "common/test_partsdb.py",
        "conftest.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("import pcbnew\n", encoding="utf-8")

    modules = {relative for relative, _path in _runtime_modules(tmp_path)}

    assert modules == {"mainwindow.py", "kicad_backend/swig/read.py"}


def test_each_exemption_is_anchored_at_the_root() -> None:
    """Ruff matches a bare file name in any directory; "./" keeps it to the root."""
    assert all(pattern.startswith("./") for pattern in _ban_patterns())


def test_the_application_layer_imports_neither_wx_nor_pcbnew() -> None:
    """No statement and no call names either, so another kind of view can use it."""
    sources = {
        path.relative_to(ROOT).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted((ROOT / "app").rglob("*.py"))
    }

    assert "app/session.py" in sources
    assert {
        relative: (import_statements(source, module) + imports_by_name(source, module))
        for relative, source in sources.items()
        for module in ("wx", "pcbnew")
        if import_statements(source, module) + imports_by_name(source, module)
    } == {}


def test_no_module_the_application_layer_reaches_imports_wx() -> None:
    """No plugin module the application layer imports, at any depth, imports wx.

    Importing the session runs only its modules' top-level imports; one
    inside a function waits for its first call, so this follows the source.
    """
    reached = _reached(
        f"app.{path.stem}" if path.stem != "__init__" else "app"
        for path in (ROOT / "app").glob("*.py")
    )
    sources = {name: _module_path(name).read_text(encoding="utf-8") for name in reached}

    assert {"app.session", "board_parts", "kicad_backend.records"} <= set(sources)
    assert {
        name
        for name, source in sources.items()
        if import_statements(source, "wx") + imports_by_name(source, "wx")
    } == set()


@pytest.mark.parametrize(
    "source, package, modules",
    [
        (
            "def view():\n    from ..library import Library",
            "app",
            {"library", "library.Library"},
        ),
        ("from .notify import Message", "app", {"app.notify", "app.notify.Message"}),
        ("from . import notify", "app", {"app", "app.notify"}),
        ("from .. import lcsc", "app", {"lcsc"}),
        ("from ...records import x", "kicad_backend.swig", {"records", "records.x"}),
        ("import wx\nfrom wx import adv", "app", set()),
    ],
)
def test_relative_imports_are_followed_from_any_depth(
    source: str, package: str, modules: set[str]
) -> None:
    """A relative import names a plugin module, wherever the statement is."""
    assert relative_imports(source, package) == modules


@pytest.mark.parametrize(
    "source, count",
    [
        ("import wx", 1),
        ("import wx.dataview as dv", 1),
        ("from wx import adv", 1),
        ('importlib.import_module("wx.lib")', 1),
        ("import wxpython_tools", 0),
        ("from .wx_harness import load", 0),
    ],
)
def test_the_scan_finds_wx_too(source: str, count: int) -> None:
    """The same scan counts wx imports when asked for wx."""
    assert import_statements(source, "wx") + imports_by_name(source, "wx") == count


def test_ruff_bans_wx_and_pcbnew_in_the_application_layer() -> None:
    """The app package's ruff settings extend the root's and ban both modules.

    Its banned-api table replaces the root's, so the pcbnew entry must be
    repeated, word for word.
    """
    root = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    app = (ROOT / "app" / "ruff.toml").read_text(encoding="utf-8")
    pcbnew_ban = re.search(r'^"pcbnew"\.msg = .*$', root, flags=re.M)

    assert re.search(r'^extend = "\.\./pyproject\.toml"$', app, flags=re.M)
    assert pcbnew_ban is not None and pcbnew_ban.group(0) in app.splitlines()
    assert re.search(r'^"wx"\.msg = ', app, flags=re.M)
