"""Static guard: closures must not read a name before its first local store.

Regression context (2026-09-12): ``_api_search_wave`` in ``bookbot/booker.py``
assigned ``booking_claimed`` but omitted it from its ``nonlocal`` declaration.
Python therefore treated the name as local, and the first read raised
``UnboundLocalError`` on *every* call -- silently disabling the whole rush
API Search race (P3) while nothing surfaced in the logs except stray
"Task exception was never retrieved" tracebacks.

This module scans every function in the ``bookbot`` package and fails if a
function reads a name (lexically) before its first local store while an
enclosing function scope or the module binds the same name, without a
``nonlocal``/``global`` declaration.

Notes:
- Comprehension scopes are treated as boundaries (their targets are not
  function locals in Python 3).
- ``x += 1`` style augmented stores count as reads, because Python reads the
  target before rebinding it (an unbound local target raises first).
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent.parent / "bookbot"

_BOUNDARY = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.Lambda,
    ast.ClassDef,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
)


def _own_scope(nodes):
    """Yield ``nodes`` and their descendants, without crossing nested scopes."""
    stack = list(nodes)
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, _BOUNDARY):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _param_names(fn) -> set[str]:
    args = fn.args
    names = {a.arg for a in [*args.posonlyargs, *args.args, *args.kwonlyargs]}
    if args.vararg:
        names.add(args.vararg.arg)
    if args.kwarg:
        names.add(args.kwarg.arg)
    return names


def _scope_info(fn):
    """Return (bindings, stores, loads, implicit_reads, nonlocals, globals)."""
    bindings = set(_param_names(fn))
    stores: dict[str, list[tuple[int, int]]] = {}
    loads: dict[str, list[tuple[int, int]]] = {}
    implicit_reads: dict[str, list[tuple[int, int]]] = {}
    nonlocals: set[str] = set()
    globals_: set[str] = set()
    for node in _own_scope(fn.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bindings.add(node.name)
        elif isinstance(node, ast.alias):
            bindings.add((node.asname or node.name).split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            bindings.add(node.name)
        elif isinstance(node, ast.Nonlocal):
            nonlocals.update(node.names)
        elif isinstance(node, ast.Global):
            globals_.update(node.names)
        elif isinstance(node, ast.Name):
            pos = (node.lineno, node.col_offset)
            if isinstance(node.ctx, ast.Store):
                stores.setdefault(node.id, []).append(pos)
                bindings.add(node.id)
            elif isinstance(node.ctx, ast.Load):
                loads.setdefault(node.id, []).append(pos)
        elif isinstance(node, ast.AugAssign) and isinstance(node.target, ast.Name):
            # ``x += 1`` reads x before rebinding: mark a read just before it.
            pos = (node.target.lineno, max(0, node.target.col_offset - 1))
            implicit_reads.setdefault(node.target.id, []).append(pos)
    return bindings, stores, loads, implicit_reads, nonlocals, globals_


def _module_bindings(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    for node in _own_scope(tree.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.alias):
            names.add((node.asname or node.name).split(".")[0])
        elif isinstance(node, ast.ExceptHandler) and node.name:
            names.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            names.add(node.id)
    return names


def _nested_functions(fn):
    return [
        n
        for n in _own_scope(fn.body)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _check_function(fn, enclosing, module_bindings, findings, filename):
    bindings, stores, loads, implicit_reads, nonlocals, globals_ = _scope_info(fn)
    params = _param_names(fn)
    for name, store_positions in stores.items():
        if name in nonlocals or name in globals_ or name in params:
            continue
        read_positions = list(loads.get(name, [])) + list(implicit_reads.get(name, []))
        if not read_positions:
            continue
        if min(read_positions) >= min(store_positions):
            continue
        if name in module_bindings or any(name in scope for scope in enclosing):
            findings.append(
                f"{filename}: `{fn.name}` (line {fn.lineno}) uses `{name}` as a "
                f"local (read at line {min(read_positions)[0]}, first store at "
                f"line {min(store_positions)[0]}) but an enclosing scope binds it "
                f"-- add `{name}` to nonlocal/global (UnboundLocalError risk)"
            )
    for child in _nested_functions(fn):
        _check_function(child, enclosing + [bindings], module_bindings, findings, filename)


def scan_source(path) -> list[str]:
    path = Path(path)
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    module_bindings = _module_bindings(tree)
    findings: list[str] = []
    for node in _own_scope(tree.body):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            _check_function(node, [], module_bindings, findings, path.name)
    # Methods (functions directly inside a class) get the same check.
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    _check_function(child, [], module_bindings, findings, path.name)
    return findings


def test_no_unbound_local_reads_before_store():
    findings: list[str] = []
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        findings.extend(scan_source(path))
    assert not findings, "Closure scoping risks found:\n" + "\n".join(findings)


if __name__ == "__main__":
    issues: list[str] = []
    for path in sorted(PACKAGE_DIR.glob("*.py")):
        issues.extend(scan_source(path))
    if issues:
        print(f"FOUND {len(issues)} issue(s):")
        for issue in issues:
            print(" -", issue)
        raise SystemExit(1)
    print(f"OK: scanned {len(list(PACKAGE_DIR.glob('*.py')))} modules -- no closure scoping risks")
