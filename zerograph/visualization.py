"""Graph visualization - generate Mermaid diagrams from StateGraph."""

from __future__ import annotations

import hashlib
import re
from typing import TYPE_CHECKING

from zerograph.constants import START, END

if TYPE_CHECKING:
    from zerograph.graph.state import StateGraph

__all__ = ("get_mermaid",)

_RESERVED = frozenset({START, END})


def get_mermaid(graph: StateGraph) -> str:
    """Generate a Mermaid flowchart from a StateGraph."""
    lines = ["flowchart TD"]

    lines.append(f'    {START}(["START"])')
    lines.append(f'    {END}(["END"])')

    for name in graph.nodes:
        safe_name = _safe_id(name)
        lines.append(f'    {safe_name}["{_escape_label(name)}"]')

    for start, end in graph.edges:
        s = _safe_id(start) if start not in _RESERVED else start
        e = _safe_id(end) if end not in _RESERVED else end
        lines.append(f"    {s} --> {e}")

    for source, branches in graph.branches.items():
        s = _safe_id(source) if source not in _RESERVED else source
        for name, branch in branches.items():
            if branch.ends:
                for cond, target in branch.ends.items():
                    t = _safe_id(target) if target not in _RESERVED else target
                    lines.append(f'    {s} -->|"{_escape_label(cond)}"| {t}')

    for starts, end in graph.waiting_edges:
        e = _safe_id(end) if end not in _RESERVED else end
        for s in starts:
            sid = _safe_id(s) if s not in _RESERVED else s
            lines.append(f"    {sid} --> {e}")

    return "\n".join(lines)


def _escape_label(name: str) -> str:
    return (name
        .replace('&', '&amp;')
        .replace('"', '&quot;')
        .replace('[', '&#91;')
        .replace(']', '&#93;')
        .replace('\n', '&#10;')
        .replace('\r', '')
        .replace('{', '&#123;')
        .replace('}', '&#125;')
        .replace('<', '&lt;')
        .replace('>', '&gt;'))


def _safe_id(name: str) -> str:
    base = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    h = hashlib.sha256(name.encode()).hexdigest()[:6]
    if base and base[0].isdigit():
        base = f"n_{base}"
    return f"{base}_{h}"
