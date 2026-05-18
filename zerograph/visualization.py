"""Graph visualization - generate Mermaid diagrams from StateGraph."""

from __future__ import annotations

from typing import TYPE_CHECKING

from zerograph.constants import START, END

if TYPE_CHECKING:
    from zerograph.graph.state import StateGraph

__all__ = ("get_mermaid",)


def get_mermaid(graph: StateGraph) -> str:
    """Generate a Mermaid flowchart from a StateGraph."""
    lines = ["flowchart TD"]

    # START and END nodes with special shapes
    lines.append(f'    {START}(["START"])')
    lines.append(f'    {END}(["END"])')

    # All user-defined nodes
    for name in graph.nodes:
        safe_name = _safe_id(name)
        lines.append(f'    {safe_name}["{name}"]')

    # Direct edges
    for start, end in graph.edges:
        s = _safe_id(start) if start != START else START
        e = _safe_id(end) if end != END else END
        lines.append(f"    {s} --> {e}")

    # Conditional edges
    for source, branches in graph.branches.items():
        s = _safe_id(source) if source != START else START
        for name, branch in branches.items():
            if branch.ends:
                for cond, target in branch.ends.items():
                    t = _safe_id(target) if target != END else END
                    lines.append(f'    {s} -->|"{cond}"| {t}')

    # Waiting edges (fan-in)
    for starts, end in graph.waiting_edges:
        e = _safe_id(end) if end != END else END
        for s in starts:
            sid = _safe_id(s) if s != START else START
            lines.append(f"    {sid} --> {e}")

    return "\n".join(lines)


def _safe_id(name: str) -> str:
    """Make a name safe for use as a Mermaid node ID."""
    import re
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)
