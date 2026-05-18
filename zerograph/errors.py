"""ZeroGraph error types."""

from __future__ import annotations

from collections.abc import Sequence


__all__ = (
    "EmptyChannelError",
    "InvalidUpdateError",
    "GraphRecursionError",
    "GraphBubbleUp",
    "GraphInterrupt",
    "ParentCommand",
)


class EmptyChannelError(Exception):
    """Raised when reading from an empty channel."""


class InvalidUpdateError(Exception):
    """Raised when attempting to update a channel with invalid values."""


class GraphRecursionError(RecursionError):
    """Raised when the graph exceeds the maximum number of steps."""


class GraphBubbleUp(Exception):
    """Base exception for graph control flow (interrupts, parent commands)."""


class GraphInterrupt(GraphBubbleUp):
    """Raised to interrupt graph execution."""

    def __init__(self, interrupts: Sequence = ()) -> None:
        self.interrupts = interrupts
        super().__init__(interrupts)


class ParentCommand(GraphBubbleUp):
    """Raised when a Command targets the parent graph."""

    def __init__(self, command) -> None:
        self.command = command
        super().__init__(command)
