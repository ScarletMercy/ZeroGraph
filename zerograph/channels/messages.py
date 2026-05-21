"""add_messages reducer for message list channels."""

from __future__ import annotations

from typing import Any, Sequence

__all__ = ("add_messages", "RemoveMessage", "messages_state")


class RemoveMessage:
    """Marker to remove a message by ID."""

    __slots__ = ("id",)

    def __init__(self, id: str) -> None:
        self.id = id

    def __repr__(self) -> str:
        return f"RemoveMessage(id={self.id!r})"


def _get_message_id(msg: Any) -> str | None:
    """Extract ID from a message-like object."""
    if isinstance(msg, dict):
        return msg.get("id")
    return getattr(msg, "id", None)


def add_messages(existing: Sequence, new_messages: Sequence) -> list:
    """Merge message lists: add new, update existing (by id), remove marked.

    Messages are matched by their 'id' field. If a new message has the same
    id as an existing one, it replaces it. RemoveMessage entries remove by id.
    New messages without matching existing ids are appended.
    """
    if not existing:
        existing = []
    if not new_messages:
        return list(existing)

    # Build lookup of existing messages by id
    existing_by_id: dict[str, Any] = {}
    for msg in existing:
        mid = _get_message_id(msg)
        if mid is not None:
            existing_by_id[mid] = msg

    # Track IDs to remove and new messages to append
    to_remove: set[str] = set()
    to_append: list[Any] = []
    updated_by_new: set[str] = set()

    new_ids: set[str] = set()
    for msg in new_messages:
        if isinstance(msg, RemoveMessage):
            to_remove.add(msg.id)
        else:
            mid = _get_message_id(msg)
            if mid is not None and mid in existing_by_id:
                existing_by_id[mid] = msg
                updated_by_new.add(mid)
            elif mid is not None and mid in new_ids:
                for i in range(len(to_append) - 1, -1, -1):
                    if _get_message_id(to_append[i]) == mid:
                        to_append[i] = msg
                        break
            else:
                to_append.append(msg)
                if mid is not None:
                    new_ids.add(mid)

    to_remove -= updated_by_new

    # Build result: existing messages (with updates applied), minus removed, plus new
    result = []
    for msg in existing:
        mid = _get_message_id(msg)
        if mid is not None and mid in to_remove:
            continue
        if mid is not None and mid in existing_by_id:
            result.append(existing_by_id[mid])
        else:
            result.append(msg)

    result.extend([msg for msg in to_append if _get_message_id(msg) not in to_remove])
    return result


def messages_state(cls: type | None = None):
    """Decorator/marker for a message state TypedDict."""
    if cls is None:
        return messages_state
    cls.__messages_state__ = True
    return cls
