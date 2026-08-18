"""Project store — persists first-class, owner-private projects.

A project is a user-defined container that groups sessions and exists
independently of its members (see ``designs/PROJECTS_PRD.md``). This store owns
the ``projects`` table. Session→project membership lives on the conversation's
metadata row (``project_id``) and is managed by the conversation store, not
here.

Projects have no ACL of their own (PRD §9): every method is scoped by
``user_id`` so a caller only ever sees and mutates their own projects.

**Orvex divergence.** A project may carry a ``shared`` flag. When set, the
*read* methods (``get``, ``list``) also resolve for non-owners; the *write*
methods (``update``, ``delete``) stay owner-only. An unshared project — the
default — behaves exactly as upstream.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from omnigent.entities import Project


class ProjectStore(ABC):
    """
    Abstract base for project persistence.

    Manages the lifecycle of projects (CRUD). Writes are scoped by ``user_id``
    because projects are owner-private. Reads are scoped by ``user_id`` too,
    except that an Orvex ``shared`` project also resolves for non-owners.
    """

    def __init__(self, storage_location: str) -> None:
        """
        Initialize the project store.

        :param storage_location: Backend-specific storage URI,
            e.g. ``"sqlite:///chat.db"`` for SQLAlchemy.
        """
        self.storage_location = storage_location

    @abstractmethod
    def create(
        self,
        project_id: str,
        name: str,
        user_id: str | None,
        config: dict[str, Any] | None = None,
        shared: bool = False,
    ) -> Project:
        """
        Insert a new, empty project.

        :param project_id: Pre-generated unique project id (a UUID string).
        :param name: Human-readable project name. Trimmed, non-empty, unique
            among the owner's projects.
        :param user_id: Owning user, or ``None`` in single-user mode.
        :param config: Optional default session settings (opaque JSON object);
            ``None`` or empty stores no defaults.
        :param shared: Orvex — create the project already readable by
            non-owners. Defaults to ``False`` (upstream behaviour).
        :returns: The newly created :class:`Project`.
        :raises OmnigentError: ``ALREADY_EXISTS`` if the owner already has a
            project with this name.
        """
        ...

    @abstractmethod
    def get(self, project_id: str, *, user_id: str | None) -> Project | None:
        """
        Return a readable project by id, or ``None`` if not found.

        Orvex: a project owned by someone else resolves only when it is
        ``shared``; otherwise it is treated as not found, as upstream.

        :param project_id: Opaque project identifier.
        :param user_id: The requesting user; a project owned by someone
            else and not shared is treated as not found.
        :returns: The :class:`Project` if found and readable, else ``None``.
        """
        ...

    @abstractmethod
    def list(self, *, user_id: str | None) -> list[Project]:
        """
        List the projects readable by ``user_id``, ordered by
        ``created_at ASC, id ASC``.

        Orvex: that is the caller's own projects **plus** every ``shared``
        project in the workspace. Without the second half, sharing works for
        no identity but the row's owner — this is the site the story calls out
        as the one whose omission hides inside the check meant to catch it.

        :param user_id: The user whose readable projects to return.
        :returns: List of :class:`Project` instances.
        """
        ...

    @abstractmethod
    def update(
        self,
        project_id: str,
        *,
        user_id: str | None,
        name: str | None = None,
        config: dict[str, Any] | None = None,
        shared: bool | None = None,
    ) -> Project | None:
        """
        Update mutable fields of an owned project.

        ``None`` leaves a field unchanged. Returns ``None`` if the project does
        not exist or is not owned by ``user_id`` — **owner-only, including for
        a ``shared`` project**: sharing opens reads, never writes.

        :param project_id: Opaque project identifier.
        :param user_id: The requesting owner.
        :param name: New name, or ``None`` to leave unchanged. Trimmed,
            non-empty, unique among the owner's projects.
        :param config: New config object to replace the stored one, or ``None``
            to leave it unchanged. An empty dict clears the stored defaults.
        :param shared: Orvex — new value for the share flag, or ``None`` to
            leave it unchanged. Owner-only, like every other write here.
        :returns: The updated :class:`Project`, or ``None`` if not found.
        :raises OmnigentError: ``ALREADY_EXISTS`` if the new name collides with
            another of the owner's projects.
        """
        ...

    @abstractmethod
    def delete(self, project_id: str, *, user_id: str | None) -> bool:
        """
        Delete an owned project. Idempotent. Owner-only, including for a
        ``shared`` project.

        Deleting a project does not delete its member sessions; unfiling them
        (clearing ``project_id``) is the caller's responsibility.

        :param project_id: Opaque project identifier.
        :param user_id: The requesting owner.
        :returns: ``True`` if removed; ``False`` if not found / not owned.
        """
        ...
