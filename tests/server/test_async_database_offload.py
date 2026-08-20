"""Regression coverage for synchronous routing calls at async boundaries."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from typing import Any

import pytest

from omnigent.server.routes._sessions import helpers


@pytest.mark.asyncio
async def test_runner_resource_lookup_runs_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The async runner-client helper does not call the sync router on-loop."""
    event_loop_thread = threading.get_ident()
    call_threads: list[int] = []
    client = object()

    async def run_in_dedicated_thread(callable_: Any, *args: Any) -> Any:
        loop = asyncio.get_running_loop()
        result: asyncio.Future[Any] = loop.create_future()

        def finish(value: Any = None, error: BaseException | None = None) -> None:
            if result.done():
                return
            if error is None:
                result.set_result(value)
            else:
                result.set_exception(error)

        def run() -> None:
            try:
                value = callable_(*args)
            except BaseException as exc:
                loop.call_soon_threadsafe(finish, None, exc)
            else:
                loop.call_soon_threadsafe(finish, value)

        thread = threading.Thread(target=run)
        thread.start()
        try:
            return await result
        finally:
            thread.join()

    monkeypatch.setattr(helpers.asyncio, "to_thread", run_in_dedicated_thread)

    class Router:
        def client_for_session_resources(
            self,
            _session_id: str,
            *,
            conversation: Any | None = None,
        ) -> Any:
            del conversation
            call_threads.append(threading.get_ident())
            return SimpleNamespace(client=client)

    assert await helpers._get_runner_client_impl("conv-1", Router()) is client  # type: ignore[arg-type]
    assert call_threads
    assert all(thread_id != event_loop_thread for thread_id in call_threads)
