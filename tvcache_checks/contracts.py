"""Test-side adapter contract, independent of an unagreed wire protocol."""

from dataclasses import dataclass
from typing import AsyncContextManager, Callable, Literal, Protocol


Path = tuple[str, ...]
Status = Literal["owner", "pending", "ready", "failed", "missing", "prefix"]


@dataclass(frozen=True)
class Outcome:
    status: Status
    node_id: int | None
    generation: int | None
    value: str | None = None
    owner_token: object | None = None
    error: str | None = None

    def validate(self) -> "Outcome":
        if self.status not in {"owner", "pending", "ready", "failed", "missing", "prefix"}:
            raise AssertionError(f"Unknown outcome status: {self.status!r}")
        if self.status != "missing":
            if type(self.node_id) is not int or self.node_id < 0:
                raise AssertionError("Node ID must be a nonnegative integer")
        if self.status in {"owner", "pending", "ready", "failed"}:
            if type(self.generation) is not int or self.generation < 1:
                raise AssertionError("Executable nodes require a positive generation")
        if self.status == "owner" and self.owner_token is None:
            raise AssertionError("Owner outcome requires a publication token")
        if self.status != "owner" and self.owner_token is not None:
            raise AssertionError("Publication token leaked to a non-owner")
        if self.status == "ready" and not isinstance(self.value, str):
            raise AssertionError("Ready fixture result must be a string, including empty strings")
        if self.status != "ready" and self.value is not None:
            raise AssertionError("Uncompleted outcome must not expose a successful result")
        return self

    def public_fields(self) -> dict:
        """Never include the private publication token in logs."""
        return {
            "status": self.status,
            "node_id": self.node_id,
            "generation": self.generation,
            "value": self.value,
            "error": self.error,
        }


class PendingClient(Protocol):
    async def lookup_or_claim(self, task_id: str, path: Path, attempt_id: str) -> Outcome:
        """Atomically return owner, pending, ready, or failed; do not block a waiter."""
        ...

    async def inspect(self, task_id: str, path: Path) -> Outcome:
        """Read-only status polling; never claim or create a node."""
        ...

    async def publish(
        self, task_id: str, path: Path, *, node_id: int, generation: int,
        owner_token: object, value: str,
    ) -> None:
        """Publish or raise on rejection; no snapshot is needed for these fixtures."""
        ...

    async def dump_all(self) -> dict:
        ...


ClientFactory = Callable[[], AsyncContextManager[PendingClient]]
