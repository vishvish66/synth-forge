from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any

import pandas as pd


@dataclass
class StoredArtifact:
    request_id: str
    created_at: datetime
    expires_at: datetime
    domain: str
    tables: dict[str, pd.DataFrame]
    metadata: dict[str, Any]


class InMemoryArtifactStore:
    def __init__(self) -> None:
        self._lock = Lock()
        self._store: dict[str, StoredArtifact] = {}

    def put(
        self,
        request_id: str,
        domain: str,
        tables: dict[str, pd.DataFrame],
        ttl_minutes: int,
        metadata: dict[str, Any] | None = None,
    ) -> StoredArtifact:
        now = datetime.now(timezone.utc)
        artifact = StoredArtifact(
            request_id=request_id,
            created_at=now,
            expires_at=now + timedelta(minutes=ttl_minutes),
            domain=domain,
            tables=tables,
            metadata=metadata or {},
        )
        with self._lock:
            self._purge_locked(now)
            self._store[request_id] = artifact
        return artifact

    def get(self, request_id: str) -> StoredArtifact | None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._purge_locked(now)
            return self._store.get(request_id)

    def _purge_locked(self, now: datetime) -> None:
        expired = [rid for rid, item in self._store.items() if item.expires_at <= now]
        for rid in expired:
            self._store.pop(rid, None)


artifact_store = InMemoryArtifactStore()
