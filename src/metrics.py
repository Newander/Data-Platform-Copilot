import threading
from typing import Dict, Tuple


class _Registry:
    def __init__(self):
        self._counters: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], int] = {}
        self._hist_ms: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], list] = {}
        self._lock = threading.Lock()

    def _key(self, name: str, labels: Dict[str, str]) -> Tuple[str, Tuple[Tuple[str, str], ...]]:
        if not labels:
            return name, tuple()
        return name, tuple(sorted(labels.items()))

    def inc(self, name: str, labels: Dict[str, str] | None = None, value: int = 1):
        with self._lock:
            k = self._key(name, labels or {})
            self._counters[k] = self._counters.get(k, 0) + value

    def observe_ms(self, name: str, value_ms: int, labels: Dict[str, str] | None = None):
        with self._lock:
            k = self._key(name, labels or {})
            self._hist_ms.setdefault(k, []).append(value_ms)

    def export_prometheus(self) -> str:
        lines: list[str] = []
        with self._lock:
            for (name, labels), val in self._counters.items():
                lbl = ""
                if labels:
                    lbl = "{" + ",".join([f'{k}="{v}"' for k, v in labels]) + "}"
                lines.append(f"{name}{lbl} {val}")
            for (name, labels), arr in self._hist_ms.items():
                if not arr:
                    continue
                lbl = ""
                if labels:
                    lbl = "{" + ",".join([f'{k}="{v}"' for k, v in labels]) + "}"
                s = sum(arr)
                c = len(arr)
                p95 = sorted(arr)[max(0, int(0.95 * c) - 1)]
                lines.append(f"{name}_sum{lbl} {s}")
                lines.append(f"{name}_count{lbl} {c}")
                lines.append(f"{name}_p95{lbl} {p95}")
        return "\n".join(lines) + "\n"


METRICS = _Registry()
