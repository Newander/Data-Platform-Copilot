import threading
from typing import Optional, Callable


class _Registry:
    def __init__(self):
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], int] = {}
        self._hist_ms: dict[tuple[str, tuple[tuple[str, str], ...]], list] = {}
        self._lock = threading.Lock()
        # Hook for external Prometheus
        self._external_exporter: Optional[Callable[[], str]] = None

    def set_external_exporter(self, exporter: Callable[[], str]):
        """ Registrate external Prometheus plaintext) """
        self._external_exporter = exporter

    def _key(self, name: str, labels: dict[str, str]) -> tuple[str, tuple[tuple[str, str], ...]]:
        if not labels:
            return name, tuple()
        return name, tuple(sorted(labels.items()))

    def inc(self, name: str, labels: dict[str, str] | None = None, value: int = 1):
        with self._lock:
            k = self._key(name, labels or {})
            self._counters[k] = self._counters.get(k, 0) + value

    def observe_ms(self, name: str, value_ms: int, labels: dict[str, str] | None = None):
        with self._lock:
            k = self._key(name, labels or {})
            self._hist_ms.setdefault(k, []).append(value_ms)

    def _export_local_prometheus(self) -> str:
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

    def export_prometheus(self) -> str:
        # Склеиваем внешний дамп (если есть, например, от Instrumentator) и локальные метрики
        external = ""
        if self._external_exporter:
            try:
                external = self._external_exporter() or ""
            except Exception:
                external = ""
        local = self._export_local_prometheus()
        if not external.strip():
            return local
        # Иначе объединяем (Prometheus терпит дубликаты разных меток/метрик в одном payload)
        return external.rstrip() + "\n" + local


METRICS = _Registry()
