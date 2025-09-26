import shutil
import tempfile
from pathlib import Path


def atomic_write_text(target: Path, content: str, encoding: str = "utf-8"):
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, encoding=encoding) as tmp:
        tmp.write(content)
        tmp_path = tmp.name
    shutil.move(tmp_path, target)


def write_files_atomic(root: Path, files: dict[str, str]) -> dict[str, str]:
    """
    files: {"relative/path.ext": "content", ...}
    Returns map of absolute paths written.
    """
    written: dict[str, str] = {}
    for rel, body in files.items():
        target = (root / rel).resolve()
        atomic_write_text(target, body)
        written[rel] = str(target)
    return written
