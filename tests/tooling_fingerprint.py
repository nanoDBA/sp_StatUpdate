"""Identify test tooling and any explicitly listed out-of-tree verdict inputs."""
import hashlib
import json
from pathlib import Path
import sys

FINGERPRINT_FORMAT = b"sp_StatUpdate_tooling_fingerprint_v2\0"
VOLATILE_DIRS = {"__pycache__", ".pytest_cache", "results", "test-results", "warning_test_results"}
ORCHESTRATION_FILES = {Path("Set-SqlContainer.ps1")}
INPUT_MANIFEST = "fingerprint-inputs.json"


def tooling_fingerprint(root: Path) -> str:
    if not root.is_dir():
        raise ValueError("Test tooling directory is unavailable")
    root = root.resolve()
    digest = hashlib.sha256(FINGERPRINT_FORMAT)
    files = sorted((p.relative_to(root) for p in root.rglob("*") if p.is_file()), key=Path.as_posix)
    count = 0
    for path in files:
        if path in ORCHESTRATION_FILES or path.suffix.lower() in {".log", ".pyc", ".pyo"} or any(
            part in VOLATILE_DIRS or part.startswith("test_logs_") for part in path.parts[:-1]
        ):
            continue
        digest.update(b"test-tree\0" + path.as_posix().encode("utf-8") + b"\0")
        digest.update(hashlib.sha256((root / path).read_bytes()).digest())
        count += 1
    manifest_path = root / INPUT_MANIFEST
    if manifest_path.is_file():
        inputs = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(inputs, list):
            raise ValueError(f"{INPUT_MANIFEST} must be a JSON array")
        labels = set()
        for item in sorted(inputs, key=lambda value: str(value.get("label", ""))):
            if not isinstance(item, dict) or not isinstance(item.get("label"), str) or not isinstance(item.get("path"), str):
                raise ValueError(f"Invalid entry in {INPUT_MANIFEST}")
            label = item["label"]
            if not label or label in labels:
                raise ValueError(f"Missing or duplicate fingerprint input label: {label!r}")
            labels.add(label)
            path = (root / item["path"]).resolve()
            if not path.is_file():
                raise ValueError(f"Fingerprint input is unavailable: {label}")
            digest.update(b"external-input\0" + label.encode("utf-8") + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
            count += 1
    if not count:
        raise ValueError("Test tooling directory is empty")
    return digest.hexdigest()


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) == 2 else Path(__file__).resolve().parent
    print(tooling_fingerprint(root))
