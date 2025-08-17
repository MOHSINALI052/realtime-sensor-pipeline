from pathlib import Path
import shutil

def _unique_path(dest: Path) -> Path:
    """Return a non-existing path by appending __N if needed."""
    if not dest.exists():
        return dest
    stem, suffix = dest.stem, dest.suffix
    i = 1
    while True:
        cand = dest.with_name(f"{stem}__{i}{suffix}")
        if not cand.exists():
            return cand
        i += 1

def move_file(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = _unique_path(dest_dir / src.name)
    return Path(shutil.move(str(src), str(dest)))

def copy_file(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = _unique_path(dest_dir / src.name)
    shutil.copy2(str(src), str(dest))
    return dest
