from pathlib import Path
import shutil
import time

def _unique_path(dest: Path) -> Path:
    """
    If `dest` exists, append a suffix like _1, _2, ... to avoid overwriting.
    """
    if not dest.exists():
        return dest
    stem, suffix = dest.stem, dest.suffix
    n = 1
    while True:
        candidate = dest.with_name(f"{stem}_{n}{suffix}")
        if not candidate.exists():
            return candidate
        n += 1

def move_file(src: Path, dest_dir: Path) -> Path:
    """
    Move `src` into `dest_dir`, creating the folder if needed.
    Avoids overwriting by generating a unique destination name.
    Returns the destination path.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = _unique_path(dest_dir / src.name)
    return Path(shutil.move(str(src), str(dest)))
