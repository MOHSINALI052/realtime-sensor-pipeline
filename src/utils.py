from pathlib import Path
import shutil

def move_file(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    return Path(shutil.move(str(src), str(dest)))

# NEW: copy instead of move
def copy_file(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    shutil.copy2(str(src), str(dest))   # preserves mtime
    return dest
