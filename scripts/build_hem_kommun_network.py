#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_out = repo_root / "data" / "interim" / "hem_kommun_network"
    default_r = repo_root / "scripts" / "hem_kommun_network.R"

    parser = argparse.ArgumentParser(
        description="Build Streamlit-ready hem_kommun_network artifacts."
    )
    parser.add_argument(
        "--input-csv",
        type=Path,
        default=None,
        help="Optional input CSV path. If omitted, the R script auto-detects under data/.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=default_out,
        help=f"Output directory (default: {default_out}).",
    )
    parser.add_argument(
        "--r-script",
        type=Path,
        default=default_r,
        help=f"Path to hem_kommun_network.R (default: {default_r}).",
    )
    parser.add_argument(
        "--allow-novus-output",
        action="store_true",
        help="Allow output under data/interim/novus (disabled by default).",
    )
    return parser.parse_args()


def is_under(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    out_dir = args.out_dir.resolve()
    novus_dir = (repo_root / "data" / "interim" / "novus").resolve()

    if is_under(out_dir, novus_dir) and not args.allow_novus_output:
        raise SystemExit(
            "Refusing to write to data/interim/novus without --allow-novus-output."
        )

    rscript = shutil.which("Rscript")
    if not rscript:
        raise SystemExit("Rscript not found in PATH.")

    if not args.r_script.exists():
        raise SystemExit(f"R script not found: {args.r_script}")

    cmd = [rscript, str(args.r_script)]
    if args.input_csv is not None:
        cmd.append(str(args.input_csv))
    cmd.append(str(out_dir))

    print("Running:", " ".join(cmd))
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(cmd, check=True, cwd=repo_root)
    print(f"Build complete: {out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
