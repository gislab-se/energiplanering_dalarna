from __future__ import annotations

from pathlib import Path

from map_factory import build_map, choose_default_field, load_layers


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    out_html = repo_root / "maps" / "exports" / "landskapstyper_map_python.html"
    out_html.parent.mkdir(parents=True, exist_ok=True)

    sty, kar = load_layers(repo_root)
    sty_field = choose_default_field(sty)
    kar_field = choose_default_field(kar)

    m = build_map(sty=sty, kar=kar, sty_field=sty_field, kar_field=kar_field, show_kar=True)
    m.save(out_html)
    print(f"Wrote: {out_html}")


if __name__ == "__main__":
    main()
