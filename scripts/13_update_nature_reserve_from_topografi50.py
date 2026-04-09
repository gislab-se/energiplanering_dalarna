from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import geopandas as gpd


@dataclass(frozen=True)
class LayerSpec:
    gpkg: Path
    layer: str


def _read_vector(path: Path, layer: str | None = None, default_crs: int = 3006) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(default_crs)
    return gdf


def _clip_polygons(gdf: gpd.GeoDataFrame, mask: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if gdf is None or len(gdf) == 0 or mask is None or len(mask) == 0:
        return gdf
    if gdf.crs is None:
        gdf = gdf.set_crs(3006)
    if mask.crs is None:
        mask = mask.set_crs(3006)
    if gdf.crs != mask.crs:
        mask = mask.to_crs(gdf.crs)
    try:
        out = gpd.clip(gdf, mask)
    except Exception:
        geom = mask.geometry.union_all() if hasattr(mask.geometry, "union_all") else mask.geometry.unary_union
        out = gdf[gdf.geometry.intersects(geom)].copy()
    if out is None or len(out) == 0:
        return out
    gtype = out.geometry.geom_type.astype(str)
    keep = gtype.isin(["Polygon", "MultiPolygon"])
    return out[keep].copy()


def _simplify_m(gdf: gpd.GeoDataFrame, tolerance_m: float) -> gpd.GeoDataFrame:
    if gdf is None or len(gdf) == 0 or tolerance_m <= 0:
        return gdf
    if gdf.crs is None:
        gdf = gdf.set_crs(3006)
    tmp = gdf.to_crs(3006).copy()
    tmp["geometry"] = tmp.geometry.simplify(tolerance=float(tolerance_m), preserve_topology=True)
    return tmp.to_crs(gdf.crs)


def _build_nature_reserve_layer(
    naturvard: LayerSpec,
    dalarna_mask: LayerSpec | None,
    objekttyper: list[str],
    simplify_m: float,
) -> gpd.GeoDataFrame:
    src = _read_vector(naturvard.gpkg, layer=naturvard.layer, default_crs=3006)
    if "objekttyp" not in src.columns:
        raise KeyError(f"Missing expected column 'objekttyp' in {naturvard.gpkg} layer={naturvard.layer}")

    wanted = {str(x).strip() for x in objekttyper if str(x).strip()}
    if wanted:
        out = src[src["objekttyp"].astype(str).isin(wanted)].copy()
        if out is None or len(out) == 0:
            raise RuntimeError(f"No features matched objekttyper={sorted(wanted)} in {naturvard.gpkg} layer={naturvard.layer}")
    else:
        out = src.copy()

    if dalarna_mask is not None:
        mask = _read_vector(dalarna_mask.gpkg, layer=dalarna_mask.layer, default_crs=3006)
        out = _clip_polygons(out, mask)
    out = _simplify_m(out, simplify_m)

    # Create a stable popup field for the app even if the source lacks real names.
    if "name" not in out.columns:
        if "objektidentitet" in out.columns:
            out["name"] = out["objekttyp"].astype(str).str.strip() + " " + out["objektidentitet"].astype(str).str.slice(0, 8)
        else:
            out["name"] = out["objekttyp"].astype(str).str.strip()

    keep_cols = [c for c in ["name", "objekttyp", "djurskyddstyp", "objektidentitet", "objekttypnr"] if c in out.columns]
    keep_cols = keep_cols + [out.geometry.name]
    out = out[keep_cols].copy()

    return out.to_crs(4326)


def _list_gpkg_layers(path: Path) -> list[str]:
    if hasattr(gpd, "list_layers"):
        layers_df = gpd.list_layers(path)
        if "name" in layers_df.columns:
            return [str(x) for x in layers_df["name"].tolist()]
    raise RuntimeError(f"Could not list layers in: {path}")


def _write_gpkg_atomic(gpkg_path: Path, layers_in_order: list[tuple[str, gpd.GeoDataFrame]]) -> None:
    gpkg_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = gpkg_path.with_name(f"{gpkg_path.stem}.tmp.gpkg")

    if tmp_path.exists():
        tmp_path.unlink()

    try:
        first = True
        for layer_name, gdf in layers_in_order:
            if first:
                gdf.to_file(tmp_path, layer=layer_name, driver="GPKG")
                first = False
            else:
                gdf.to_file(tmp_path, layer=layer_name, driver="GPKG", mode="a")
        tmp_path.replace(gpkg_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Replace `nature_reserve` in data/cloud/lst_layers.gpkg using Lantmäteriet Topografi 50 "
            "(naturvard_ln20.gpkg, layer skyddadnatur). Uses the full skyddadnatur layer by default and "
            "makes a timestamped backup by default."
        )
    )
    p.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument(
        "--topografi50-gpkg",
        type=Path,
        default=Path("C:/gislab/data/dataraw/topologi50/naturvard_ln20.gpkg"),
        help="Path to Lantmäteriet Topografi 50 naturvard_ln20.gpkg",
    )
    p.add_argument("--topografi50-layer", type=str, default="skyddadnatur")
    p.add_argument(
        "--objekttyper",
        type=str,
        default="",
        help="Comma-separated objekttyp values to keep. Leave empty to keep all skyddadnatur polygon types.",
    )
    p.add_argument("--admin-gpkg", type=Path, default=None, help="Default: data/cloud/admin_boundaries.gpkg")
    p.add_argument("--admin-layer", type=str, default="lan", help="Layer to use as clip mask when --clip-to-admin is set.")
    p.add_argument("--clip-to-admin", action="store_true", help="Clip the output to the chosen admin layer.")
    p.add_argument("--lst-bundle", type=Path, default=None, help="Default: data/cloud/lst_layers.gpkg")
    p.add_argument("--output", type=Path, default=None, help="Write updated bundle here instead of in-place replace.")
    p.add_argument("--no-backup", action="store_true", help="Skip backup when updating in place.")
    p.add_argument("--simplify-m", type=float, default=0.0, help="Optional simplification tolerance in meters.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root

    admin_gpkg = args.admin_gpkg or (repo_root / "data" / "cloud" / "admin_boundaries.gpkg")
    lst_bundle = args.lst_bundle or (repo_root / "data" / "cloud" / "lst_layers.gpkg")

    if not lst_bundle.exists():
        raise FileNotFoundError(f"Missing bundle: {lst_bundle}")
    if args.clip_to_admin and not admin_gpkg.exists():
        raise FileNotFoundError(f"Missing admin bundle: {admin_gpkg}")
    if not args.topografi50_gpkg.exists():
        raise FileNotFoundError(f"Missing Topografi 50 source: {args.topografi50_gpkg}")

    nature_reserve = _build_nature_reserve_layer(
        naturvard=LayerSpec(args.topografi50_gpkg, args.topografi50_layer),
        dalarna_mask=LayerSpec(admin_gpkg, args.admin_layer) if args.clip_to_admin else None,
        objekttyper=[x.strip() for x in args.objekttyper.split(",")],
        simplify_m=float(args.simplify_m),
    )
    print(f"Loaded Topo50 nature_reserve: rows={len(nature_reserve)} crs={nature_reserve.crs}")

    # Copy all layers, replacing only nature_reserve.
    layer_names = _list_gpkg_layers(lst_bundle)
    if "nature_reserve" not in set(layer_names):
        raise RuntimeError(f"{lst_bundle} is missing required layer: nature_reserve")

    layers_out: list[tuple[str, gpd.GeoDataFrame]] = []
    for layer in layer_names:
        if layer == "nature_reserve":
            layers_out.append(("nature_reserve", nature_reserve))
            continue
        layers_out.append((layer, _read_vector(lst_bundle, layer=layer, default_crs=4326)))

    out_path = args.output or lst_bundle
    if args.output is None and not args.no_backup:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = lst_bundle.with_name(f"{lst_bundle.stem}.bak_{ts}{lst_bundle.suffix}")
        backup.write_bytes(lst_bundle.read_bytes())
        print(f"Backup written: {backup}")

    _write_gpkg_atomic(out_path, layers_out)
    print(f"Updated bundle written: {out_path}")


if __name__ == "__main__":
    main()
