from __future__ import annotations

import argparse
from pathlib import Path

import geopandas as gpd


def _read_4326(path: Path, default_crs: int = 3006) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(default_crs)
    return gdf.to_crs(4326)


def _write_layer(gdf: gpd.GeoDataFrame, gpkg_path: Path, layer_name: str) -> None:
    gdf.to_file(gpkg_path, layer=layer_name, driver="GPKG")


def _read_topografi50_nature_layer(path: Path, simplify_m: float = 0.0) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(path, layer="skyddadnatur")
    if gdf.crs is None:
        gdf = gdf.set_crs(3006)
    if "name" not in gdf.columns:
        if "objektidentitet" in gdf.columns and "objekttyp" in gdf.columns:
            gdf["name"] = gdf["objekttyp"].astype(str).str.strip() + " " + gdf["objektidentitet"].astype(str).str.slice(0, 8)
        elif "objekttyp" in gdf.columns:
            gdf["name"] = gdf["objekttyp"].astype(str).str.strip()
    keep_cols = [c for c in ["name", "objekttyp", "djurskyddstyp", "objektidentitet", "objekttypnr"] if c in gdf.columns]
    gdf = gdf[keep_cols + [gdf.geometry.name]].copy()
    gdf = gdf.to_crs(4326)
    if simplify_m > 0:
        tmp = gdf.to_crs(3006).copy()
        tmp["geometry"] = tmp.geometry.simplify(tolerance=simplify_m, preserve_topology=True)
        gdf = tmp.to_crs(4326)
    return gdf


def build_background_bundle(cloud_dir: Path) -> Path:
    out = cloud_dir / "background_layers.gpkg"
    if out.exists():
        out.unlink()

    lan_shp = cloud_dir / "Dalarna lansgrans.shp"
    if not lan_shp.exists():
        raise FileNotFoundError(f"Missing source: {lan_shp}")

    lan = _read_4326(lan_shp, default_crs=3006)
    _write_layer(lan, out, "lan_boundary")
    return out


def build_lst_bundle(
    cloud_dir: Path,
    topografi50_gpkg: Path,
    simplify_nature_reserve_m: float = 0.0,
) -> Path:
    out = cloud_dir / "lst_layers.gpkg"
    if out.exists():
        out.unlink()

    source_by_layer = {
        "landskapstyp": cloud_dir / "lst_landskapstyper.gpkg",
        "landskapskaraktar": cloud_dir / "lst_landskapskaraktar.gpkg",
        "rorligt_friluftsliv": cloud_dir / "lst_rorligt_friluftsliv.gpkg",
        "utbyggnad_vindkraft": cloud_dir / "lst_utbyggnad_vindkraft.gpkg",
        "kulturmiljovard": cloud_dir / "lst_kulturmiljovard.gpkg",
    }

    for layer_name, src in source_by_layer.items():
        if not src.exists():
            raise FileNotFoundError(f"Missing source: {src}")
        gdf = _read_4326(src, default_crs=3006)
        _write_layer(gdf, out, layer_name)

    if not topografi50_gpkg.exists():
        raise FileNotFoundError(f"Missing source: {topografi50_gpkg}")
    nature_gdf = _read_topografi50_nature_layer(topografi50_gpkg, simplify_m=simplify_nature_reserve_m)
    _write_layer(nature_gdf, out, "nature_reserve")
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build compact Streamlit Cloud bundles from data/cloud layers.")
    p.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument(
        "--topografi50-gpkg",
        type=Path,
        default=Path("C:/gislab/data/dataraw/topologi50/naturvard_ln20.gpkg"),
        help="Path to Lantmäteriet Topografi 50 naturvard_ln20.gpkg",
    )
    p.add_argument(
        "--simplify-nature-reserve-m",
        type=float,
        default=0.0,
        help="Optional simplification tolerance in meters for nature_reserve layer.",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    cloud_dir = args.repo_root / "data" / "cloud"
    if not cloud_dir.exists():
        raise FileNotFoundError(f"Missing directory: {cloud_dir}")

    bg = build_background_bundle(cloud_dir)
    lst = build_lst_bundle(
        cloud_dir,
        topografi50_gpkg=args.topografi50_gpkg,
        simplify_nature_reserve_m=args.simplify_nature_reserve_m,
    )
    print(f"wrote={bg}")
    print(f"wrote={lst}")
    print("Done. App will prefer these bundles if present.")


if __name__ == "__main__":
    main()
