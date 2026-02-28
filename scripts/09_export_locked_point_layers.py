from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.map_factory import _db_settings


def _read_gdf(con, sql: str) -> gpd.GeoDataFrame:
    return gpd.read_postgis(sql, con, geom_col="geom")


def main() -> None:
    cfg = _db_settings()
    con = psycopg2.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )
    try:
        plats_1 = _read_gdf(
            con,
            """
            SELECT
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod,
              p.admin_2, p.plats_fritext, p.lat, p.lon,
              ST_Transform(p.geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats1_geom_3006 p
            """,
        )
        plats_2 = _read_gdf(
            con,
            """
            SELECT
              p.qgis_id, p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod,
              p.admin_2, p.plats_fritext, p.lat, p.lon,
              ST_Transform(p.geom, 4326)::geometry(Point, 4326) AS geom
            FROM novus.v_plats2_geom_3006 p
            """,
        )

        # Plats 3: extra känslig enligt rapportflöde (Q10/Q11).
        # - En plats svarad: Q10 = 1 => Plats 1
        # - Två platser svarade: Q11 = 1 => Plats 1, Q11 = 2 => Plats 2
        plats_3 = _read_gdf(
            con,
            """
            WITH pts AS (
              SELECT
                p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2,
                p.plats_fritext, p.lat, p.lon,
                ST_Transform(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 3006)::geometry(Point, 3006) AS geom
              FROM novus.v_points_valid p
            ),
            grp AS (
              SELECT
                record, respid, kommungrupp,
                COUNT(*) AS n_pts,
                MAX(CASE WHEN plats_nr = 1 THEN 1 ELSE 0 END) AS has_p1
              FROM pts
              GROUP BY 1,2,3
            ),
            n AS (
              SELECT
                record, respid, kommungrupp,
                btrim(coalesce(q10::text, '')) AS q10,
                btrim(coalesce(q11::text, '')) AS q11
              FROM novus.novus_full_dataframe
            )
            SELECT
              row_number() OVER () AS qgis_id,
              p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2,
              p.plats_fritext, p.lat, p.lon,
              ST_Transform(p.geom, 4326)::geometry(Point, 4326) AS geom
            FROM pts p
            JOIN grp g
              ON g.record = p.record
             AND g.respid = p.respid
             AND g.kommungrupp = p.kommungrupp
            JOIN n
              ON n.record = p.record
             AND n.respid = p.respid
             AND n.kommungrupp = p.kommungrupp
            WHERE (
              g.n_pts = 1
              AND g.has_p1 = 1
              AND p.plats_nr = 1
              AND n.q10 = '1'
            )
            OR (
              g.n_pts >= 2
              AND p.plats_nr = 1
              AND n.q11 = '1'
            )
            OR (
              g.n_pts >= 2
              AND p.plats_nr = 2
              AND n.q11 = '2'
            )
            """,
        )

        # Plats 4: inte känslig enligt rapportflöde.
        # - En plats svarad: Q10 = 97 (inte extra känslig)
        # - Två platser svarade: Q11 = 97 (ingen av platserna extra känslig)
        # För två-platser väljs plats 1 som representativ punkt (en punkt per respondent).
        plats_4 = _read_gdf(
            con,
            """
            WITH pts AS (
              SELECT
                p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2,
                p.plats_fritext, p.lat, p.lon,
                ST_Transform(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 3006)::geometry(Point, 3006) AS geom
              FROM novus.v_points_valid p
            ),
            grp AS (
              SELECT
                record, respid, kommungrupp,
                COUNT(*) AS n_pts,
                MAX(CASE WHEN plats_nr = 1 THEN 1 ELSE 0 END) AS has_p1
              FROM pts
              GROUP BY 1,2,3
            ),
            n AS (
              SELECT
                record, respid, kommungrupp,
                btrim(coalesce(q10::text, '')) AS q10,
                btrim(coalesce(q11::text, '')) AS q11
              FROM novus.novus_full_dataframe
            )
            SELECT
              row_number() OVER () AS qgis_id,
              p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2,
              p.plats_fritext, p.lat, p.lon,
              ST_Transform(p.geom, 4326)::geometry(Point, 4326) AS geom
            FROM pts p
            JOIN grp g
              ON g.record = p.record
             AND g.respid = p.respid
             AND g.kommungrupp = p.kommungrupp
            JOIN n
              ON n.record = p.record
             AND n.respid = p.respid
             AND n.kommungrupp = p.kommungrupp
            WHERE g.n_pts = 1
              AND g.has_p1 = 1
              AND p.plats_nr = 1
              AND n.q10 = '97'
            UNION ALL
            SELECT
              row_number() OVER () AS qgis_id,
              p.record, p.respid, p.kommungrupp, p.plats_nr, p.kommunkod, p.admin_2,
              p.plats_fritext, p.lat, p.lon,
              ST_Transform(p.geom, 4326)::geometry(Point, 4326) AS geom
            FROM pts p
            JOIN grp g
              ON g.record = p.record
             AND g.respid = p.respid
             AND g.kommungrupp = p.kommungrupp
            JOIN n
              ON n.record = p.record
             AND n.respid = p.respid
             AND n.kommungrupp = p.kommungrupp
            WHERE g.n_pts >= 2
              AND p.plats_nr = 1
              AND n.q11 = '97'
            """,
        )
    finally:
        con.close()

    out_dir = Path("data/processed/locked_layers")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_gpkg = out_dir / "novus_locked_points.gpkg"

    if out_gpkg.exists():
        out_gpkg.unlink()

    plats_1.to_file(out_gpkg, layer="plats_1", driver="GPKG")
    plats_2.to_file(out_gpkg, layer="plats_2", driver="GPKG")
    plats_3.to_file(out_gpkg, layer="plats_3_sensitive", driver="GPKG")
    plats_4.to_file(out_gpkg, layer="plats_4_not_sensitive", driver="GPKG")

    summary = pd.DataFrame(
        [
            {"layer": "plats_1", "n_points": len(plats_1)},
            {"layer": "plats_2", "n_points": len(plats_2)},
            {"layer": "plats_3_sensitive", "n_points": len(plats_3)},
            {"layer": "plats_4_not_sensitive", "n_points": len(plats_4)},
        ]
    )
    summary_path = out_dir / "novus_locked_points_summary.csv"
    summary.to_csv(summary_path, index=False)

    print(f"wrote={out_gpkg}")
    print(f"wrote={summary_path}")
    print(summary.to_string(index=False))


if __name__ == "__main__":
    main()
