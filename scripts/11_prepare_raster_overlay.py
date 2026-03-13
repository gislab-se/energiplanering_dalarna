from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import geopandas as gpd
import numpy as np
from PIL import Image, ImageDraw
from pyproj import Transformer

# Input rasters are local/trusted project data and can be very large.
# Disable Pillow's decompression bomb guard for this utility script.
Image.MAX_IMAGE_PIXELS = None


def _parse_world_file(path: Path) -> tuple[float, float, float, float, float, float]:
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines() if ln.strip()]
    if len(lines) < 6:
        raise RuntimeError(f"World file must contain 6 numeric rows: {path}")
    vals = [float(lines[i]) for i in range(6)]
    return vals[0], vals[1], vals[2], vals[3], vals[4], vals[5]


def _find_world_file(tif_path: Path, explicit: Path | None) -> Path:
    candidates: list[Path] = []
    if explicit is not None:
        candidates.append(explicit)
    candidates.extend(
        [
            tif_path.with_suffix(".tfw"),
            Path(str(tif_path) + ".tfw"),
            tif_path.with_suffix(".tifw"),
            tif_path.with_suffix(".wld"),
        ]
    )
    seen: set[str] = set()
    for c in candidates:
        key = str(c).lower()
        if key in seen:
            continue
        seen.add(key)
        if c.exists():
            return c
    raise FileNotFoundError(f"Could not find world file for {tif_path}")


def _pixel_to_world(
    col: float,
    row: float,
    a: float,
    d: float,
    b: float,
    e: float,
    c: float,
    f: float,
) -> tuple[float, float]:
    x = a * col + b * row + c
    y = d * col + e * row + f
    return x, y


def _world_to_pixel(
    x: float,
    y: float,
    a: float,
    d: float,
    b: float,
    e: float,
    c: float,
    f: float,
) -> tuple[float, float]:
    det = (a * e) - (b * d)
    if det == 0:
        raise RuntimeError("Invalid geotransform: determinant is zero.")
    dx = x - c
    dy = y - f
    col = ((e * dx) - (b * dy)) / det
    row = ((-d * dx) + (a * dy)) / det
    return col, row


def _bounds_4326_from_world_file(
    width: int,
    height: int,
    world_params: tuple[float, float, float, float, float, float],
    source_crs: str,
) -> list[list[float]]:
    a, d, b, e, c, f = world_params
    corners_px = [
        (-0.5, -0.5),
        (width - 0.5, -0.5),
        (width - 0.5, height - 0.5),
        (-0.5, height - 0.5),
    ]
    corners_xy = [_pixel_to_world(px, py, a, d, b, e, c, f) for px, py in corners_px]
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    corners_ll = [transformer.transform(x, y) for x, y in corners_xy]
    lons = [p[0] for p in corners_ll]
    lats = [p[1] for p in corners_ll]
    west, east = min(lons), max(lons)
    south, north = min(lats), max(lats)
    return [[south, west], [north, east]]


def _bounds_4326_for_source_window(
    window_left: float,
    window_top: float,
    window_right: float,
    window_bottom: float,
    world_params: tuple[float, float, float, float, float, float],
    source_crs: str,
) -> list[list[float]]:
    a, d, b, e, c, f = world_params
    corners_px = [
        (window_left - 0.5, window_top - 0.5),
        (window_right - 0.5, window_top - 0.5),
        (window_right - 0.5, window_bottom - 0.5),
        (window_left - 0.5, window_bottom - 0.5),
    ]
    corners_xy = [_pixel_to_world(px, py, a, d, b, e, c, f) for px, py in corners_px]
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    corners_ll = [transformer.transform(x, y) for x, y in corners_xy]
    lons = [p[0] for p in corners_ll]
    lats = [p[1] for p in corners_ll]
    return [[min(lats), min(lons)], [max(lats), max(lons)]]


def _resample_filter(name: str) -> int:
    resampling = getattr(Image, "Resampling", Image)
    if name == "nearest":
        return resampling.NEAREST
    if name == "bicubic":
        return resampling.BICUBIC
    if name == "lanczos":
        return resampling.LANCZOS
    return resampling.BILINEAR


def _prepare_mode_for_png(img: Image.Image) -> Image.Image:
    if img.mode in {"1", "L", "LA", "P", "RGB", "RGBA"}:
        return img
    if img.mode in {"F", "I", "I;16", "I;16B", "I;16L"}:
        imgf = img.convert("F")
        lo, hi = imgf.getextrema()
        if hi <= lo:
            return imgf.convert("L")
        scale = 255.0 / (hi - lo)
        shifted = imgf.point(lambda v: int(max(0, min(255, (float(v) - lo) * scale))))
        return shifted.convert("L")
    return img.convert("RGBA")


def _resize_if_needed(
    img: Image.Image,
    max_width: int,
    max_height: int,
    resample_name: str,
) -> Image.Image:
    if max_width <= 0 and max_height <= 0:
        return img
    scale_w = max_width / img.width if max_width > 0 else 1.0
    scale_h = max_height / img.height if max_height > 0 else 1.0
    scale = min(1.0, scale_w, scale_h)
    if scale >= 1.0:
        return img

    # For huge rasters, reduce in an integer step first to lower memory pressure.
    shrink = int(max(img.width / max(1, max_width), img.height / max(1, max_height)))
    if shrink >= 2:
        img = img.reduce(shrink)

    scale_w2 = max_width / img.width if max_width > 0 else 1.0
    scale_h2 = max_height / img.height if max_height > 0 else 1.0
    scale2 = min(1.0, scale_w2, scale_h2)
    if scale2 >= 1.0:
        return img
    new_size = (max(1, int(round(img.width * scale2))), max(1, int(round(img.height * scale2))))
    return img.resize(new_size, _resample_filter(resample_name))


def _target_size(width: int, height: int, max_width: int, max_height: int) -> tuple[int, int]:
    if max_width <= 0 and max_height <= 0:
        return width, height
    scale_w = max_width / width if max_width > 0 else 1.0
    scale_h = max_height / height if max_height > 0 else 1.0
    scale = min(1.0, scale_w, scale_h)
    out_w = max(1, int(round(width * scale)))
    out_h = max(1, int(round(height * scale)))
    return out_w, out_h


def _resize_tiled(
    src: Image.Image,
    max_width: int,
    max_height: int,
    resample_name: str,
    tile_out_px: int = 256,
) -> Image.Image:
    out_w, out_h = _target_size(src.width, src.height, max_width=max_width, max_height=max_height)
    if out_w == src.width and out_h == src.height:
        return _prepare_mode_for_png(src.copy())

    scale_x = src.width / out_w
    scale_y = src.height / out_h
    resample = _resample_filter(resample_name)
    out: Image.Image | None = None

    for oy in range(0, out_h, tile_out_px):
        oy2 = min(out_h, oy + tile_out_px)
        tile_h = oy2 - oy
        for ox in range(0, out_w, tile_out_px):
            ox2 = min(out_w, ox + tile_out_px)
            tile_w = ox2 - ox

            sx0 = max(0, int(math.floor(ox * scale_x)))
            sy0 = max(0, int(math.floor(oy * scale_y)))
            sx1 = min(src.width, int(math.ceil(ox2 * scale_x)))
            sy1 = min(src.height, int(math.ceil(oy2 * scale_y)))
            if sx1 <= sx0:
                sx1 = min(src.width, sx0 + 1)
            if sy1 <= sy0:
                sy1 = min(src.height, sy0 + 1)

            patch = src.crop((sx0, sy0, sx1, sy1))
            patch = _prepare_mode_for_png(patch)
            patch = patch.resize((tile_w, tile_h), resample=resample)

            if out is None:
                out = Image.new(patch.mode, (out_w, out_h))
            elif patch.mode != out.mode:
                patch = patch.convert(out.mode)

            out.paste(patch, (ox, oy))

    if out is None:
        return Image.new("L", (out_w, out_h))
    return out


def _load_clip_geometry_from_admin(admin_gpkg: Path, admin_layer: str, source_crs: str):
    if not admin_gpkg.exists():
        raise FileNotFoundError(f"Missing admin boundary gpkg: {admin_gpkg}")
    lan = gpd.read_file(admin_gpkg, layer=admin_layer)
    if lan is None or len(lan) == 0:
        raise RuntimeError(f"Empty admin boundary layer: {admin_layer}")
    if lan.crs is None:
        lan = lan.set_crs(3006)
    lan = lan.to_crs(source_crs)
    try:
        geom = lan.geometry.union_all()
    except Exception:
        geom = lan.geometry.unary_union
    if geom is None or getattr(geom, "is_empty", True):
        raise RuntimeError(f"Could not build union geometry from {admin_layer}")
    return geom


def _iter_polygons(geom):
    if geom is None or getattr(geom, "is_empty", True):
        return
    gtype = getattr(geom, "geom_type", "")
    if gtype == "Polygon":
        yield geom
        return
    if gtype == "MultiPolygon":
        for g in geom.geoms:
            if not getattr(g, "is_empty", True):
                yield g
        return
    if hasattr(geom, "geoms"):
        for g in geom.geoms:
            yield from _iter_polygons(g)


def _apply_clip_mask_and_crop(
    image: Image.Image,
    clip_geom_src_crs,
    src_width: int,
    src_height: int,
    world_params: tuple[float, float, float, float, float, float],
    source_crs: str,
    crop_to_mask_bbox: bool,
) -> tuple[Image.Image, list[list[float]]]:
    out_w, out_h = image.size
    sx_scale = src_width / out_w
    sy_scale = src_height / out_h
    a, d, b, e, c, f = world_params

    mask = Image.new("L", (out_w, out_h), 0)
    draw = ImageDraw.Draw(mask)

    def _to_out_xy(x: float, y: float) -> tuple[float, float]:
        src_col, src_row = _world_to_pixel(x, y, a, d, b, e, c, f)
        return src_col / sx_scale, src_row / sy_scale

    for poly in _iter_polygons(clip_geom_src_crs):
        ext = [_to_out_xy(x, y) for x, y in poly.exterior.coords]
        if len(ext) >= 3:
            draw.polygon(ext, fill=255)
        for ring in poly.interiors:
            hole = [_to_out_xy(x, y) for x, y in ring.coords]
            if len(hole) >= 3:
                draw.polygon(hole, fill=0)

    bbox = mask.getbbox()
    if bbox is None:
        raise RuntimeError("Clip mask produced empty result. Check CRS/layer source.")

    rgba = image.convert("RGBA")
    rgba.putalpha(mask)

    if not crop_to_mask_bbox:
        bounds = _bounds_4326_from_world_file(src_width, src_height, world_params, source_crs)
        return rgba, bounds

    left, top, right, bottom = bbox
    cropped = rgba.crop((left, top, right, bottom))

    src_left = left * sx_scale
    src_top = top * sy_scale
    src_right = right * sx_scale
    src_bottom = bottom * sy_scale
    bounds = _bounds_4326_for_source_window(
        window_left=src_left,
        window_top=src_top,
        window_right=src_right,
        window_bottom=src_bottom,
        world_params=world_params,
        source_crs=source_crs,
    )
    return cropped, bounds


def _extract_value_and_alpha(img: Image.Image) -> tuple[np.ndarray, np.ndarray | None]:
    arr = np.asarray(img)
    if arr.ndim == 2:
        return arr.astype(np.uint8), None
    if arr.ndim == 3 and arr.shape[2] == 2:
        return arr[:, :, 0].astype(np.uint8), arr[:, :, 1].astype(np.uint8)
    if arr.ndim == 3 and arr.shape[2] >= 4:
        rgb = arr[:, :, :3].astype(np.float32)
        val = np.clip(np.rint(0.299 * rgb[:, :, 0] + 0.587 * rgb[:, :, 1] + 0.114 * rgb[:, :, 2]), 0, 255).astype(np.uint8)
        return val, arr[:, :, 3].astype(np.uint8)
    if arr.ndim == 3 and arr.shape[2] == 3:
        rgb = arr.astype(np.float32)
        val = np.clip(np.rint(0.299 * rgb[:, :, 0] + 0.587 * rgb[:, :, 1] + 0.114 * rgb[:, :, 2]), 0, 255).astype(np.uint8)
        return val, None
    gray = np.asarray(img.convert("L")).astype(np.uint8)
    return gray, None


def _turbo_rgb(norm: np.ndarray) -> np.ndarray:
    x = np.clip(norm.astype(np.float32), 0.0, 1.0)
    x2 = x * x
    x3 = x2 * x
    x4 = x3 * x
    x5 = x4 * x
    r = 0.13572138 + 4.61539260 * x - 42.66032258 * x2 + 132.13108234 * x3 - 152.94239396 * x4 + 59.28637943 * x5
    g = 0.09140261 + 2.19418839 * x + 4.84296658 * x2 - 14.18503333 * x3 + 4.27729857 * x4 + 2.82956604 * x5
    b = 0.10667330 + 12.64194608 * x - 60.58204836 * x2 + 110.36276771 * x3 - 89.90310912 * x4 + 27.34824973 * x5
    rgb = np.stack([r, g, b], axis=-1)
    rgb = np.clip(np.rint(rgb * 255.0), 0, 255).astype(np.uint8)
    return rgb


def _green_rgb(norm: np.ndarray) -> np.ndarray:
    # Low values: light green. High values: dark green.
    x = np.clip(norm.astype(np.float32), 0.0, 1.0)
    low = np.array([220.0, 245.0, 225.0], dtype=np.float32)
    high = np.array([0.0, 68.0, 27.0], dtype=np.float32)
    rgb = low + (high - low) * x[..., None]
    return np.clip(np.rint(rgb), 0, 255).astype(np.uint8)


def _forest_heat_rgb(values: np.ndarray) -> np.ndarray:
    # Warm, forest-oriented ramp:
    # 1-30 muted green, 31-60 light green, 61-70 yellow,
    # 71-80 orange, 81-90 red, 91+ dark red.
    v = values.astype(np.float32)
    anchors = np.array([1.0, 30.0, 31.0, 60.0, 61.0, 70.0, 71.0, 80.0, 81.0, 90.0, 91.0, 100.0], dtype=np.float32)
    colors = np.array(
        [
            [27.0, 94.0, 32.0],
            [58.0, 121.0, 61.0],
            [102.0, 154.0, 77.0],
            [205.0, 232.0, 167.0],
            [253.0, 216.0, 53.0],
            [255.0, 235.0, 59.0],
            [251.0, 140.0, 0.0],
            [245.0, 124.0, 0.0],
            [229.0, 57.0, 53.0],
            [198.0, 40.0, 40.0],
            [127.0, 0.0, 0.0],
            [79.0, 0.0, 0.0],
        ],
        dtype=np.float32,
    )
    clipped = np.clip(v, anchors[0], anchors[-1])
    rgb = np.empty(clipped.shape + (3,), dtype=np.float32)
    for idx in range(len(anchors) - 1):
        lo = anchors[idx]
        hi = anchors[idx + 1]
        mask = (clipped >= lo) & (clipped <= hi if idx == len(anchors) - 2 else clipped < hi)
        if not np.any(mask):
            continue
        span = max(hi - lo, 1.0)
        t = ((clipped[mask] - lo) / span).astype(np.float32)
        rgb[mask] = colors[idx] + (colors[idx + 1] - colors[idx]) * t[:, None]
    return np.clip(np.rint(rgb), 0, 255).astype(np.uint8)


def _build_display_image(
    value_u8: np.ndarray,
    alpha_u8: np.ndarray | None,
    color_ramp: str,
    ramp_min: float,
    ramp_max: float,
) -> Image.Image:
    # Opacity mapping requested (custom anchors):
    # 1=>1, 2=>10, 3=>30, 4=>35, 5=>40, 6=>45, 7=>50, 8=>55, 9=>60, 10=>70.
    # For classes >10: continue +5 per class, clamped to 100%.
    v = value_u8.astype(np.float32)
    class_opacity_pct = np.where(
        v <= 0,
        0.0,
        np.where(
            v == 1,
            1.0,
            np.where(
                v == 2,
                10.0,
                np.where(
                    v == 3,
                    30.0,
                    np.where(
                        v == 4,
                        35.0,
                        np.where(
                            v == 5,
                            40.0,
                            np.where(
                                v == 6,
                                45.0,
                                np.where(
                                    v == 7,
                                    50.0,
                                    np.where(
                                        v == 8,
                                        55.0,
                                        np.where(v == 9, 60.0, np.where(v == 10, 70.0, 70.0 + (v - 10.0) * 5.0)),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),
    )
    class_opacity_pct = np.clip(class_opacity_pct, 0.0, 100.0)
    class_pct = class_opacity_pct / 100.0
    # Keep the 1-30 band visible enough to orient the viewer,
    # while still letting higher-value hotspots dominate.
    class_pct = np.where(
        (v >= 1.0) & (v <= 30.0),
        np.maximum(class_pct * 0.75, 0.08),
        class_pct,
    )
    class_alpha = np.clip(np.rint(class_pct * 255.0), 0, 255).astype(np.uint8)
    if alpha_u8 is None:
        final_alpha = class_alpha
    else:
        final_alpha = np.clip(np.rint((alpha_u8.astype(np.float32) * class_alpha.astype(np.float32)) / 255.0), 0, 255).astype(np.uint8)

    if color_ramp == "none":
        gray = value_u8.astype(np.uint8)
        rgb = np.dstack([gray, gray, gray])
        return Image.fromarray(np.dstack([rgb, final_alpha]), mode="RGBA")

    lo = float(ramp_min)
    hi = float(ramp_max)
    if hi <= lo:
        hi = lo + 1.0
    if color_ramp == "green":
        norm = (value_u8.astype(np.float32) - lo) / (hi - lo)
        rgb = _green_rgb(norm)
    elif color_ramp == "forest_heat":
        rgb = _forest_heat_rgb(value_u8.astype(np.float32))
    else:
        norm = (value_u8.astype(np.float32) - lo) / (hi - lo)
        rgb = _turbo_rgb(norm)
    return Image.fromarray(np.dstack([rgb, final_alpha]), mode="RGBA")


def _build_overlay(
    source_tif: Path,
    source_tfw: Path | None,
    source_crs: str,
    output_image: Path,
    output_json: Path,
    sample_image: Path,
    layer_name: str,
    opacity: float,
    zindex: int,
    max_width: int,
    max_height: int,
    resample: str,
    color_ramp: str,
    ramp_min: float,
    ramp_max: float,
    clip_admin_gpkg: Path | None,
    clip_admin_layer: str,
    crop_to_mask_bbox: bool,
) -> tuple[Path, Path]:
    if not source_tif.exists():
        raise FileNotFoundError(f"Missing source tif: {source_tif}")

    tfw = _find_world_file(source_tif, source_tfw)
    world_params = _parse_world_file(tfw)
    clip_geom = None
    if clip_admin_gpkg is not None:
        clip_geom = _load_clip_geometry_from_admin(clip_admin_gpkg, clip_admin_layer, source_crs)

    with Image.open(source_tif) as src:
        original_size = src.size
        bounds_4326 = _bounds_4326_from_world_file(src.width, src.height, world_params, source_crs)
        # Resize first (while still in source mode), then convert for PNG export.
        try:
            out_img = _resize_if_needed(src, max_width=max_width, max_height=max_height, resample_name=resample)
            out_img = _prepare_mode_for_png(out_img)
        except MemoryError:
            out_img = _resize_tiled(src, max_width=max_width, max_height=max_height, resample_name=resample)

        if clip_geom is not None:
            out_img, bounds_4326 = _apply_clip_mask_and_crop(
                image=out_img,
                clip_geom_src_crs=clip_geom,
                src_width=src.width,
                src_height=src.height,
                world_params=world_params,
                source_crs=source_crs,
                crop_to_mask_bbox=crop_to_mask_bbox,
            )

        value_u8, alpha_u8 = _extract_value_and_alpha(out_img)
        valid = value_u8
        if alpha_u8 is not None:
            valid = value_u8[alpha_u8 > 0]
        valid = valid[valid > 0]
        if valid.size > 0:
            raster_min = int(valid.min())
            raster_max = int(valid.max())
        else:
            raster_min, raster_max = 0, 0

        sample_image.parent.mkdir(parents=True, exist_ok=True)
        if alpha_u8 is None:
            sample_img = Image.fromarray(value_u8, mode="L")
        else:
            sample_img = Image.fromarray(np.dstack([value_u8, alpha_u8]), mode="LA")
        sample_img.save(sample_image, format="PNG", optimize=True, compress_level=9)

        display_img = _build_display_image(
            value_u8=value_u8,
            alpha_u8=alpha_u8,
            color_ramp=color_ramp,
            ramp_min=ramp_min,
            ramp_max=ramp_max,
        )
        output_image.parent.mkdir(parents=True, exist_ok=True)
        display_img.save(output_image, format="PNG", optimize=True, compress_level=9)
        output_size = display_img.size
        sample_size = sample_img.size

    payload = {
        "name": layer_name,
        "image": output_image.name,
        "sample_image": sample_image.name,
        "bounds_4326": bounds_4326,
        "opacity": float(opacity),
        "opacity_mode": "custom_anchors_with_1_30_floor_0p08_scale_0p75",
        "zindex": int(zindex),
        "color_ramp": color_ramp,
        "ramp_min": float(ramp_min),
        "ramp_max": float(ramp_max),
        "raster_min": int(raster_min),
        "raster_max": int(raster_max),
        "source_crs": source_crs,
        "source_tif": str(source_tif),
        "source_tfw": str(tfw),
        "clip_admin_gpkg": str(clip_admin_gpkg) if clip_admin_gpkg is not None else "",
        "clip_admin_layer": clip_admin_layer if clip_admin_gpkg is not None else "",
        "crop_to_mask_bbox": bool(crop_to_mask_bbox),
        "original_size_px": [int(original_size[0]), int(original_size[1])],
        "output_size_px": [int(output_size[0]), int(output_size[1])],
        "sample_size_px": [int(sample_size[0]), int(sample_size[1])],
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_image, output_json


def _args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[1]
    default_cloud = repo_root / "data" / "cloud"
    default_image = default_cloud / "tathetsanalys_3000m_procent_light.png"
    p = argparse.ArgumentParser(description="Build lightweight PNG raster overlay + metadata for Streamlit app.")
    p.add_argument("--source-tif", type=Path, required=True, help="Path to source raster image (for example GeoTIFF/PNG).")
    p.add_argument("--source-tfw", type=Path, default=None, help="Optional world file path (.tfw).")
    p.add_argument("--source-crs", type=str, default="EPSG:3006", help="CRS for world-file coordinates.")
    p.add_argument(
        "--output-image",
        type=Path,
        default=default_image,
        help="Output PNG path used by folium ImageOverlay.",
    )
    p.add_argument(
        "--sample-image",
        type=Path,
        default=default_cloud / "tathetsanalys_3000m_procent_values.png",
        help="Output grayscale value PNG used for point filtering (slider).",
    )
    p.add_argument(
        "--output-json",
        type=Path,
        default=default_cloud / "tathetsanalys_3000m_procent.overlay.json",
        help="Output JSON metadata path consumed by app.py.",
    )
    p.add_argument("--name", type=str, default="Tathetsanalys boreal region", help="Layer name shown in map controls.")
    p.add_argument("--opacity", type=float, default=1.0, help="Overlay opacity in map control.")
    p.add_argument("--zindex", type=int, default=5, help="Overlay z-index in Leaflet.")
    p.add_argument("--max-width", type=int, default=1800, help="Max output PNG width in pixels.")
    p.add_argument("--max-height", type=int, default=1800, help="Max output PNG height in pixels.")
    p.add_argument(
        "--resample",
        choices=["nearest", "bilinear", "bicubic", "lanczos"],
        default="bilinear",
        help="Resampling mode used when downscaling.",
    )
    p.add_argument(
        "--color-ramp",
        choices=["none", "turbo", "green", "forest_heat"],
        default="forest_heat",
        help="Color ramp for display image.",
    )
    p.add_argument("--ramp-min", type=float, default=1.0, help="Minimum value mapped in color ramp.")
    p.add_argument("--ramp-max", type=float, default=94.0, help="Maximum value mapped in color ramp.")
    p.add_argument(
        "--clip-admin-gpkg",
        type=Path,
        default=default_cloud / "admin_boundaries.gpkg",
        help="Optional admin GPKG used to clip to lan boundary. Set --no-clip-admin to disable.",
    )
    p.add_argument(
        "--clip-admin-layer",
        type=str,
        default="lan",
        help="Layer name in --clip-admin-gpkg used as clip geometry.",
    )
    p.add_argument(
        "--no-clip-admin",
        action="store_true",
        help="Disable clipping to admin boundary geometry.",
    )
    p.add_argument(
        "--crop-to-mask-bbox",
        action="store_true",
        help="After clip mask, crop output image to mask bbox for smaller file size.",
    )
    return p.parse_args()


def main() -> None:
    args = _args()
    out_image, out_json = _build_overlay(
        source_tif=args.source_tif,
        source_tfw=args.source_tfw,
        source_crs=args.source_crs,
        output_image=args.output_image,
        output_json=args.output_json,
        sample_image=args.sample_image,
        layer_name=args.name,
        opacity=args.opacity,
        zindex=args.zindex,
        max_width=args.max_width,
        max_height=args.max_height,
        resample=args.resample,
        color_ramp=args.color_ramp,
        ramp_min=args.ramp_min,
        ramp_max=args.ramp_max,
        clip_admin_gpkg=None if args.no_clip_admin else args.clip_admin_gpkg,
        clip_admin_layer=args.clip_admin_layer,
        crop_to_mask_bbox=args.crop_to_mask_bbox,
    )
    in_mb = args.source_tif.stat().st_size / (1024 * 1024)
    out_mb = out_image.stat().st_size / (1024 * 1024)
    sample_mb = args.sample_image.stat().st_size / (1024 * 1024)
    print(f"source={args.source_tif}")
    print(f"source_size_mb={in_mb:.2f}")
    print(f"overlay_png={out_image}")
    print(f"overlay_png_size_mb={out_mb:.2f}")
    print(f"sample_png={args.sample_image}")
    print(f"sample_png_size_mb={sample_mb:.2f}")
    print(f"overlay_json={out_json}")
    print("Done.")


if __name__ == "__main__":
    main()
