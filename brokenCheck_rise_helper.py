from __future__ import annotations

import io
import os
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd

XLF_EXTENSIONS = (".xlf", ".xliff", ".xml")
EXCEL_EXTENSIONS = (".xlsx", ".xlsm", ".xltx", ".xltm")
REPORT_PREFIX = "reporte_rise_"

def _is_rise_report_excel(name: str) -> bool:
    p = Path(name)
    return p.suffix.lower() in EXCEL_EXTENSIONS and p.name.lower().startswith(REPORT_PREFIX)

def _read_excel_bytes(data: bytes, source_name: str) -> pd.DataFrame:
    bio = io.BytesIO(data)
    try:
        df = pd.read_excel(bio)
    except Exception as e:
        raise RuntimeError(f"No se pudo leer el Excel '{source_name}': {e}") from e

    if df is None:
        return pd.DataFrame()
    return df.copy()

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _collect_text(elem: ET.Element) -> str:
    parts: List[str] = []
    for txt in elem.itertext():
        t = (txt or "").strip()
        if t:
            parts.append(t)
    return " ".join(parts).strip()


def parse_xliff_bytes(data: bytes, file_name: str) -> str:
    try:
        root = ET.fromstring(data)
    except Exception as e:
        raise RuntimeError(f"No se pudo parsear '{file_name}': {e}") from e

    texts: List[str] = []

    for elem in root.iter():
        tag = _strip_ns(elem.tag).lower()
        if tag in {"source", "target", "seg-source"}:
            txt = _collect_text(elem)
            if txt:
                texts.append(txt)

    # fallback muy robusto por si no hay tags esperados
    if not texts:
        all_text = _collect_text(root)
        if all_text:
            texts.append(all_text)

    # limpieza ligera
    cleaned: List[str] = []
    seen = set()
    for t in texts:
        t2 = re.sub(r"\s+", " ", t).strip()
        if t2 and t2 not in seen:
            cleaned.append(t2)
            seen.add(t2)

    return "\n".join(cleaned).strip()

def _write_txt(text: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(text, encoding="utf-8")
    return out_path

def _normalize_report_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["curso_nombre", "archivo_xliff", "url"])

    out = df.copy()
    out.columns = [str(c).strip() for c in out.columns]

    # normalización mínima robusta
    rename_map = {}
    for c in out.columns:
        c_low = c.lower().strip()
        if c_low == "curso_nombre":
            rename_map[c] = "curso_nombre"
        elif c_low == "archivo_xliff":
            rename_map[c] = "archivo_xliff"
        elif c_low == "url":
            rename_map[c] = "url"

    out = out.rename(columns=rename_map)

    for col in ("curso_nombre", "archivo_xliff", "url"):
        if col not in out.columns:
            out[col] = ""

    out = out[["curso_nombre", "archivo_xliff", "url"]].copy()
    out = out.fillna("")
    return out

def _write_unified_excel(report_df: pd.DataFrame, out_path: Path, warnings: Sequence[str], stats: Dict[str, Any]) -> Path:
    summary_rows = [
        {"Concepto": "Fecha de generación", "Valor": datetime.now().strftime("%d/%m/%Y %H:%M:%S")},
        {"Concepto": "ZIP procesados", "Valor": stats.get("zip_count", 0)},
        {"Concepto": "XLF encontrados", "Valor": stats.get("xlf_count", 0)},
        {"Concepto": "TXT generados", "Valor": stats.get("txt_count", 0)},
        {"Concepto": "Reportes encontrados", "Valor": stats.get("report_count", 0)},
        {"Concepto": "Advertencias", "Valor": " | ".join(warnings) if warnings else "Sin advertencias"},
    ]
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        if report_df.empty:
            pd.DataFrame(columns=["curso_nombre", "archivo_xliff", "url"]).to_excel(
                writer, index=False, sheet_name="Reporte_Unificado"
            )
        else:
            report_df.to_excel(writer, index=False, sheet_name="Reporte_Unificado")
        pd.DataFrame(summary_rows).to_excel(writer, index=False, sheet_name="Resumen_Proceso")
    return out_path


def _bundle_outputs(txt_paths: Sequence[str], excel_path: Path, out_zip: Path) -> Path:
    with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for txt_path in txt_paths:
            p = Path(txt_path)
            if p.exists():
                zf.write(p, arcname=f"txt/{p.name}")
        if excel_path.exists():
            zf.write(excel_path, arcname=excel_path.name)
    return out_zip

def process_rise_zip_uploads(
    zip_uploads: Sequence[Tuple[str, bytes]],
    work_dir: str,
) -> Dict[str, Any]:
    work_path = Path(work_dir)
    txt_dir = work_path / "rise_txt"
    txt_dir.mkdir(parents=True, exist_ok=True)

    warnings: List[str] = []
    report_frames: List[pd.DataFrame] = []
    txt_paths: List[str] = []
    txt_meta: Dict[str, Dict[str, Any]] = {}

    extracted_items: List[Dict[str, Any]] = []
    xlf_count = 0
    report_count = 0

    for zip_name, zip_bytes in zip_uploads:
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
                inner_names = zf.namelist()

                xlf_items = [
                    n for n in inner_names
                    if (not n.endswith("/")) and Path(n).suffix.lower() in XLF_EXTENSIONS
                ]
                report_items = [
                    n for n in inner_names
                    if (not n.endswith("/")) and _is_rise_report_excel(Path(n).name)
                ]

                if not xlf_items:
                    warnings.append(f"No se encontraron XLF/XLIFF/XML en {zip_name}")

                report_frames_zip: List[pd.DataFrame] = []

                for report_item in report_items:
                    try:
                        df_rep = _read_excel_bytes(zf.read(report_item), f"{zip_name}::{report_item}")
                        df_rep = _normalize_report_columns(df_rep)
                        if not df_rep.empty:
                            df_rep["__zip_name"] = zip_name
                            df_rep["__report_name"] = Path(report_item).name
                            report_frames.append(df_rep)
                            report_frames_zip.append(df_rep)
                            report_count += 1
                    except Exception as e:
                        warnings.append(f"No se pudo leer reporte '{report_item}' en {zip_name}: {e}")

                report_df_zip = pd.concat(report_frames_zip, ignore_index=True) if report_frames_zip else pd.DataFrame(
                    columns=["curso_nombre", "archivo_xliff", "url"]
                )

                report_lookup = {}
                if not report_df_zip.empty:
                    for _, rr in report_df_zip.iterrows():
                        key = str(rr.get("archivo_xliff", "")).strip().lower()
                        if key and key not in report_lookup:
                            report_lookup[key] = {
                                "curso_nombre": str(rr.get("curso_nombre", "")).strip(),
                                "archivo_xliff": str(rr.get("archivo_xliff", "")).strip(),
                                "url": str(rr.get("url", "")).strip(),
                            }

                for idx, xlf_item in enumerate(xlf_items, start=1):
                    try:
                        raw_name = Path(xlf_item).name
                        xlf_bytes = zf.read(xlf_item)
                        text = parse_xliff_bytes(xlf_bytes, raw_name)

                        if not text.strip():
                            warnings.append(f"XLF sin texto útil en {zip_name}: {raw_name}")
                            continue

                        txt_name = f"{Path(raw_name).stem}.txt"
                        txt_path = txt_dir / txt_name
                        _write_txt(text, txt_path)

                        meta_rep = report_lookup.get(raw_name.strip().lower(), {})
                        txt_meta[str(txt_path)] = {
                            "display_name": txt_name,
                            "origin": "upload_zip_rise",
                            "source_url": "",
                            "Archivo": meta_rep.get("archivo_xliff", raw_name),
                            "name": meta_rep.get("curso_nombre", ""),
                            "link_class": meta_rep.get("url", ""),
                            "zip_name": zip_name,
                            "archivo_xliff": meta_rep.get("archivo_xliff", raw_name),
                            "curso_nombre": meta_rep.get("curso_nombre", ""),
                            "url": meta_rep.get("url", ""),
                        }
                        txt_paths.append(str(txt_path))
                        extracted_items.append({"txt_path": str(txt_path), "meta": txt_meta[str(txt_path)]})
                        xlf_count += 1
                    except Exception as e:
                        warnings.append(f"No se pudo procesar XLF '{xlf_item}' en {zip_name}: {e}")

        except Exception as e:
            warnings.append(f"No se pudo abrir ZIP '{zip_name}': {e}")

    report_df = pd.concat(report_frames, ignore_index=True) if report_frames else pd.DataFrame(
        columns=["curso_nombre", "archivo_xliff", "url"]
    )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unified_excel_path = work_path / f"rise_reporte_unificado_{timestamp}.xlsx"
    bundle_zip_path = work_path / f"rise_txt_bundle_{timestamp}.zip"

    stats = {
        "zip_count": len(zip_uploads),
        "xlf_count": xlf_count,
        "txt_count": len(txt_paths),
        "report_count": report_count,
    }

    _write_unified_excel(report_df, unified_excel_path, warnings, stats)
    _bundle_outputs(txt_paths, unified_excel_path, bundle_zip_path)

    return {
        "txt_paths": txt_paths,
        "txt_meta": txt_meta,
        "report_df": report_df,
        "bundle_zip_path": str(bundle_zip_path),
        "unified_excel_path": str(unified_excel_path),
        "warnings": warnings,
        "stats": stats,
    }

def _extract_links_from_txt_rise_bytes(
    txt_bytes: bytes,
    filename: str,
    *,
    meta: Optional[Dict[str, Any]] = None,
    url_extractor: Optional[Callable[[str], List[str]]] = None,
) -> List[Dict[str, Any]]:
    meta = meta or {}
    text = txt_bytes.decode("utf-8", errors="replace")

    if url_extractor is None:
        regex = re.compile(r"https?://[^\s<>\"]+")
        links = regex.findall(text)
    else:
        links = url_extractor(text)

    rows: List[Dict[str, Any]] = []
    seen = set()

    for link in links:
        link_clean = str(link).strip()
        if not link_clean:
            continue
        key = link_clean.lower()
        if key in seen:
            continue
        seen.add(key)

        rows.append(
            {
                "Nombre del Archivo": filename,
                "Archivo": meta.get("archivo_xliff") or meta.get("Archivo") or filename,
                "name": meta.get("curso_nombre") or meta.get("name") or "",
                "link_class": meta.get("url") or meta.get("link_class") or "",
                "Página/Diapositiva": "",
                "Links": link_clean,
                "source_url": "",
            }
        )

    return rows

def run_rise_txt_link_report_streamlit(
    txt_paths: List[str],
    *,
    txt_meta: Optional[Dict[str, Dict[str, Any]]] = None,
    progress_bar=None,
    status_text=None,
    url_extractor: Optional[Callable[[str], List[str]]] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    txt_meta = txt_meta or {}
    all_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    total = len(txt_paths)
    for idx, path in enumerate(txt_paths, start=1):
        p = Path(path)
        meta = txt_meta.get(str(path), {})

        if status_text is not None:
            status_text.markdown(f"Analizando Rise TXT **{idx}/{total}** · `{p.name}`")

        try:
            with open(p, "rb") as fh:
                data = fh.read()
            rows = _extract_links_from_txt_rise_bytes(
                data,
                p.name,
                meta=meta,
                url_extractor=url_extractor,
            )
            all_rows.extend(rows)
        except Exception as e:
            errors.append({"Archivo": p.name, "Error": str(e)})

    if all_rows:
        df = pd.DataFrame(all_rows)
        df = df.sort_values(["Nombre del Archivo", "Página/Diapositiva", "Links"]).reset_index(drop=True)
    else:
        df = pd.DataFrame(
            columns=[
                "Nombre del Archivo",
                "Archivo",
                "name",
                "link_class",
                "Página/Diapositiva",
                "Links",
                "source_url",
            ]
        )

    return df, errors

