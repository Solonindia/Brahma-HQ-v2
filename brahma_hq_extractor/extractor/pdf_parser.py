"""PDF datasheet extractor.

Goals (Phase C+):
1) Extract as many fields as possible from the datasheet.
2) Never lose information: if we can't confidently parse a value into a dedicated
   field, still persist it as raw text / generic key-value pairs.
3) Avoid NULLs downstream: return empty strings for missing fields; Publisher
   will also coerce None -> "".

This module intentionally returns MOST values as strings. This ensures:
- "mixed" values (e.g., "45±2 °C", "1500 VDC (IEC)", "80±5 %") are preserved
  exactly as they appear.
- Downstream SQLite can store everything as TEXT while still allowing numeric
  parsing later if needed.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from io import BytesIO
from typing import Dict, List, Tuple

import pdfplumber


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _norm_key(label: str) -> str:
    """Convert any label into a safe sqlite column name."""
    s = (label or "").strip().lower()
    s = s.replace("%", " pct ").replace("°", " deg ")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "unnamed"
    # sqlite keyword safety (minimal)
    if s in {"select", "from", "where", "table", "group", "order"}:
        s = f"f_{s}"
    return s


def _first_match(text: str, patterns: List[str]) -> str:
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            # Some patterns intentionally match without a capturing group.
            # If there is no group(1), fall back to the entire match.
            grp = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
            return _norm_ws(grp)
    return ""


def _extract_kv_from_lines(text: str) -> Dict[str, str]:
    """Heuristic key/value extraction from plain text lines.

Works well for lines like:
  "Maximum System Voltage 1500 VDC (IEC)"
  "Nominal Operating Cell Temperature -NOCT 45±2 °C"
"""
    out: Dict[str, str] = {}
    for line in (text or "").splitlines():
        l = _norm_ws(line)
        if not l or len(l) < 6:
            continue

        # common separators
        if ":" in l:
            left, right = l.split(":", 1)
        elif "  " in l:
            # split on 2+ spaces (label  value)
            parts = re.split(r"\s{2,}", l, maxsplit=1)
            if len(parts) != 2:
                continue
            left, right = parts
        else:
            continue

        left = _norm_ws(left)
        right = _norm_ws(right)
        if len(left) < 3 or len(right) < 1:
            continue

        k = _norm_key(left)
        # don't overwrite an earlier richer value
        if k not in out or (out[k] == "" and right != ""):
            out[k] = right
    return out


def _extract_kv_from_tables(tables: List[List[List[str]]]) -> Dict[str, str]:
    """Extract key/value pairs from any 2-column-like tables."""
    out: Dict[str, str] = {}
    for table in tables or []:
        for row in table or []:
            if not row:
                continue
            # flatten
            cells = [(_norm_ws(c) if c is not None else "") for c in row]
            cells = [c for c in cells if c != ""]
            if len(cells) < 2:
                continue
            left, right = cells[0], cells[1]
            if len(left) < 3 or len(right) < 1:
                continue
            k = _norm_key(left)
            if k not in out or (out[k] == "" and right != ""):
                out[k] = right
    return out


def _parse_row_numbers(line: str, expected: int = 5) -> List[str]:
    nums = re.findall(r"\d+\.\d+|\d+", line or "")
    return nums[-expected:] if len(nums) >= expected else []


def _parse_jinko_like_variants(text: str) -> List[Dict[str, str]]:
    """Parse datasheets that show STC values in a single line per metric.

Example:
  Maximum Power - Pmax 570 575 580 585 590
  Maximum Power Voltage - Vmp 43.58 ...
"""
    lines = [_norm_ws(l) for l in (text or "").splitlines() if _norm_ws(l)]

    def find_line(prefix: str) -> str:
        for l in lines:
            if prefix.lower() in l.lower():
                return l
        return ""

    pmax = _parse_row_numbers(find_line("Maximum Power - Pmax"), 5)
    if not pmax:
        return []

    vmp = _parse_row_numbers(find_line("Maximum Power Voltage - Vmp"), 5)
    imp = _parse_row_numbers(find_line("Maximum Power Current - Imp"), 5)
    voc = _parse_row_numbers(find_line("Open-circuit Voltage - Voc"), 5)
    isc = _parse_row_numbers(find_line("Short-circuit Current - Isc"), 5)

    eff_line = find_line("Module Efficiency STC")
    eff = _parse_row_numbers(eff_line, 5)

    # NOCT block often appears as:
    # Specifications (NOCT)
    # 430 433 437 441 445
    # 40.56 ...
    noct_vals = {"noct_pmax_w": [], "noct_vmp_v": [], "noct_imp_a": [], "noct_voc_v": [], "noct_isc_a": []}
    try:
        i = next(i for i, l in enumerate(lines) if l.strip() == "Specifications (NOCT)")
        noct_vals["noct_pmax_w"] = _parse_row_numbers(lines[i + 1], 5)
        noct_vals["noct_vmp_v"] = _parse_row_numbers(lines[i + 2], 5)
        noct_vals["noct_imp_a"] = _parse_row_numbers(lines[i + 3], 5)
        noct_vals["noct_voc_v"] = _parse_row_numbers(lines[i + 4], 5)
        noct_vals["noct_isc_a"] = _parse_row_numbers(lines[i + 5], 5)
    except StopIteration:
        pass
    except Exception:
        # ignore, keep empty
        pass

    variants: List[Dict[str, str]] = []
    for idx, p in enumerate(pmax):
        variants.append(
            {
                "pmax_w": str(p),
                "vmp_v": (vmp[idx] if idx < len(vmp) else ""),
                "imp_a": (imp[idx] if idx < len(imp) else ""),
                "voc_v": (voc[idx] if idx < len(voc) else ""),
                "isc_a": (isc[idx] if idx < len(isc) else ""),
                "efficiency_pct": (eff[idx] if idx < len(eff) else ""),
                "noct_pmax_w": (noct_vals["noct_pmax_w"][idx] if idx < len(noct_vals["noct_pmax_w"]) else ""),
                "noct_vmp_v": (noct_vals["noct_vmp_v"][idx] if idx < len(noct_vals["noct_vmp_v"]) else ""),
                "noct_imp_a": (noct_vals["noct_imp_a"][idx] if idx < len(noct_vals["noct_imp_a"]) else ""),
                "noct_voc_v": (noct_vals["noct_voc_v"][idx] if idx < len(noct_vals["noct_voc_v"]) else ""),
                "noct_isc_a": (noct_vals["noct_isc_a"][idx] if idx < len(noct_vals["noct_isc_a"]) else ""),
            }
        )
    return variants


def _parse_renewsys_like_variants(text: str) -> List[Dict[str, str]]:
    """Parse datasheets that list STC variants as repeating blocks:
    e.g. "600 39.38 15.25 46.95 15.99 22.24".
    """
    # Pmax Vmp Imp Voc Isc Eff
    pat = re.compile(r"(?<!\d)(\d{3})\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)\s+(\d+\.\d+)")
    rows: Dict[str, Tuple[str, str, str, str, str]] = {}
    for p, vmp, imp, voc, isc, eff in pat.findall(text or ""):
        # filter out obvious non-STC numeric junk
        try:
            p_i = int(p)
            vmp_f = float(vmp)
            voc_f = float(voc)
            eff_f = float(eff)
        except Exception:
            continue
        if not (500 <= p_i <= 800 and 20 <= vmp_f <= 80 and 30 <= voc_f <= 90 and 10 <= eff_f <= 30):
            continue
        rows[p] = (vmp, imp, voc, isc, eff)

    # Keep only the first "nominal" band if there are many (e.g. bifacial gain rows).
    keys = sorted(rows.keys(), key=lambda x: int(x))
    if not keys:
        return []

    # If this is a 600-625 sheet, keep 600-625; otherwise keep the lowest 6 variants.
    keys_600 = [k for k in keys if 600 <= int(k) <= 625]
    use_keys = keys_600 if keys_600 else keys[:6]

    # Try to parse NOCT block (often includes pmax/vmp/imp only)
    noct_map: Dict[str, Dict[str, str]] = {}
    lines = [_norm_ws(l) for l in (text or "").splitlines() if _norm_ws(l)]
    for i, l in enumerate(lines):
        if "noct" in l.lower() and "@800" in l.lower():
            powers = re.findall(r"\b(\d{3})\b", l)
            # next 3 lines typically: noct_pmax, noct_vmp, noct_imp
            if i + 3 < len(lines):
                pmax_noct = re.findall(r"\d+\.\d+", lines[i + 1])
                vmp_noct = re.findall(r"\d+\.\d+", lines[i + 2])
                imp_noct = re.findall(r"\d+\.\d+", lines[i + 3])
                for j, pwr in enumerate(powers[: len(pmax_noct)]):
                    noct_map[pwr] = {
                        "noct_pmax_w": pmax_noct[j] if j < len(pmax_noct) else "",
                        "noct_vmp_v": vmp_noct[j] if j < len(vmp_noct) else "",
                        "noct_imp_a": imp_noct[j] if j < len(imp_noct) else "",
                    }
            break

    variants: List[Dict[str, str]] = []
    for p in use_keys:
        vmp, imp, voc, isc, eff = rows[p]
        noct = noct_map.get(p, {})
        variants.append(
            {
                "pmax_w": str(p),
                "vmp_v": vmp,
                "imp_a": imp,
                "voc_v": voc,
                "isc_a": isc,
                "efficiency_pct": eff,
                "noct_pmax_w": noct.get("noct_pmax_w", ""),
                "noct_vmp_v": noct.get("noct_vmp_v", ""),
                "noct_imp_a": noct.get("noct_imp_a", ""),
                "noct_voc_v": noct.get("noct_voc_v", ""),
                "noct_isc_a": noct.get("noct_isc_a", ""),
            }
        )
    return variants


def extract_parameters(pdf_bytes: bytes) -> Dict[str, object]:
    """Return a FLAT dict of extracted fields + variants.

    Output keys:
      - many top-level keys that are safe sqlite column names
      - "variants": list[dict] (each dict is also flat)
      - "raw_text": full extracted text (never empty unless PDF is unreadable)
      - "extraction_method": string
    """
    all_text = ""
    all_tables: List[List[List[str]]] = []
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            all_text += (page.extract_text() or "") + "\n"
            try:
                all_tables.extend(page.extract_tables() or [])
            except Exception:
                # table extraction can fail for some PDFs; keep going
                pass

    text = _norm_ws(all_text)

    # Start with generic key/value extraction
    kv = {}
    kv.update(_extract_kv_from_tables(all_tables))
    kv.update(_extract_kv_from_lines(all_text))

    # Canonical fields (top-level) using robust patterns.
    # These become "expected" columns, but Publisher will still be dynamic.
    canonical: Dict[str, str] = {
        "power_tolerance": _first_match(all_text, [r"Power\s+Tolerance\s*[:\-]?\s*([^\n]+)"]),
        "temperature_noct": _first_match(all_text, [r"45\s*±\s*2\s*°C", r"NOCT[^\n]*?([0-9]+\s*±\s*[0-9]+\s*°C)"]),
        "maximum_system_voltage": _first_match(all_text, [r"Maximum\s+System\s+Voltage\s*[:\-]?\s*([^\n]+)"]),
        "maximum_series_fuse_rating": _first_match(all_text, [r"Maximum\s+Series\s+Fuse\s+Rating\s*[:\-]?\s*([^\n]+)"]),
        "refer_bifacial_factor": _first_match(all_text, [r"Refer\.?\s*Bifacial\s*Factor\s*[:\-]?\s*([^\n]+)"]),
        "temperature_coefficient_pmax": _first_match(all_text, [r"Temperature\s+Coefficient\s+of\s+Pmax\s*[:\-]?\s*([^\n]+)", r"TEMPERATURE\s*COEFFICIENT\s*[:\-]?\s*([^\n]+)"]),
        "temperature_coefficient_voc": _first_match(all_text, [r"Temperature\s+Coefficient\s+of\s+Voc\s*[:\-]?\s*([^\n]+)"]),
        "temperature_coefficient_isc": _first_match(all_text, [r"Temperature\s+Coefficient\s+of\s+Isc\s*[:\-]?\s*([^\n]+)"]),
    }

    # Mechanical / packaging (common on many datasheets)
    canonical.update(
        {
            "dimensions": _first_match(all_text, [r"Dimensions\s*[:\-]?\s*([^\n]+)"]),
            "weight": _first_match(all_text, [r"Weight\s*[:\-]?\s*([^\n]+)"]),
            "front_glass": _first_match(all_text, [r"Front\s+Glass\s*[:\-]?\s*([^\n]+)"]),
            "back_glass": _first_match(all_text, [r"Back\s+Glass\s*[:\-]?\s*([^\n]+)"]),
            "frame": _first_match(all_text, [r"Frame\s*[:\-]?\s*([^\n]+)"]),
            "junction_box": _first_match(all_text, [r"Junction\s+Box\s*[:\-]?\s*([^\n]+)"]),
            "protection_class": _first_match(all_text, [r"Protection\s+Class\s*[:\-]?\s*([^\n]+)"]),
            "iec_fire_type": _first_match(all_text, [r"IEC\s*Fire\s*Type\s*[:\-]?\s*([^\n]+)"]),
            "output_cables": _first_match(all_text, [r"Output\s+Cables\s*[:\-]?\s*([^\n]+)"]),
            "pallet_dimensions": _first_match(all_text, [r"Pallet\s+Dimen(?:tions|sions)\s*[:\-]?\s*([^\n]+)"]),
            "packing_details": _first_match(all_text, [r"(\d+\s*pcs/pallet[^\n]*)"]),
        }
    )

    # Merge: canonical overrides generic kv when it has a value
    out: Dict[str, object] = {}
    for k, v in kv.items():
        out[k] = v
    for k, v in canonical.items():
        if v:
            out[_norm_key(k)] = v
        else:
            # ensure column exists downstream even if empty
            out.setdefault(_norm_key(k), "")

    # Variants
    variants = _parse_jinko_like_variants(all_text)
    if not variants:
        variants = _parse_renewsys_like_variants(all_text)
    out["variants"] = variants

    # Always store raw text and raw tables (as JSON) so values are never lost.
    out["raw_text"] = all_text or ""
    out["raw_tables_json"] = json.dumps(all_tables, ensure_ascii=False)
    out["extraction_method"] = "pdfplumber_generic_kv+variants_v1"

    # Ensure no None values (Publisher expects this too)
    for k, v in list(out.items()):
        if v is None:
            out[k] = ""
    # Also normalize variant dicts
    if isinstance(out.get("variants"), list):
        norm_vars = []
        for vv in out["variants"]:
            if not isinstance(vv, dict):
                continue
            vv2 = {str(kk): ("" if vv[kk] is None else str(vv[kk])) for kk in vv.keys()}
            norm_vars.append(vv2)
        out["variants"] = norm_vars

    return out
