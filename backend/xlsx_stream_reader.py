from __future__ import annotations

import os
import posixpath
import re
import struct
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from array import array
from collections import OrderedDict
from pathlib import Path

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

N = f"{{{MAIN_NS}}}"
R = f"{{{REL_NS}}}"
P = f"{{{PKG_REL_NS}}}"

DIMENSION_TAG = f"{N}dimension"
SHEETDATA_TAG = f"{N}sheetData"
ROW_TAG = f"{N}row"
CELL_TAG = f"{N}c"
VALUE_TAG = f"{N}v"
INLINE_STRING_TAG = f"{N}is"
TEXT_TAG = f"{N}t"
FORMULA_TAG = f"{N}f"
SHEETS_TAG = f"{N}sheets"
WORKBOOK_SHEET_TAG = f"{N}sheet"
RELATIONSHIP_TAG = f"{P}Relationship"
SI_TAG = f"{N}si"

CELL_REF_COL_RE = re.compile(r"[A-Z]+")
CELL_REF_ROW_RE = re.compile(r"(\d+)")
SHARED_STRING_LENGTH_PACKER = struct.Struct("<I")
SHARED_STRINGS_IN_MEMORY_LIMIT = int(os.getenv("XLSX_SHARED_STRINGS_IN_MEMORY_LIMIT", "250000"))
SHARED_STRINGS_LOOKUP_CACHE_SIZE = int(os.getenv("XLSX_SHARED_STRINGS_LOOKUP_CACHE_SIZE", "8192"))


def _normalize_target(base_dir: str, target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join(base_dir, target))


def _column_index_from_ref(cell_ref: str, fallback_index: int) -> int:
    if not cell_ref:
        return fallback_index
    match = CELL_REF_COL_RE.match(cell_ref.upper())
    if not match:
        return fallback_index
    value = 0
    for char in match.group(0):
        value = (value * 26) + (ord(char) - 64)
    return max(0, value - 1)


def _extract_text(element: ET.Element) -> str:
    parts = []
    for node in element.iter():
        if node.tag == TEXT_TAG and node.text:
            parts.append(node.text)
    return "".join(parts)


def _parse_dimension_last_row(ref: str | None) -> int | None:
    if not ref:
        return None
    tail = ref.split(":")[-1]
    match = CELL_REF_ROW_RE.search(tail)
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


class XlsxStreamReader:
    def __init__(self, file_path: str | Path):
        self.file_path = Path(file_path)
        self._zip = zipfile.ZipFile(self.file_path, "r")
        self._sheet_path_by_name: dict[str, str] | None = None
        self._shared_strings_path: str | None = None
        self._shared_strings_temp_dir = self._resolve_shared_strings_temp_dir()

        self._shared_strings_mode = "none"
        self._shared_strings_memory: list[str] = []
        self._shared_strings_offsets = array("Q")
        self._shared_strings_count = 0
        self._shared_strings_complete = False
        self._shared_strings_lookup_count = 0
        self._shared_strings_prepare_seconds = 0.0
        self._shared_strings_lookup_seconds = 0.0

        self._shared_strings_source = None
        self._shared_strings_iter = None
        self._shared_strings_temp_file = None
        self._shared_strings_temp_path: str | None = None
        self._shared_strings_lookup_cache: OrderedDict[int, str] = OrderedDict()

    def _resolve_shared_strings_temp_dir(self) -> str | None:
        configured = os.getenv("XLSX_SHARED_STRINGS_TEMP_DIR", "").strip()
        candidates = []

        if configured:
            candidates.append(Path(configured))

        candidates.append(self.file_path.parent / ".xlsx_stream_tmp")

        for candidate in candidates:
            try:
                candidate.mkdir(parents=True, exist_ok=True)
                return str(candidate)
            except Exception:
                continue

        return None

    def _create_shared_strings_temp_file(self) -> tuple[str, object]:
        attempts = [self._shared_strings_temp_dir]
        if None not in attempts:
            attempts.append(None)

        last_error = None

        for temp_dir in attempts:
            kwargs = {
                "prefix": "xlsx_shared_strings_",
                "suffix": ".bin",
            }
            if temp_dir:
                kwargs["dir"] = temp_dir

            try:
                fd, temp_path = tempfile.mkstemp(**kwargs)
                os.close(fd)
                return temp_path, open(temp_path, "w+b")
            except Exception as exc:
                last_error = exc

        raise OSError("Nao foi possivel criar o arquivo temporario de shared strings.") from last_error

    def close(self):
        if self._shared_strings_source is not None:
            try:
                self._shared_strings_source.close()
            except Exception:
                pass
            self._shared_strings_source = None
        if self._shared_strings_temp_file is not None:
            try:
                self._shared_strings_temp_file.close()
            except Exception:
                pass
            self._shared_strings_temp_file = None
        if self._shared_strings_temp_path:
            try:
                Path(self._shared_strings_temp_path).unlink(missing_ok=True)
            except Exception:
                pass
            self._shared_strings_temp_path = None
        self._zip.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def load_container(self):
        rels_path = "xl/_rels/workbook.xml.rels"
        workbook_path = "xl/workbook.xml"

        rels_tree = ET.parse(self._zip.open(rels_path))
        rels_root = rels_tree.getroot()
        rel_target_by_id: dict[str, str] = {}
        workbook_base = posixpath.dirname(workbook_path)

        for rel in rels_root.findall(RELATIONSHIP_TAG):
            rel_id = rel.get("Id")
            rel_type = rel.get("Type", "")
            target = rel.get("Target", "")
            if not rel_id or not target:
                continue
            normalized_target = _normalize_target(workbook_base, target)
            rel_target_by_id[rel_id] = normalized_target
            if rel_type.endswith("/sharedStrings"):
                self._shared_strings_path = normalized_target

        workbook_tree = ET.parse(self._zip.open(workbook_path))
        workbook_root = workbook_tree.getroot()
        sheets_root = workbook_root.find(SHEETS_TAG)

        sheet_path_by_name: dict[str, str] = {}
        if sheets_root is not None:
            for sheet in sheets_root.findall(WORKBOOK_SHEET_TAG):
                name = sheet.get("name")
                rel_id = sheet.get(f"{R}id")
                if not name or not rel_id:
                    continue
                target = rel_target_by_id.get(rel_id)
                if target:
                    sheet_path_by_name[name] = target

        self._sheet_path_by_name = sheet_path_by_name
        if not self._shared_strings_path:
            self._shared_strings_complete = True

        return {
            "sheet_path_by_name": dict(sheet_path_by_name),
            "shared_strings_path": self._shared_strings_path,
        }

    @property
    def sheet_path_by_name(self):
        if self._sheet_path_by_name is None:
            self.load_container()
        return self._sheet_path_by_name or {}

    def resolve_sheet_name(self, desired_name: str, normalizer):
        if desired_name in self.sheet_path_by_name:
            return desired_name
        target = normalizer(desired_name)
        for name in self.sheet_path_by_name:
            if normalizer(name) == target:
                return name
        return None

    def _ensure_shared_strings_iter(self):
        if self._shared_strings_complete or not self._shared_strings_path or self._shared_strings_iter is not None:
            return
        self._shared_strings_source = self._zip.open(self._shared_strings_path)
        self._shared_strings_iter = ET.iterparse(self._shared_strings_source, events=("end",))
        if self._shared_strings_mode == "none":
            self._shared_strings_mode = "memory"

    def _switch_shared_strings_to_disk(self):
        self._shared_strings_temp_path, self._shared_strings_temp_file = self._create_shared_strings_temp_file()
        self._shared_strings_mode = "disk"

        for value in self._shared_strings_memory:
            self._write_shared_string_to_disk(value)
        self._shared_strings_memory.clear()

    def _remember_shared_string_in_cache(self, index: int, value: str):
        if SHARED_STRINGS_LOOKUP_CACHE_SIZE <= 0:
            return
        self._shared_strings_lookup_cache[index] = value
        self._shared_strings_lookup_cache.move_to_end(index)
        if len(self._shared_strings_lookup_cache) > SHARED_STRINGS_LOOKUP_CACHE_SIZE:
            self._shared_strings_lookup_cache.popitem(last=False)

    def _write_shared_string_to_disk(self, value: str, remember: bool = False):
        if self._shared_strings_temp_file is None:
            self._switch_shared_strings_to_disk()
        data = value.encode("utf-8")
        index = len(self._shared_strings_offsets)
        offset = self._shared_strings_temp_file.tell()
        self._shared_strings_offsets.append(offset)
        self._shared_strings_temp_file.write(SHARED_STRING_LENGTH_PACKER.pack(len(data)))
        self._shared_strings_temp_file.write(data)
        if remember:
            self._remember_shared_string_in_cache(index, value)

    def _store_shared_string(self, value: str):
        if self._shared_strings_mode == "memory" and len(self._shared_strings_memory) < SHARED_STRINGS_IN_MEMORY_LIMIT:
            self._shared_strings_memory.append(value)
        else:
            if self._shared_strings_mode != "disk":
                self._switch_shared_strings_to_disk()
            self._write_shared_string_to_disk(value, remember=True)
        self._shared_strings_count += 1

    def _finalize_shared_strings_iter(self):
        if self._shared_strings_source is not None:
            try:
                self._shared_strings_source.close()
            except Exception:
                pass
        self._shared_strings_source = None
        self._shared_strings_iter = None
        self._shared_strings_complete = True

    def _advance_shared_strings_until(self, target_index: int):
        if self._shared_strings_complete or target_index < self._shared_strings_count:
            return

        self._ensure_shared_strings_iter()
        started_at = time.perf_counter()
        try:
            for _, elem in self._shared_strings_iter:
                if elem.tag != SI_TAG:
                    continue
                self._store_shared_string(_extract_text(elem))
                elem.clear()
                if self._shared_strings_count > target_index:
                    break
            else:
                self._finalize_shared_strings_iter()
        finally:
            self._shared_strings_prepare_seconds += time.perf_counter() - started_at

    def _read_shared_string_from_disk(self, index: int) -> str:
        cached = self._shared_strings_lookup_cache.get(index)
        if cached is not None:
            self._shared_strings_lookup_cache.move_to_end(index)
            return cached

        if self._shared_strings_temp_file is None or index >= len(self._shared_strings_offsets):
            raise IndexError(index)

        self._shared_strings_temp_file.seek(self._shared_strings_offsets[index])
        length_raw = self._shared_strings_temp_file.read(SHARED_STRING_LENGTH_PACKER.size)
        if len(length_raw) != SHARED_STRING_LENGTH_PACKER.size:
            raise IndexError(index)
        (length,) = SHARED_STRING_LENGTH_PACKER.unpack(length_raw)
        value = self._shared_strings_temp_file.read(length).decode("utf-8")
        self._remember_shared_string_in_cache(index, value)
        return value

    def get_shared_string(self, index: int) -> str:
        if index < 0:
            raise IndexError(index)

        if index >= self._shared_strings_count:
            self._advance_shared_strings_until(index)

        started_at = time.perf_counter()
        self._shared_strings_lookup_count += 1
        try:
            if self._shared_strings_mode == "memory":
                return self._shared_strings_memory[index]
            if self._shared_strings_mode == "disk":
                return self._read_shared_string_from_disk(index)
            raise IndexError(index)
        finally:
            self._shared_strings_lookup_seconds += time.perf_counter() - started_at

    def shared_strings_stats(self):
        return {
            "mode": self._shared_strings_mode,
            "count": self._shared_strings_count,
            "prepare_seconds": self._shared_strings_prepare_seconds,
            "lookup_seconds": self._shared_strings_lookup_seconds,
            "lookup_count": self._shared_strings_lookup_count,
            "complete": self._shared_strings_complete,
        }

    def debug_sheet_xml_rows(self, sheet_path: str, max_rows: int = 5, max_cells: int = 12):
        rows = []
        with self._zip.open(sheet_path) as fh:
            for _, row_elem in ET.iterparse(fh, events=("end",)):
                if row_elem.tag != ROW_TAG:
                    continue

                cells = []
                for cell_elem in row_elem.findall(CELL_TAG)[:max_cells]:
                    inline_elem = cell_elem.find(INLINE_STRING_TAG)
                    raw_value = cell_elem.findtext(VALUE_TAG)
                    formula_value = cell_elem.findtext(FORMULA_TAG)
                    cells.append({
                        "r": cell_elem.get("r", ""),
                        "t": cell_elem.get("t", ""),
                        "v": raw_value,
                        "inline": _extract_text(inline_elem) if inline_elem is not None else None,
                        "f": formula_value,
                    })

                rows.append({
                    "row_ref": row_elem.get("r"),
                    "cells": cells,
                })
                row_elem.clear()
                if len(rows) >= max_rows:
                    break
        return rows

    def estimate_sheet_total_rows(self, sheet_path: str) -> int | None:
        with self._zip.open(sheet_path) as fh:
            for _, elem in ET.iterparse(fh, events=("start",)):
                if elem.tag == DIMENSION_TAG:
                    return _parse_dimension_last_row(elem.get("ref"))
                if elem.tag == SHEETDATA_TAG:
                    break
        return None

    def iter_rows(self, sheet_path: str):
        with self._zip.open(sheet_path) as fh:
            for _, row_elem in ET.iterparse(fh, events=("end",)):
                if row_elem.tag != ROW_TAG:
                    continue

                row_ref = row_elem.get("r")
                row_index = (int(row_ref) - 1) if row_ref and row_ref.isdigit() else None
                values = []
                next_col_index = 0

                for cell_elem in row_elem.findall(CELL_TAG):
                    cell_ref = cell_elem.get("r", "")
                    col_index = _column_index_from_ref(cell_ref, next_col_index)
                    while len(values) < col_index:
                        values.append(None)

                    cell_type = cell_elem.get("t")
                    inline_elem = cell_elem.find(INLINE_STRING_TAG)
                    raw_value = cell_elem.findtext(VALUE_TAG)

                    if inline_elem is not None and (cell_type == "inlineStr" or raw_value is None):
                        cell_value = _extract_text(inline_elem)
                    elif raw_value is None:
                        formula_value = cell_elem.findtext(FORMULA_TAG)
                        cell_value = formula_value or ""
                    elif cell_type == "s":
                        try:
                            cell_value = self.get_shared_string(int(raw_value))
                        except Exception:
                            cell_value = raw_value
                    elif cell_type == "b":
                        cell_value = 1 if raw_value == "1" else 0
                    elif cell_type in {"str", "e", "d"}:
                        cell_value = raw_value
                    else:
                        try:
                            if "." not in raw_value and "e" not in raw_value.lower():
                                cell_value = int(raw_value)
                            else:
                                cell_value = float(raw_value)
                        except Exception:
                            cell_value = raw_value

                    values.append(cell_value)
                    next_col_index = len(values)

                if row_index is None:
                    row_index = 0

                yield row_index, values
                row_elem.clear()
