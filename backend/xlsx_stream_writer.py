from __future__ import annotations

import math
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

MAIN_NS = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
REL_NS = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

INVALID_SHEET_TITLE_CHARS = set('[]:*?/\\')


def _column_letter(index: int) -> str:
    letters = []
    while index > 0:
        index, remainder = divmod(index - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))


def _needs_preserve_spaces(value: str) -> bool:
    return value != value.strip() or "  " in value or "\n" in value or "\r" in value or "\t" in value


def _format_number(value) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return ""
        return format(value, ".15g")
    return str(value)


def _sanitize_sheet_title(title: str, existing_titles: set[str]) -> str:
    cleaned = "".join(" " if ch in INVALID_SHEET_TITLE_CHARS else ch for ch in (title or "Sheet")).strip()
    cleaned = cleaned or "Sheet"
    cleaned = cleaned[:31]

    if cleaned not in existing_titles:
        return cleaned

    base = cleaned[:28] or "Sheet"
    suffix = 2
    while True:
        candidate = f"{base}_{suffix}"[:31]
        if candidate not in existing_titles:
            return candidate
        suffix += 1


class StreamingXlsxWorksheet:
    def __init__(self, workbook: "StreamingXlsxWorkbook", title: str, index: int):
        self.workbook = workbook
        self.title = title
        self.index = index
        self.path_in_zip = f"xl/worksheets/sheet{index}.xml"
        self._row_number = 1
        self._max_col_index = 0
        self._opened = False
        self._closed = False

    def _open_stream(self):
        self.workbook._sheet_stream = self.workbook._zip.open(self.path_in_zip, "w")
        self.workbook._sheet_stream.write(
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                f'<worksheet xmlns="{MAIN_NS}"><sheetData>'
            ).encode("utf-8")
        )
        self.workbook._active_sheet = self
        self._opened = True

    def _ensure_open(self):
        if self._closed:
            raise RuntimeError(f"A planilha '{self.title}' ja foi finalizada.")
        self.workbook._activate_sheet(self)
        if self._opened:
            return
        self._open_stream()

    def append(self, values):
        self._ensure_open()
        row_parts = [f'<row r="{self._row_number}">']

        for col_index, value in enumerate(values, start=1):
            if value is None or value == "":
                continue

            cell_ref = f"{_column_letter(col_index)}{self._row_number}"
            self._max_col_index = max(self._max_col_index, col_index)

            if isinstance(value, (int, float)) and not isinstance(value, bool):
                number = _format_number(value)
                if not number:
                    continue
                row_parts.append(f'<c r="{cell_ref}"><v>{number}</v></c>')
                continue

            if isinstance(value, bool):
                row_parts.append(f'<c r="{cell_ref}"><v>{_format_number(value)}</v></c>')
                continue

            text = str(value)
            if text == "":
                continue

            preserve = ' xml:space="preserve"' if _needs_preserve_spaces(text) else ""
            row_parts.append(
                f'<c r="{cell_ref}" t="inlineStr"><is><t{preserve}>{escape(text)}</t></is></c>'
            )

        row_parts.append("</row>")
        self.workbook._sheet_stream.write("".join(row_parts).encode("utf-8"))
        self._row_number += 1

    def close(self):
        if self._closed:
            return
        if not self._opened:
            self.workbook._activate_sheet(self)
            self._open_stream()
        if self.workbook._sheet_stream is not None:
            self.workbook._sheet_stream.write(b"</sheetData></worksheet>")
            self.workbook._sheet_stream.close()
            self.workbook._sheet_stream = None
            self.workbook._active_sheet = None
        self._closed = True


class StreamingXlsxWorkbook:
    def __init__(self, output_path: str | Path):
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._zip = zipfile.ZipFile(self.output_path, "w", compression=zipfile.ZIP_DEFLATED)
        self._worksheets: list[StreamingXlsxWorksheet] = []
        self._active_sheet: StreamingXlsxWorksheet | None = None
        self._sheet_stream = None
        self._saved = False
        self._closed = False
        self._sheet_titles: set[str] = set()

    @property
    def sheetnames(self):
        return [worksheet.title for worksheet in self._worksheets]

    def create_sheet(self, title: str):
        safe_title = _sanitize_sheet_title(title, self._sheet_titles)
        self._sheet_titles.add(safe_title)
        worksheet = StreamingXlsxWorksheet(self, safe_title, len(self._worksheets) + 1)
        self._worksheets.append(worksheet)
        return worksheet

    def _activate_sheet(self, worksheet: StreamingXlsxWorksheet):
        if self._active_sheet is worksheet:
            return
        if self._active_sheet is not None:
            self._active_sheet.close()

    def _write_workbook_parts(self):
        sheets_xml = []
        rels_xml = []
        content_types = [
            '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>',
            '<Default Extension="xml" ContentType="application/xml"/>',
            '<Override PartName="/xl/workbook.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>',
        ]

        for worksheet in self._worksheets:
            rel_id = f"rId{worksheet.index}"
            sheets_xml.append(
                f'<sheet name="{escape(worksheet.title)}" sheetId="{worksheet.index}" '
                f'xmlns:r="{REL_NS}" r:id="{rel_id}"/>'
            )
            rels_xml.append(
                f'<Relationship Id="{rel_id}" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="worksheets/sheet{worksheet.index}.xml"/>'
            )
            content_types.append(
                f'<Override PartName="/xl/worksheets/sheet{worksheet.index}.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )

        workbook_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<workbook xmlns="{MAIN_NS}" xmlns:r="{REL_NS}"><sheets>'
            + "".join(sheets_xml)
            + "</sheets></workbook>"
        )
        workbook_rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<Relationships xmlns="{PKG_REL_NS}">'
            + "".join(rels_xml)
            + "</Relationships>"
        )
        root_rels_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<Relationships xmlns="{PKG_REL_NS}">'
            '<Relationship Id="rId1" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
            'Target="xl/workbook.xml"/>'
            "</Relationships>"
        )
        content_types_xml = (
            '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
            + "".join(content_types)
            + "</Types>"
        )

        self._zip.writestr("[Content_Types].xml", content_types_xml)
        self._zip.writestr("_rels/.rels", root_rels_xml)
        self._zip.writestr("xl/workbook.xml", workbook_xml)
        self._zip.writestr("xl/_rels/workbook.xml.rels", workbook_rels_xml)

    def save(self):
        if self._saved:
            return
        if not self._worksheets:
            self.create_sheet("Sheet1")
        if self._active_sheet is not None:
            self._active_sheet.close()
        for worksheet in self._worksheets:
            worksheet.close()
        self._write_workbook_parts()
        self._zip.close()
        self._saved = True
        self._closed = True

    def close(self, discard: bool = False):
        if self._closed:
            return
        if not self._saved:
            if self._active_sheet is not None:
                self._active_sheet.close()
            for worksheet in self._worksheets:
                worksheet.close()
            self._zip.close()
            if discard:
                self.output_path.unlink(missing_ok=True)
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.save()
        else:
            self.close(discard=True)
