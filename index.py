#!/usr/bin/env python3
"""MTDPS v0.3.1 — Minor Thesis Data Processing System."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import tempfile
import zipfile
from collections import Counter
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


VERSION = "0.3.1"

WRITING_TIME_TOLERANCE_MS = Decimal("10")
LARGE_INSERTION_THRESHOLD_CHARS = 50
BULK_REPLACEMENT_THRESHOLD_CHARS = 20

DEFAULT_INPUT_DIR = Path(
    r"D:\Data\Desktop\研二上\MT1-90043\202608-Participant Data"
)
DEFAULT_OUTPUT_DIR = Path("output")


MANIFEST_FIELDS = [
    "participant",
    "task",
    "condition",
    "condition_label",
    "planning_mode",
    "familiarity",
    "topic",
    "planned_planning_limit_seconds",
    "actual_planning_time_ms",
    "actual_planning_time_seconds",
    "planning_end_reason",
    "planned_writing_limit_seconds",
    "actual_writing_time_ms",
    "actual_writing_time_seconds",
    "writing_end_reason",
    "word_count",
    "snapshot_count",
    "keystroke_rows",
    "ai_chat_present",
    "source_zip",
    "raw_text_file",
    "warnings",
]

REPORT_FIELDS = [
    "source_zip",
    "status",
    "participant",
    "task",
    "condition",
    "raw_text_file",
    "warnings",
    "error",
]

TIMESERIES_FIELDS = [
    "participant",
    "task",
    "condition",
    "planning_mode",
    "familiarity",
    "topic",
    "observation_type",
    "minute",
    "elapsed_time_ms",
    "elapsed_time_seconds",
    "cumulative_word_count",
    "delta_word_count",
    "actual_writing_time_seconds",
    "writing_end_reason",
    "source_zip",
]

QC_FIELDS = [
    "source_zip",
    "participant",
    "task",
    "condition",
    "rule",
    "category",
    "severity",
    "result",
    "evidence",
]

QC_SEVERITIES = {"INFO", "WARNING", "FLAG", "ERROR"}
QC_RESULTS = {"PASS", "FAIL", "NOT_CHECKED", "NOT_APPLICABLE"}
SEVERITY_RANK = {
    "INFO": 0,
    "WARNING": 1,
    "FLAG": 2,
    "ERROR": 3,
}

METADATA_IDENTITY_KEYS = {
    "subject_code",
    "task_number",
    "topic_code",
    "planning_mode",
    "familiarity",
    "topic",
}

KEYSTROKE_REQUIRED_COLUMNS = {
    "subject_code",
    "topic_code",
    "task_number",
    "condition",
    "familiarity",
    "planning_mode",
    "time_ms",
    "event",
    "key",
    "inputType",
    "data",
    "cursor_start",
    "cursor_end",
    "word_count",
}

SYSTEM_BASENAMES = {
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
}

SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
SNAPSHOT_HEADER_RE = re.compile(
    r"^=== Minute ([0-9]+) ===[ \t]*$",
    re.MULTILINE,
)
SNAPSHOT_HEADER_LIKE_RE = re.compile(
    r"^[ \t]*={2,}[ \t]*Minute\b[^\n]*$",
    re.IGNORECASE | re.MULTILINE,
)
SNAPSHOT_WORD_COUNT_RE = re.compile(
    r"^\n[ \t]*Word count:[ \t]*([0-9]+)[ \t]*\n"
    r"(?:[ \t]*\n)?"
)
SNAPSHOT_PREAMBLE_RE = re.compile(
    r"^(Subject code|Topic code|Task number):[ \t]*(.*?)[ \t]*$",
    re.MULTILINE,
)


class PackageError(Exception):
    """A Gamma ZIP package cannot be processed reliably."""

    def __init__(
        self,
        message: str,
        *,
        rule: str = "PACKAGE_PROCESSING_ERROR",
        category: str = "archive",
        severity: str = "ERROR",
        evidence: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.rule = rule
        self.category = category
        self.severity = severity
        self.evidence = evidence or {"error": message}
        self.participant = ""
        self.task: int | str = ""
        self.condition = ""
        self.qc_rows: list[dict[str, Any]] = []


@dataclass(frozen=True)
class Snapshot:
    """One observed minute snapshot in source-file order."""

    sequence: int
    minute: int
    text: str
    declared_word_count: int | None
    cumulative_word_count: int


@dataclass(frozen=True)
class WritingEndObservation:
    """The final writing endpoint recorded in the keystroke log."""

    elapsed_time_ms: Decimal
    cumulative_word_count: int


@dataclass(frozen=True)
class KeystrokeResult:
    """Validated information extracted from one keystroke CSV."""

    info: zipfile.ZipInfo | None
    row_count: int | None
    writing_end: WritingEndObservation | None
    warnings: list[str]


@dataclass(frozen=True)
class KeystrokeReplayResult:
    """Result and diagnostics from replaying confirmed text-edit events."""

    text: str
    applied_events: int
    input_events_without_beforeinput: list[dict[str, Any]]
    unsupported_events: list[dict[str, Any]]
    invalid_cursor_events: list[dict[str, Any]]
    large_insertions: list[dict[str, Any]]
    bulk_replacements: list[dict[str, Any]]
    paste_or_drop_events: list[dict[str, Any]]


@dataclass(frozen=True)
class QCRecord:
    """One machine-readable quality-control result."""

    source_zip: str
    participant: str
    task: int | str
    condition: str
    rule: str
    category: str
    severity: str
    result: str
    evidence: str

    def as_row(self) -> dict[str, Any]:
        return {
            "source_zip": self.source_zip,
            "participant": self.participant,
            "task": self.task,
            "condition": self.condition,
            "rule": self.rule,
            "category": self.category,
            "severity": self.severity,
            "result": self.result,
            "evidence": self.evidence,
        }


@dataclass(frozen=True)
class BatchSummary:
    discovered: int
    processed: int
    duplicates: int
    failed: int
    timeseries_rows: int
    manifest_path: Path
    report_path: Path
    timeseries_path: Path
    qc_path: Path


@dataclass
class ParsedPackage:
    source_zip: Path
    manifest_row: dict[str, Any]
    timeseries_rows: list[dict[str, Any]]
    essay_bytes: bytes
    research_sha256: str
    warnings: list[str]
    qc_rows: list[dict[str, Any]]


def qc_evidence(**values: Any) -> str:
    """Encode QC evidence as stable, machine-readable JSON."""
    return json.dumps(values, ensure_ascii=False, sort_keys=True)


def make_qc_row(
    source_zip: str,
    participant: str,
    task: int | str,
    condition: str,
    rule: str,
    category: str,
    severity: str,
    result: str,
    **evidence: Any,
) -> dict[str, Any]:
    """Build and validate one QC result row."""
    if severity not in QC_SEVERITIES:
        raise ValueError(f"Unsupported QC severity: {severity}")
    if result not in QC_RESULTS:
        raise ValueError(f"Unsupported QC result: {result}")

    return QCRecord(
        source_zip=source_zip,
        participant=participant,
        task=task,
        condition=condition,
        rule=rule,
        category=category,
        severity=severity,
        result=result,
        evidence=qc_evidence(**evidence),
    ).as_row()


def qc_summary(qc_rows: Iterable[dict[str, Any]]) -> str:
    """Create a compact compatibility summary for legacy warnings fields."""
    findings = [
        row
        for row in qc_rows
        if row["result"] == "FAIL" and row["severity"] != "INFO"
    ]
    if not findings:
        return ""

    highest = max(
        findings,
        key=lambda row: SEVERITY_RANK[str(row["severity"])],
    )["severity"]
    return f"qc_findings={len(findings)}; qc_max_severity={highest}"


def normalise_member_name(name: str) -> str:
    """Normalise ZIP member separators without extracting the member."""
    return name.replace("\\", "/")


def normalise_newlines(text: str) -> str:
    """Normalise CRLF and legacy CR newlines to LF for parsing."""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def normalise_text_for_comparison(text: str) -> str:
    """Ignore newline style and outer whitespace in text comparisons."""
    return normalise_newlines(text).strip()


def is_research_member(info: zipfile.ZipInfo) -> bool:
    """Return False for directories and common operating-system files."""
    if info.is_dir():
        return False

    normalised = normalise_member_name(info.filename)
    parts = PurePosixPath(normalised).parts

    if not parts:
        return False

    basename = parts[-1]

    if "__MACOSX" in parts:
        return False

    if basename.startswith("._"):
        return False

    if basename in SYSTEM_BASENAMES:
        return False

    return True


def unsafe_member_path(name: str) -> bool:
    """Detect absolute paths and parent traversal inside a ZIP."""
    normalised = normalise_member_name(name)
    path = PurePosixPath(normalised)
    return path.is_absolute() or ".." in path.parts


def metadata_candidates(
    archive: zipfile.ZipFile,
    members: Iterable[zipfile.ZipInfo],
) -> list[tuple[zipfile.ZipInfo, dict[str, Any]]]:
    """Find metadata by JSON content rather than the outer ZIP name."""
    candidates: list[tuple[zipfile.ZipInfo, dict[str, Any]]] = []

    for info in members:
        if not info.filename.lower().endswith(".json"):
            continue

        try:
            value = json.loads(archive.read(info).decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            basename = PurePosixPath(
                normalise_member_name(info.filename)
            ).name.lower()

            if "metadata" in basename:
                raise PackageError(
                    f"Metadata candidate is invalid JSON: {info.filename}",
                    rule="METADATA_JSON_VALID",
                    evidence={"filename": info.filename},
                )

            continue

        if isinstance(value, dict) and METADATA_IDENTITY_KEYS.issubset(value):
            candidates.append((info, value))

    return candidates


def choose_single(
    candidates: list[zipfile.ZipInfo],
    role: str,
) -> zipfile.ZipInfo:
    """Require exactly one candidate for an essential research role."""
    if not candidates:
        raise PackageError(f"Missing {role}")

    if len(candidates) > 1:
        names = ", ".join(info.filename for info in candidates)
        raise PackageError(f"Multiple {role} candidates: {names}")

    return candidates[0]


def find_final_essay(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
) -> tuple[zipfile.ZipInfo, bytes]:
    """Find and validate the final essay TXT."""
    candidates = [
        info
        for info in members
        if PurePosixPath(
            normalise_member_name(info.filename)
        ).name.lower().endswith("_final_text.txt")
    ]

    if not candidates:
        raise PackageError(
            "Missing final essay",
            rule="FINAL_ESSAY_PRESENT_UNIQUE",
            evidence={"candidate_count": 0},
        )
    if len(candidates) > 1:
        raise PackageError(
            "Multiple final essay candidates: "
            + ", ".join(info.filename for info in candidates),
            rule="FINAL_ESSAY_PRESENT_UNIQUE",
            evidence={
                "candidate_count": len(candidates),
                "candidates": [info.filename for info in candidates],
            },
        )

    info = candidates[0]
    raw = archive.read(info)

    try:
        raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PackageError(
            f"Final essay is not valid UTF-8: {info.filename}",
            rule="FINAL_ESSAY_UTF8_VALID",
            evidence={"filename": info.filename, "error": str(exc)},
        ) from exc

    return info, raw


def decimal_from_csv(value: str, field: str) -> Decimal:
    """Parse a finite decimal stored in a CSV field."""
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise PackageError(
            f"Invalid {field} in keystroke log: {value!r}",
            rule="KEYSTROKE_TIME_VALUES_VALID",
            category="keystroke",
            evidence={"field": field, "value": value},
        ) from exc

    if not parsed.is_finite():
        raise PackageError(
            f"Non-finite {field} in keystroke log: {value!r}",
            rule="KEYSTROKE_TIME_VALUES_VALID",
            category="keystroke",
            evidence={"field": field, "value": value},
        )

    return parsed


def parse_writing_end(
    rows: list[dict[str, str]],
    participant: str,
    task: int,
    condition: str,
) -> tuple[WritingEndObservation | None, list[str]]:
    """Parse the unique writing_end endpoint from a keystroke log."""
    warnings: list[str] = []
    start_rows = [row for row in rows if row.get("event") == "writing_start"]

    if not start_rows:
        warnings.append("missing_keystroke_writing_start")
    elif len(start_rows) > 1:
        warnings.append(
            f"multiple_keystroke_writing_start(count={len(start_rows)})"
        )
    else:
        start_time = decimal_from_csv(
            start_rows[0].get("time_ms", ""),
            "writing_start time_ms",
        )
        if start_time != 0:
            warnings.append(
                "nonzero_keystroke_writing_start"
                f"(time_ms={decimal_csv(start_time)})"
            )

    end_rows = [row for row in rows if row.get("event") == "writing_end"]

    if not end_rows:
        warnings.append("missing_keystroke_writing_end")
        return None, warnings

    if len(end_rows) > 1:
        warnings.append(f"multiple_keystroke_writing_end(count={len(end_rows)})")
        return None, warnings

    row = end_rows[0]
    expected_identity = {
        "subject_code": participant,
        "task_number": str(task),
        "topic_code": condition,
    }
    for key, expected in expected_identity.items():
        observed = str(row.get(key, ""))
        if key == "topic_code":
            observed = observed.lower()
        if observed != expected:
            warnings.append(f"keystroke_{key}_mismatch")

    elapsed_time_ms = decimal_from_csv(row.get("time_ms", ""), "writing_end time_ms")

    if elapsed_time_ms < 0:
        raise PackageError(
            f"Negative writing_end time_ms in keystroke log: {elapsed_time_ms}",
            rule="KEYSTROKE_TIME_VALUES_VALID",
            category="keystroke",
            evidence={"event": "writing_end", "time_ms": str(elapsed_time_ms)},
        )

    try:
        word_count = int(row.get("word_count", ""))
    except (TypeError, ValueError) as exc:
        raise PackageError(
            "Invalid writing_end word_count in keystroke log: "
            f"{row.get('word_count')!r}",
            rule="KEYSTROKE_WRITING_END_WORD_COUNT_VALID",
            category="word_count",
            evidence={"value": row.get("word_count")},
        ) from exc

    if word_count < 0:
        raise PackageError(
            f"Negative writing_end word_count in keystroke log: {word_count}",
            rule="KEYSTROKE_WRITING_END_WORD_COUNT_VALID",
            category="word_count",
            evidence={"value": word_count},
        )

    return (
        WritingEndObservation(
            elapsed_time_ms=elapsed_time_ms,
            cumulative_word_count=word_count,
        ),
        warnings,
    )


def find_keystroke_log(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    participant: str,
    task: int,
    condition: str,
) -> KeystrokeResult:
    """Identify the keystroke CSV and parse its final writing endpoint."""
    warnings: list[str] = []
    candidates: list[tuple[zipfile.ZipInfo, list[dict[str, str]]]] = []
    named_missing_columns: list[tuple[zipfile.ZipInfo, list[str]]] = []

    for info in members:
        if not info.filename.lower().endswith(".csv"):
            continue

        try:
            text = archive.read(info).decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text, newline=""))
            columns = set(reader.fieldnames or [])

            if KEYSTROKE_REQUIRED_COLUMNS.issubset(columns):
                candidates.append((info, list(reader)))
            elif "keystroke" in info.filename.lower():
                named_missing_columns.append(
                    (
                        info,
                        sorted(KEYSTROKE_REQUIRED_COLUMNS - columns),
                    )
                )
        except (UnicodeDecodeError, csv.Error) as exc:
            if "keystroke" in info.filename.lower():
                raise PackageError(
                    f"Invalid keystroke CSV in {info.filename}: {exc}",
                    rule="KEYSTROKE_CSV_VALID",
                    category="keystroke",
                    evidence={"filename": info.filename, "error": str(exc)},
                ) from exc

    if named_missing_columns:
        raise PackageError(
            "Keystroke CSV is missing required columns",
            rule="KEYSTROKE_COLUMNS_COMPLETE",
            category="keystroke",
            evidence={
                "files": [
                    {
                        "filename": info.filename,
                        "missing_columns": missing,
                    }
                    for info, missing in named_missing_columns
                ]
            },
        )

    if not candidates:
        warnings.extend(
            [
                "missing_keystroke_log",
                "missing_final_keystroke_timepoint",
            ]
        )
        return KeystrokeResult(None, None, None, warnings)

    if len(candidates) > 1:
        names = ", ".join(info.filename for info, _ in candidates)
        raise PackageError(
            f"Multiple keystroke log candidates: {names}",
            rule="KEYSTROKE_FILE_PRESENT_UNIQUE",
            category="keystroke",
            evidence={
                "candidate_count": len(candidates),
                "candidates": [info.filename for info, _ in candidates],
            },
        )

    info, rows = candidates[0]
    writing_end, endpoint_warnings = parse_writing_end(
        rows,
        participant,
        task,
        condition,
    )
    warnings.extend(endpoint_warnings)

    if writing_end is None:
        warnings.append("missing_final_keystroke_timepoint")

    return KeystrokeResult(info, len(rows), writing_end, warnings)


def count_words(text: str) -> int:
    """Reproduce Gamma's observed whitespace-based word count."""
    return len(re.findall(r"\S+", text))


def utf16_length(text: str) -> int:
    """Return the number of UTF-16 code units used by browser offsets."""
    return len(text.encode("utf-16-le")) // 2


def python_index_from_utf16(text: str, offset: int) -> int | None:
    """Translate a browser UTF-16 offset to a Python string index."""
    if offset < 0:
        return None

    consumed = 0
    for index, character in enumerate(text):
        if consumed == offset:
            return index
        consumed += 2 if ord(character) > 0xFFFF else 1
        if consumed > offset:
            return None

    return len(text) if consumed == offset else None


def text_sha256(text: str) -> str:
    """Hash Unicode text using its UTF-8 representation."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def first_text_difference(first: str, second: str) -> dict[str, Any] | None:
    """Describe the first exact character difference between two texts."""
    shared_length = min(len(first), len(second))
    index = next(
        (
            position
            for position in range(shared_length)
            if first[position] != second[position]
        ),
        shared_length,
    )
    if index == shared_length and len(first) == len(second):
        return None

    return {
        "python_index": index,
        "utf16_offset": utf16_length(first[:index]),
        "reconstructed_character": (
            None if index >= len(first) else first[index]
        ),
        "final_essay_character": (
            None if index >= len(second) else second[index]
        ),
    }


def replay_keystroke_text(
    rows: list[dict[str, str]],
) -> KeystrokeReplayResult:
    """Rebuild text from confirmed beforeinput/input edit pairs."""
    text = ""
    applied_events = 0
    missing_beforeinput: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []
    invalid_cursors: list[dict[str, Any]] = []
    large_insertions: list[dict[str, Any]] = []
    bulk_replacements: list[dict[str, Any]] = []
    paste_or_drop_events: list[dict[str, Any]] = []

    insertion_types = {
        "insertText",
        "insertCompositionText",
        "insertReplacementText",
        "insertFromPaste",
        "insertFromPasteAsQuotation",
        "insertFromDrop",
        "insertFromYank",
    }
    line_break_types = {"insertLineBreak", "insertParagraph"}
    selection_delete_types = {
        "deleteByCut",
        "deleteByDrag",
        "deleteContent",
    }
    paste_drop_types = {
        "insertFromPaste",
        "insertFromPasteAsQuotation",
        "insertFromDrop",
        "insertFromYank",
    }

    for input_index, input_row in enumerate(rows):
        if input_row.get("event") != "input":
            continue

        before_index = input_index - 1
        before_row = rows[before_index] if before_index >= 0 else None
        chain_matches = (
            before_row is not None
            and before_row.get("event") == "beforeinput"
            and before_row.get("inputType") == input_row.get("inputType")
            and before_row.get("data") == input_row.get("data")
        )
        if not chain_matches:
            missing_beforeinput.append(
                {
                    "csv_row": input_index + 2,
                    "time_ms": input_row.get("time_ms", ""),
                    "input_type": input_row.get("inputType", ""),
                    "data_sha256": text_sha256(input_row.get("data", "")),
                    "preceding_event": (
                        None if before_row is None else before_row.get("event")
                    ),
                }
            )
            continue

        assert before_row is not None
        input_type = before_row.get("inputType", "")
        csv_row = before_index + 2
        try:
            cursor_start = int(before_row.get("cursor_start", ""))
            cursor_end = int(before_row.get("cursor_end", ""))
        except (TypeError, ValueError):
            invalid_cursors.append(
                {
                    "csv_row": csv_row,
                    "input_csv_row": input_index + 2,
                    "cursor_start": before_row.get("cursor_start"),
                    "cursor_end": before_row.get("cursor_end"),
                    "text_utf16_length": utf16_length(text),
                    "reason": "cursor_not_an_integer",
                }
            )
            continue

        start_index = python_index_from_utf16(text, cursor_start)
        end_index = python_index_from_utf16(text, cursor_end)
        if (
            start_index is None
            or end_index is None
            or cursor_start > cursor_end
        ):
            invalid_cursors.append(
                {
                    "csv_row": csv_row,
                    "input_csv_row": input_index + 2,
                    "cursor_start": cursor_start,
                    "cursor_end": cursor_end,
                    "text_utf16_length": utf16_length(text),
                    "reason": "cursor_out_of_range_or_split_surrogate",
                }
            )
            continue

        replacement_start = start_index
        replacement_end = end_index
        inserted_text = ""
        if input_type in insertion_types:
            inserted_text = before_row.get("data", "")
        elif input_type in line_break_types:
            inserted_text = "\n"
        elif input_type == "deleteContentBackward":
            if start_index == end_index and start_index > 0:
                replacement_start -= 1
        elif input_type == "deleteContentForward":
            if start_index == end_index and end_index < len(text):
                replacement_end += 1
        elif input_type in selection_delete_types:
            pass
        else:
            unsupported.append(
                {
                    "csv_row": csv_row,
                    "input_csv_row": input_index + 2,
                    "time_ms": before_row.get("time_ms", ""),
                    "input_type": input_type,
                }
            )
            continue

        removed_text = text[replacement_start:replacement_end]
        event_evidence = {
            "csv_row": csv_row,
            "input_csv_row": input_index + 2,
            "time_ms": before_row.get("time_ms", ""),
            "input_type": input_type,
            "cursor_start": cursor_start,
            "cursor_end": cursor_end,
            "inserted_character_count": len(inserted_text),
            "removed_character_count": len(removed_text),
            "inserted_text_sha256": text_sha256(inserted_text),
        }

        if len(inserted_text) >= LARGE_INSERTION_THRESHOLD_CHARS:
            large_insertions.append(event_evidence.copy())

        if (
            inserted_text
            and len(removed_text) >= BULK_REPLACEMENT_THRESHOLD_CHARS
        ):
            bulk_replacements.append(event_evidence.copy())

        if input_type in paste_drop_types:
            internal_occurrences = text.count(inserted_text) if inserted_text else 0
            source_assessment = (
                "possibly_internal_copy"
                if internal_occurrences > 0
                else "source_unknown_or_external"
            )
            paste_evidence = event_evidence.copy()
            paste_evidence.update(
                {
                    "source_assessment": source_assessment,
                    "preexisting_occurrences": internal_occurrences,
                }
            )
            paste_or_drop_events.append(paste_evidence)

        text = (
            text[:replacement_start]
            + inserted_text
            + text[replacement_end:]
        )
        applied_events += 1

    return KeystrokeReplayResult(
        text=text,
        applied_events=applied_events,
        input_events_without_beforeinput=missing_beforeinput,
        unsupported_events=unsupported,
        invalid_cursor_events=invalid_cursors,
        large_insertions=large_insertions,
        bulk_replacements=bulk_replacements,
        paste_or_drop_events=paste_or_drop_events,
    )


def parse_snapshot_text(
    text: str,
    participant: str,
    task: int,
    condition: str,
) -> tuple[list[Snapshot], list[str]]:
    """Parse all observed Minute blocks from one snapshot file."""
    warnings: list[str] = []
    normalised = normalise_newlines(text)
    headers = list(SNAPSHOT_HEADER_RE.finditer(normalised))

    exact_header_lines = {match.group(0) for match in headers}
    malformed_headers = [
        match.group(0)
        for match in SNAPSHOT_HEADER_LIKE_RE.finditer(normalised)
        if match.group(0) not in exact_header_lines
    ]

    if malformed_headers:
        warnings.append(f"malformed_minute_header(count={len(malformed_headers)})")

    if not headers:
        return [], warnings

    preamble = normalised[: headers[0].start()]
    preamble_fields = {
        key.lower().replace(" ", "_"): value
        for key, value in SNAPSHOT_PREAMBLE_RE.findall(preamble)
    }

    expected_preamble = {
        "subject_code": participant,
        "task_number": str(task),
        "topic_code": condition,
    }

    for key, expected in expected_preamble.items():
        if key not in preamble_fields:
            continue

        observed = preamble_fields[key]

        if key == "topic_code":
            observed = observed.lower()

        if observed != expected:
            warnings.append(f"snapshot_{key}_mismatch")

    snapshots: list[Snapshot] = []

    for index, header in enumerate(headers):
        minute = int(header.group(1))
        block_end = (
            headers[index + 1].start()
            if index + 1 < len(headers)
            else len(normalised)
        )
        block = normalised[header.end() : block_end]
        word_count_match = SNAPSHOT_WORD_COUNT_RE.match(block)

        if word_count_match is None:
            declared_word_count = None
            body = block.lstrip("\n").rstrip("\n")
            warnings.append(
                f"missing_declared_snapshot_word_count(minute={minute})"
            )
        else:
            declared_word_count = int(word_count_match.group(1))
            body = block[word_count_match.end() :].rstrip("\n")

        cumulative_word_count = count_words(body)

        if (
            declared_word_count is not None
            and declared_word_count != cumulative_word_count
        ):
            warnings.append(
                "snapshot_word_count_mismatch"
                f"(minute={minute},"
                f"declared={declared_word_count},"
                f"computed={cumulative_word_count})"
            )

        snapshots.append(
            Snapshot(
                sequence=index + 1,
                minute=minute,
                text=body,
                declared_word_count=declared_word_count,
                cumulative_word_count=cumulative_word_count,
            )
        )

    minutes = [snapshot.minute for snapshot in snapshots]
    duplicate_minutes = sorted(
        minute for minute in set(minutes) if minutes.count(minute) > 1
    )

    if duplicate_minutes:
        values = ",".join(str(value) for value in duplicate_minutes)
        warnings.append(f"duplicate_minute_number(minutes={values})")

    expected_minutes = list(range(1, len(minutes) + 1))

    if minutes != expected_minutes:
        warnings.append("nonsequential_snapshot_minutes")

    return snapshots, warnings


def find_snapshots(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    final_info: zipfile.ZipInfo,
    participant: str,
    task: int,
    condition: str,
) -> tuple[zipfile.ZipInfo | None, list[Snapshot], list[str]]:
    """Identify, parse, and validate the minute snapshot TXT."""
    warnings: list[str] = []
    valid_candidates: list[tuple[zipfile.ZipInfo, str]] = []
    named_without_headers: list[tuple[zipfile.ZipInfo, str]] = []

    for info in members:
        if info.filename == final_info.filename:
            continue

        if not info.filename.lower().endswith(".txt"):
            continue

        basename = PurePosixPath(
            normalise_member_name(info.filename)
        ).name.lower()

        try:
            text = archive.read(info).decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            if "snapshot" in basename:
                raise PackageError(
                    f"Invalid snapshot UTF-8 in {info.filename}: {exc}",
                    rule="SNAPSHOT_UTF8_VALID",
                    category="snapshot",
                    evidence={"filename": info.filename, "error": str(exc)},
                ) from exc

            continue

        normalised = normalise_newlines(text)

        if SNAPSHOT_HEADER_RE.search(normalised):
            valid_candidates.append((info, text))
        elif "snapshot" in basename:
            named_without_headers.append((info, text))

    if len(valid_candidates) > 1:
        names = ", ".join(info.filename for info, _ in valid_candidates)
        raise PackageError(
            f"Multiple minute snapshot candidates: {names}",
            rule="SNAPSHOT_FILE_PRESENT_UNIQUE",
            category="snapshot",
            evidence={
                "candidate_count": len(valid_candidates),
                "candidates": [info.filename for info, _ in valid_candidates],
            },
        )

    if valid_candidates:
        info, text = valid_candidates[0]
        snapshots, parse_warnings = parse_snapshot_text(
            text,
            participant,
            task,
            condition,
        )
        warnings.extend(parse_warnings)

        if named_without_headers:
            warnings.append(
                "additional_unparsed_snapshot_file"
                f"(count={len(named_without_headers)})"
            )

        return info, snapshots, warnings

    if len(named_without_headers) > 1:
        names = ", ".join(info.filename for info, _ in named_without_headers)
        raise PackageError(
            f"Multiple unparseable minute snapshot candidates: {names}",
            rule="SNAPSHOT_FILE_PRESENT_UNIQUE",
            category="snapshot",
            evidence={
                "candidate_count": len(named_without_headers),
                "candidates": [
                    info.filename for info, _ in named_without_headers
                ],
            },
        )

    if named_without_headers:
        info, text = named_without_headers[0]
        _, parse_warnings = parse_snapshot_text(
            text,
            participant,
            task,
            condition,
        )
        warnings.extend(parse_warnings)
        warnings.append("snapshot_file_has_no_valid_minute_blocks")
        return info, [], warnings

    warnings.append("missing_minute_snapshots")
    return None, [], warnings


def find_chat_log(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    metadata_info: zipfile.ZipInfo,
    *,
    content_applicable: bool,
) -> tuple[zipfile.ZipInfo | None, dict[str, Any] | None]:
    """Identify an AI chat JSON without validating irrelevant content."""
    candidates: list[tuple[zipfile.ZipInfo, dict[str, Any] | None]] = []

    for info in members:
        if info.filename == metadata_info.filename:
            continue

        if not info.filename.lower().endswith(".json"):
            continue

        try:
            value = json.loads(archive.read(info).decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            if "chat" in info.filename.lower():
                if content_applicable:
                    raise PackageError(
                        f"Invalid chat JSON: {info.filename}",
                        rule="AI_CHAT_JSON_VALID",
                        category="ai_chat",
                        evidence={"filename": info.filename},
                    )
                candidates.append((info, None))

            continue

        if isinstance(value, dict) and isinstance(value.get("chat"), list):
            candidates.append((info, value))
        elif not content_applicable and "chat" in info.filename.lower():
            candidates.append((info, None))

    if len(candidates) > 1:
        names = ", ".join(info.filename for info, _ in candidates)
        raise PackageError(
            f"Multiple AI chat log candidates: {names}",
            rule="AI_CHAT_FILE_PRESENT_UNIQUE",
            category="ai_chat",
            evidence={
                "candidate_count": len(candidates),
                "candidates": [info.filename for info, _ in candidates],
            },
        )

    if candidates:
        return candidates[0]

    return None, None


def validate_identity(
    metadata: dict[str, Any],
) -> tuple[str, int, str, str, str]:
    """Read and validate identity fields used in generated outputs."""
    participant = str(metadata["subject_code"])

    try:
        task = int(metadata["task_number"])
    except (TypeError, ValueError) as exc:
        raise PackageError(
            "task_number is not an integer",
            rule="METADATA_IDENTITY_VALID",
            category="identity",
            evidence={"field": "task_number", "value": metadata["task_number"]},
        ) from exc

    condition = str(metadata["topic_code"]).lower()
    planning_mode = str(metadata["planning_mode"]).lower()
    familiarity = str(metadata["familiarity"]).lower()

    if not SAFE_COMPONENT_RE.fullmatch(participant):
        raise PackageError(
            f"Unsafe subject_code for output filename: {participant!r}",
            rule="METADATA_IDENTITY_VALID",
            category="identity",
            evidence={"field": "subject_code", "value": participant},
        )

    if task < 1:
        raise PackageError(
            f"Invalid task_number: {task}",
            rule="METADATA_IDENTITY_VALID",
            category="identity",
            evidence={"field": "task_number", "value": task},
        )

    if not SAFE_COMPONENT_RE.fullmatch(condition):
        raise PackageError(
            f"Unsafe topic_code for output filename: {condition!r}",
            rule="METADATA_IDENTITY_VALID",
            category="identity",
            evidence={"field": "topic_code", "value": condition},
        )

    return participant, task, condition, planning_mode, familiarity


def metadata_value(metadata: dict[str, Any], key: str) -> Any:
    """Return a CSV-safe metadata value."""
    value = metadata.get(key, "")
    return "" if value is None else value


def research_fingerprint(
    archive: zipfile.ZipFile,
    role_members: Iterable[tuple[str, zipfile.ZipInfo | None]],
) -> str:
    """Hash identified research files, independent of their filenames."""
    digest = hashlib.sha256()

    for role, info in role_members:
        role_bytes = role.encode("ascii")
        content = b"" if info is None else archive.read(info)

        digest.update(len(role_bytes).to_bytes(4, "big"))
        digest.update(role_bytes)
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)

    return digest.hexdigest()


def decimal_csv(value: Decimal) -> str:
    """Render a Decimal without exponent notation or unnecessary zeros."""
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def build_qc_rows(
    archive: zipfile.ZipFile,
    all_members: list[zipfile.ZipInfo],
    members: list[zipfile.ZipInfo],
    metadata_info: zipfile.ZipInfo,
    metadata: dict[str, Any],
    final_info: zipfile.ZipInfo,
    essay_text: str,
    keystroke: KeystrokeResult,
    snapshot_info: zipfile.ZipInfo | None,
    snapshots: list[Snapshot],
    chat_info: zipfile.ZipInfo | None,
    chat_data: dict[str, Any] | None,
    participant: str,
    task: int,
    condition: str,
    planning_mode: str,
    familiarity: str,
    source_zip: str,
) -> list[dict[str, Any]]:
    """Run structured package, identity, content, and timing QC rules."""
    rows: list[dict[str, Any]] = []

    def add(
        rule: str,
        category: str,
        severity: str,
        result: str,
        **evidence: Any,
    ) -> None:
        rows.append(
            make_qc_row(
                source_zip,
                participant,
                task,
                condition,
                rule,
                category,
                severity,
                result,
                **evidence,
            )
        )

    def scalar_match(
        rule: str,
        source: str,
        field: str,
        observed: Any,
        expected: Any,
        *,
        normalise_lower: bool = False,
    ) -> None:
        if observed is None or observed == "":
            add(
                rule,
                "identity",
                "FLAG",
                "FAIL",
                source=source,
                field=field,
                expected=expected,
                observed=None,
                reason="field_missing",
            )
            return

        observed_text = str(observed)
        expected_text = str(expected)
        if normalise_lower:
            observed_text = observed_text.lower()
            expected_text = expected_text.lower()
        matches = observed_text == expected_text
        add(
            rule,
            "identity",
            "FLAG",
            "PASS" if matches else "FAIL",
            source=source,
            field=field,
            expected=expected_text,
            observed=observed_text,
        )

    def set_match(
        rule: str,
        field: str,
        values: set[str],
        expected: Any,
        *,
        normalise_lower: bool = False,
    ) -> None:
        if not values:
            add(
                rule,
                "identity",
                "FLAG",
                "NOT_CHECKED",
                source="keystroke",
                field=field,
                reason="no_keystroke_rows",
            )
            return

        observed_values = {
            value.lower() if normalise_lower else value for value in values
        }
        expected_text = str(expected)
        if normalise_lower:
            expected_text = expected_text.lower()
        matches = observed_values == {expected_text}
        add(
            rule,
            "identity",
            "FLAG",
            "PASS" if matches else "FAIL",
            source="keystroke",
            field=field,
            expected=expected_text,
            observed=sorted(observed_values),
        )

    ignored_members = [
        info.filename for info in all_members if not is_research_member(info)
    ]
    unsafe_members = [
        info.filename for info in members if unsafe_member_path(info.filename)
    ]

    add(
        "ARCHIVE_READABLE",
        "archive",
        "ERROR",
        "PASS",
        member_count=len(all_members),
    )
    add(
        "ARCHIVE_CRC_VALID",
        "archive",
        "ERROR",
        "PASS",
        checked_members=len(all_members),
    )
    add(
        "ARCHIVE_SYSTEM_ENTRIES_ABSENT",
        "archive",
        "INFO",
        "FAIL" if ignored_members else "PASS",
        count=len(ignored_members),
        members=ignored_members,
    )
    add(
        "ARCHIVE_MEMBER_PATHS_SAFE",
        "archive",
        "WARNING",
        "FAIL" if unsafe_members else "PASS",
        count=len(unsafe_members),
        members=unsafe_members,
        extraction_performed=False,
    )
    add(
        "METADATA_PRESENT_UNIQUE",
        "archive",
        "ERROR",
        "PASS",
        filename=metadata_info.filename,
    )
    add(
        "METADATA_JSON_VALID",
        "archive",
        "ERROR",
        "PASS",
        filename=metadata_info.filename,
    )
    add(
        "FINAL_ESSAY_PRESENT_UNIQUE",
        "archive",
        "ERROR",
        "PASS",
        filename=final_info.filename,
    )
    add(
        "FINAL_ESSAY_UTF8_VALID",
        "archive",
        "ERROR",
        "PASS",
        filename=final_info.filename,
    )

    derived_condition = planning_mode + familiarity
    add(
        "METADATA_CONDITION_CODE_MATCH",
        "identity",
        "FLAG",
        "PASS" if condition == derived_condition else "FAIL",
        topic_code=condition,
        planning_mode=planning_mode,
        familiarity=familiarity,
        derived_condition=derived_condition,
    )
    add(
        "METADATA_PLANNING_MODE_VALID",
        "identity",
        "FLAG",
        "PASS" if planning_mode in {"a", "i", "n"} else "FAIL",
        observed=planning_mode,
        allowed=["a", "i", "n"],
    )
    add(
        "METADATA_FAMILIARITY_VALID",
        "identity",
        "FLAG",
        "PASS" if familiarity in {"h", "l"} else "FAIL",
        observed=familiarity,
        allowed=["h", "l"],
    )

    metadata_word_count: int | None = None
    raw_metadata_word_count = metadata.get("final_word_count")
    try:
        metadata_word_count = int(raw_metadata_word_count)
        metadata_wc_valid = metadata_word_count >= 0
    except (TypeError, ValueError):
        metadata_wc_valid = False

    add(
        "FINAL_WORD_COUNT_METADATA_VALID",
        "word_count",
        "FLAG",
        "PASS" if metadata_wc_valid else "FAIL",
        observed=raw_metadata_word_count,
    )
    if not metadata_wc_valid:
        metadata_word_count = None

    essay_word_count = count_words(essay_text)
    keystroke_word_count = (
        None
        if keystroke.writing_end is None
        else keystroke.writing_end.cumulative_word_count
    )

    def word_count_pair(
        rule: str,
        first_name: str,
        first_value: int | None,
        second_name: str,
        second_value: int | None,
    ) -> None:
        if first_value is None or second_value is None:
            add(
                rule,
                "word_count",
                "FLAG",
                "NOT_CHECKED",
                first_source=first_name,
                first_value=first_value,
                second_source=second_name,
                second_value=second_value,
                reason="one_or_more_sources_unavailable",
            )
            return

        matches = first_value == second_value
        add(
            rule,
            "word_count",
            "FLAG",
            "PASS" if matches else "FAIL",
            first_source=first_name,
            first_value=first_value,
            second_source=second_name,
            second_value=second_value,
            difference=first_value - second_value,
        )

    word_count_pair(
        "WORD_COUNT_METADATA_VS_FINAL_ESSAY",
        "metadata.final_word_count",
        metadata_word_count,
        "final_essay_recalculated",
        essay_word_count,
    )
    word_count_pair(
        "WORD_COUNT_METADATA_VS_KEYSTROKE",
        "metadata.final_word_count",
        metadata_word_count,
        "keystroke.writing_end.word_count",
        keystroke_word_count,
    )
    word_count_pair(
        "WORD_COUNT_FINAL_ESSAY_VS_KEYSTROKE",
        "final_essay_recalculated",
        essay_word_count,
        "keystroke.writing_end.word_count",
        keystroke_word_count,
    )
    add(
        "PROTOCOL_FINAL_WORD_COUNT_200_250",
        "protocol",
        "FLAG",
        "PASS" if 200 <= essay_word_count <= 250 else "FAIL",
        observed=essay_word_count,
        minimum=200,
        maximum=250,
        source="final_essay_recalculated",
    )

    add(
        "SNAPSHOT_FILE_PRESENT",
        "snapshot",
        "WARNING",
        "PASS" if snapshot_info is not None else "FAIL",
        filename=None if snapshot_info is None else snapshot_info.filename,
    )
    add(
        "SNAPSHOT_UTF8_VALID",
        "snapshot",
        "ERROR",
        "PASS" if snapshot_info is not None else "NOT_CHECKED",
        filename=None if snapshot_info is None else snapshot_info.filename,
        reason=None if snapshot_info is not None else "snapshot_file_unavailable",
    )

    if snapshot_info is None:
        for rule in (
            "SNAPSHOT_HEADERS_WELL_FORMED",
            "SNAPSHOT_MINUTES_START_AT_1",
            "SNAPSHOT_MINUTES_UNIQUE",
            "SNAPSHOT_MINUTES_CONTIGUOUS",
            "SNAPSHOT_DECLARED_WORD_COUNTS_PRESENT",
            "SNAPSHOT_WORD_COUNTS_MATCH",
            "SNAPSHOT_COUNT_MATCH_WRITING_TIME",
            "SNAPSHOT_SUBJECT_CODE_MATCH",
            "SNAPSHOT_TASK_NUMBER_MATCH",
            "SNAPSHOT_TOPIC_CODE_MATCH",
            "LAST_SNAPSHOT_MATCHES_FINAL_ESSAY",
        ):
            category = "identity" if "_MATCH" in rule and rule.startswith(
                "SNAPSHOT_"
            ) and rule not in {
                "SNAPSHOT_WORD_COUNTS_MATCH",
                "SNAPSHOT_COUNT_MATCH_WRITING_TIME",
            } else "snapshot"
            add(
                rule,
                category,
                "FLAG" if category == "identity" else "WARNING",
                "NOT_CHECKED",
                reason="snapshot_file_unavailable",
            )
    else:
        snapshot_text = archive.read(snapshot_info).decode("utf-8-sig")
        snapshot_normalised = normalise_newlines(snapshot_text)
        headers = list(SNAPSHOT_HEADER_RE.finditer(snapshot_normalised))
        exact_header_lines = {match.group(0) for match in headers}
        malformed_headers = [
            match.group(0)
            for match in SNAPSHOT_HEADER_LIKE_RE.finditer(snapshot_normalised)
            if match.group(0) not in exact_header_lines
        ]
        headers_valid = bool(headers) and not malformed_headers
        add(
            "SNAPSHOT_HEADERS_WELL_FORMED",
            "snapshot",
            "WARNING",
            "PASS" if headers_valid else "FAIL",
            valid_header_count=len(headers),
            malformed_headers=malformed_headers,
        )

        minutes = [snapshot.minute for snapshot in snapshots]
        if not minutes:
            for rule in (
                "SNAPSHOT_MINUTES_START_AT_1",
                "SNAPSHOT_MINUTES_UNIQUE",
                "SNAPSHOT_MINUTES_CONTIGUOUS",
                "SNAPSHOT_DECLARED_WORD_COUNTS_PRESENT",
                "SNAPSHOT_WORD_COUNTS_MATCH",
            ):
                add(
                    rule,
                    "snapshot",
                    "WARNING",
                    "NOT_CHECKED",
                    reason="no_valid_minute_blocks",
                )
        else:
            add(
                "SNAPSHOT_MINUTES_START_AT_1",
                "snapshot",
                "WARNING",
                "PASS" if minutes[0] == 1 else "FAIL",
                first_observed_minute=minutes[0],
            )
            duplicates = sorted(
                minute
                for minute, count in Counter(minutes).items()
                if count > 1
            )
            add(
                "SNAPSHOT_MINUTES_UNIQUE",
                "snapshot",
                "WARNING",
                "PASS" if not duplicates else "FAIL",
                duplicate_minutes=duplicates,
            )
            expected_minutes = list(range(1, len(minutes) + 1))
            add(
                "SNAPSHOT_MINUTES_CONTIGUOUS",
                "snapshot",
                "WARNING",
                "PASS" if minutes == expected_minutes else "FAIL",
                observed=minutes,
                expected=expected_minutes,
            )

            missing_declared = [
                snapshot.minute
                for snapshot in snapshots
                if snapshot.declared_word_count is None
            ]
            if missing_declared:
                for minute in missing_declared:
                    add(
                        "SNAPSHOT_DECLARED_WORD_COUNTS_PRESENT",
                        "snapshot",
                        "WARNING",
                        "FAIL",
                        minute=minute,
                        declared_word_count=None,
                    )
            else:
                add(
                    "SNAPSHOT_DECLARED_WORD_COUNTS_PRESENT",
                    "snapshot",
                    "WARNING",
                    "PASS",
                    checked_minutes=len(snapshots),
                )

            mismatches = [
                snapshot
                for snapshot in snapshots
                if snapshot.declared_word_count is not None
                and snapshot.declared_word_count
                != snapshot.cumulative_word_count
            ]
            if mismatches:
                for snapshot in mismatches:
                    add(
                        "SNAPSHOT_WORD_COUNTS_MATCH",
                        "snapshot",
                        "WARNING",
                        "FAIL",
                        minute=snapshot.minute,
                        declared=snapshot.declared_word_count,
                        recalculated=snapshot.cumulative_word_count,
                    )
            else:
                add(
                    "SNAPSHOT_WORD_COUNTS_MATCH",
                    "snapshot",
                    "WARNING",
                    "PASS",
                    checked_minutes=len(snapshots),
                )

        preamble_end = headers[0].start() if headers else len(snapshot_normalised)
        preamble = snapshot_normalised[:preamble_end]
        preamble_fields = {
            key.lower().replace(" ", "_"): value
            for key, value in SNAPSHOT_PREAMBLE_RE.findall(preamble)
        }
        scalar_match(
            "SNAPSHOT_SUBJECT_CODE_MATCH",
            "snapshot",
            "subject_code",
            preamble_fields.get("subject_code"),
            participant,
        )
        scalar_match(
            "SNAPSHOT_TASK_NUMBER_MATCH",
            "snapshot",
            "task_number",
            preamble_fields.get("task_number"),
            task,
        )
        scalar_match(
            "SNAPSHOT_TOPIC_CODE_MATCH",
            "snapshot",
            "topic_code",
            preamble_fields.get("topic_code"),
            condition,
            normalise_lower=True,
        )

        actual_writing_ms: Decimal | None = None
        raw_writing_ms = metadata.get("actual_writing_time_ms")
        try:
            actual_writing_ms = Decimal(str(raw_writing_ms))
            if not actual_writing_ms.is_finite() or actual_writing_ms < 0:
                actual_writing_ms = None
        except (InvalidOperation, ValueError):
            actual_writing_ms = None

        if actual_writing_ms is None:
            add(
                "SNAPSHOT_COUNT_MATCH_WRITING_TIME",
                "snapshot",
                "WARNING",
                "NOT_CHECKED",
                observed_snapshot_count=len(snapshots),
                actual_writing_time_ms=raw_writing_ms,
                reason="invalid_or_missing_actual_writing_time_ms",
            )
        else:
            expected_snapshot_count = int(
                actual_writing_ms // Decimal(60_000)
            )
            add(
                "SNAPSHOT_COUNT_MATCH_WRITING_TIME",
                "snapshot",
                "WARNING",
                "PASS"
                if len(snapshots) == expected_snapshot_count
                else "FAIL",
                observed_snapshot_count=len(snapshots),
                expected_snapshot_count=expected_snapshot_count,
                actual_writing_time_ms=decimal_csv(actual_writing_ms),
            )

        if snapshots:
            text_matches = (
                normalise_text_for_comparison(snapshots[-1].text)
                == normalise_text_for_comparison(essay_text)
            )
            add(
                "LAST_SNAPSHOT_MATCHES_FINAL_ESSAY",
                "snapshot",
                "INFO",
                "PASS" if text_matches else "FAIL",
                last_snapshot_minute=snapshots[-1].minute,
                last_snapshot_word_count=snapshots[-1].cumulative_word_count,
                final_essay_word_count=essay_word_count,
                text_equal=text_matches,
            )
        else:
            add(
                "LAST_SNAPSHOT_MATCHES_FINAL_ESSAY",
                "snapshot",
                "INFO",
                "NOT_CHECKED",
                reason="no_valid_minute_blocks",
            )

    add(
        "KEYSTROKE_FILE_PRESENT",
        "keystroke",
        "WARNING",
        "PASS" if keystroke.info is not None else "FAIL",
        filename=None if keystroke.info is None else keystroke.info.filename,
    )
    add(
        "KEYSTROKE_COLUMNS_COMPLETE",
        "keystroke",
        "ERROR",
        "PASS" if keystroke.info is not None else "NOT_CHECKED",
        reason=None if keystroke.info is not None else "keystroke_file_unavailable",
        required_columns=sorted(KEYSTROKE_REQUIRED_COLUMNS),
    )

    keystroke_rows: list[dict[str, str]] = []
    if keystroke.info is not None:
        keystroke_text = archive.read(keystroke.info).decode("utf-8-sig")
        keystroke_rows = list(
            csv.DictReader(io.StringIO(keystroke_text, newline=""))
        )

    if not keystroke_rows:
        for rule, category, severity in (
            ("KEYSTROKE_SUBJECT_CODE_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_TASK_NUMBER_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_TOPIC_CODE_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_CONDITION_LABEL_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_PLANNING_MODE_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_FAMILIARITY_MATCH", "identity", "FLAG"),
            ("KEYSTROKE_WRITING_START_UNIQUE", "keystroke", "WARNING"),
            ("KEYSTROKE_WRITING_START_AT_ZERO", "keystroke", "WARNING"),
            ("KEYSTROKE_WRITING_END_UNIQUE", "keystroke", "WARNING"),
            ("KEYSTROKE_TIME_VALUES_VALID", "keystroke", "ERROR"),
            ("KEYSTROKE_TIME_MONOTONIC", "keystroke", "WARNING"),
            ("KEYSTROKE_DUPLICATE_EVENTS_ABSENT", "keystroke", "WARNING"),
            (
                "KEYSTROKE_INPUT_EVENTS_HAVE_BEFOREINPUT",
                "keystroke",
                "WARNING",
            ),
            (
                "KEYSTROKE_REPLAY_EVENTS_INTERPRETABLE",
                "keystroke",
                "WARNING",
            ),
            (
                "KEYSTROKE_RECONSTRUCTED_FINAL_TEXT_MATCH",
                "keystroke",
                "FLAG",
            ),
            (
                "KEYSTROKE_UNUSUALLY_LARGE_INSERTIONS",
                "editing",
                "FLAG",
            ),
            ("KEYSTROKE_BULK_REPLACEMENTS", "editing", "FLAG"),
            ("KEYSTROKE_PASTE_OR_DROP_INSERTION", "editing", "INFO"),
            ("KEYSTROKE_NO_EVENTS_AFTER_WRITING_END", "keystroke", "WARNING"),
            ("KEYSTROKE_WRITING_END_TIME_MATCH_METADATA", "keystroke", "WARNING"),
            ("KEYSTROKE_WRITING_END_AFTER_LAST_SNAPSHOT", "keystroke", "WARNING"),
        ):
            add(
                rule,
                category,
                severity,
                "NOT_CHECKED",
                reason="keystroke_log_unavailable_or_empty",
            )
    else:
        set_match(
            "KEYSTROKE_SUBJECT_CODE_MATCH",
            "subject_code",
            {str(row.get("subject_code", "")) for row in keystroke_rows},
            participant,
        )
        set_match(
            "KEYSTROKE_TASK_NUMBER_MATCH",
            "task_number",
            {str(row.get("task_number", "")) for row in keystroke_rows},
            task,
        )
        set_match(
            "KEYSTROKE_TOPIC_CODE_MATCH",
            "topic_code",
            {str(row.get("topic_code", "")) for row in keystroke_rows},
            condition,
            normalise_lower=True,
        )
        set_match(
            "KEYSTROKE_CONDITION_LABEL_MATCH",
            "condition",
            {str(row.get("condition", "")) for row in keystroke_rows},
            metadata.get("condition", ""),
        )
        set_match(
            "KEYSTROKE_PLANNING_MODE_MATCH",
            "planning_mode",
            {str(row.get("planning_mode", "")) for row in keystroke_rows},
            planning_mode,
            normalise_lower=True,
        )
        set_match(
            "KEYSTROKE_FAMILIARITY_MATCH",
            "familiarity",
            {str(row.get("familiarity", "")) for row in keystroke_rows},
            familiarity,
            normalise_lower=True,
        )

        start_indices = [
            index
            for index, row in enumerate(keystroke_rows)
            if row.get("event") == "writing_start"
        ]
        end_indices = [
            index
            for index, row in enumerate(keystroke_rows)
            if row.get("event") == "writing_end"
        ]
        add(
            "KEYSTROKE_WRITING_START_UNIQUE",
            "keystroke",
            "WARNING",
            "PASS" if len(start_indices) == 1 else "FAIL",
            count=len(start_indices),
            csv_rows=[index + 2 for index in start_indices],
        )
        if len(start_indices) == 1:
            raw_start_time = keystroke_rows[start_indices[0]].get("time_ms", "")
            try:
                start_time = Decimal(raw_start_time)
                start_valid = start_time.is_finite() and start_time == 0
            except (InvalidOperation, ValueError):
                start_valid = False
            add(
                "KEYSTROKE_WRITING_START_AT_ZERO",
                "keystroke",
                "WARNING",
                "PASS" if start_valid else "FAIL",
                observed_time_ms=raw_start_time,
                expected_time_ms="0",
            )
        else:
            add(
                "KEYSTROKE_WRITING_START_AT_ZERO",
                "keystroke",
                "WARNING",
                "NOT_CHECKED",
                reason="writing_start_not_unique",
            )

        add(
            "KEYSTROKE_WRITING_END_UNIQUE",
            "keystroke",
            "WARNING",
            "PASS" if len(end_indices) == 1 else "FAIL",
            count=len(end_indices),
            csv_rows=[index + 2 for index in end_indices],
        )

        parsed_times: list[Decimal] = []
        invalid_time_rows: list[dict[str, Any]] = []
        for index, row in enumerate(keystroke_rows):
            raw_time = row.get("time_ms", "")
            try:
                parsed_time = Decimal(raw_time)
                if not parsed_time.is_finite() or parsed_time < 0:
                    raise InvalidOperation
                parsed_times.append(parsed_time)
            except (InvalidOperation, ValueError):
                invalid_time_rows.append(
                    {"csv_row": index + 2, "value": raw_time}
                )

        add(
            "KEYSTROKE_TIME_VALUES_VALID",
            "keystroke",
            "ERROR",
            "PASS" if not invalid_time_rows else "FAIL",
            invalid_count=len(invalid_time_rows),
            invalid_rows=invalid_time_rows[:20],
        )

        if invalid_time_rows:
            add(
                "KEYSTROKE_TIME_MONOTONIC",
                "keystroke",
                "WARNING",
                "NOT_CHECKED",
                reason="one_or_more_invalid_time_values",
            )
        else:
            backwards = [
                {
                    "csv_row": index + 2,
                    "previous_ms": decimal_csv(parsed_times[index - 1]),
                    "current_ms": decimal_csv(parsed_times[index]),
                }
                for index in range(1, len(parsed_times))
                if parsed_times[index] < parsed_times[index - 1]
            ]
            add(
                "KEYSTROKE_TIME_MONOTONIC",
                "keystroke",
                "WARNING",
                "PASS" if not backwards else "FAIL",
                backwards_count=len(backwards),
                first_backwards_events=backwards[:20],
                equal_timestamps_allowed=True,
            )

        signature_fields = [
            "subject_code",
            "topic_code",
            "task_number",
            "condition",
            "familiarity",
            "planning_mode",
            "time_ms",
            "event",
            "key",
            "inputType",
            "data",
            "cursor_start",
            "cursor_end",
            "word_count",
        ]
        signatures = [
            tuple(row.get(field, "") for field in signature_fields)
            for row in keystroke_rows
        ]
        duplicate_signatures = [
            {"occurrences": count, "values": dict(zip(signature_fields, sig))}
            for sig, count in Counter(signatures).items()
            if count > 1
        ]
        add(
            "KEYSTROKE_DUPLICATE_EVENTS_ABSENT",
            "keystroke",
            "WARNING",
            "PASS" if not duplicate_signatures else "FAIL",
            duplicated_event_groups=len(duplicate_signatures),
            duplicated_extra_rows=sum(
                item["occurrences"] - 1 for item in duplicate_signatures
            ),
            first_duplicate_groups=duplicate_signatures[:10],
        )

        replay = replay_keystroke_text(keystroke_rows)
        missing_chains = replay.input_events_without_beforeinput
        add(
            "KEYSTROKE_INPUT_EVENTS_HAVE_BEFOREINPUT",
            "keystroke",
            "WARNING",
            "PASS" if not missing_chains else "FAIL",
            input_events_without_matching_beforeinput=len(missing_chains),
            first_events=missing_chains[:20],
            pairing_rule=(
                "immediately_preceding beforeinput has same inputType and data"
            ),
        )

        replay_interpretable = not (
            replay.unsupported_events or replay.invalid_cursor_events
        )
        add(
            "KEYSTROKE_REPLAY_EVENTS_INTERPRETABLE",
            "keystroke",
            "WARNING",
            "PASS" if replay_interpretable else "FAIL",
            applied_edit_events=replay.applied_events,
            unsupported_event_count=len(replay.unsupported_events),
            unsupported_events=replay.unsupported_events[:20],
            invalid_cursor_event_count=len(replay.invalid_cursor_events),
            invalid_cursor_events=replay.invalid_cursor_events[:20],
            browser_cursor_unit="UTF-16 code unit",
        )

        replay_complete = replay_interpretable and not missing_chains
        if replay_complete:
            reconstructed_matches = replay.text == essay_text
            add(
                "KEYSTROKE_RECONSTRUCTED_FINAL_TEXT_MATCH",
                "keystroke",
                "FLAG",
                "PASS" if reconstructed_matches else "FAIL",
                exact_match_required=True,
                applied_edit_events=replay.applied_events,
                reconstructed_character_count=len(replay.text),
                final_essay_character_count=len(essay_text),
                reconstructed_utf16_length=utf16_length(replay.text),
                final_essay_utf16_length=utf16_length(essay_text),
                reconstructed_sha256=text_sha256(replay.text),
                final_essay_sha256=text_sha256(essay_text),
                first_difference=first_text_difference(replay.text, essay_text),
            )
        else:
            add(
                "KEYSTROKE_RECONSTRUCTED_FINAL_TEXT_MATCH",
                "keystroke",
                "FLAG",
                "NOT_CHECKED",
                reason="replay_incomplete",
                input_events_without_beforeinput=len(missing_chains),
                unsupported_event_count=len(replay.unsupported_events),
                invalid_cursor_event_count=len(replay.invalid_cursor_events),
                reconstructed_character_count=len(replay.text),
                final_essay_character_count=len(essay_text),
            )

        if replay.large_insertions:
            for event in replay.large_insertions:
                add(
                    "KEYSTROKE_UNUSUALLY_LARGE_INSERTIONS",
                    "editing",
                    "FLAG",
                    "FAIL",
                    threshold_character_count=(
                        LARGE_INSERTION_THRESHOLD_CHARS
                    ),
                    **event,
                )
        else:
            add(
                "KEYSTROKE_UNUSUALLY_LARGE_INSERTIONS",
                "editing",
                "FLAG",
                "PASS",
                threshold_character_count=LARGE_INSERTION_THRESHOLD_CHARS,
                detected_count=0,
            )

        if replay.bulk_replacements:
            for event in replay.bulk_replacements:
                add(
                    "KEYSTROKE_BULK_REPLACEMENTS",
                    "editing",
                    "FLAG",
                    "FAIL",
                    threshold_removed_character_count=(
                        BULK_REPLACEMENT_THRESHOLD_CHARS
                    ),
                    **event,
                )
        else:
            add(
                "KEYSTROKE_BULK_REPLACEMENTS",
                "editing",
                "FLAG",
                "PASS",
                threshold_removed_character_count=(
                    BULK_REPLACEMENT_THRESHOLD_CHARS
                ),
                detected_count=0,
            )

        if replay.paste_or_drop_events:
            for event in replay.paste_or_drop_events:
                source_unknown = (
                    event["source_assessment"] == "source_unknown_or_external"
                )
                add(
                    "KEYSTROKE_PASTE_OR_DROP_INSERTION",
                    "editing",
                    "FLAG" if source_unknown else "INFO",
                    "FAIL",
                    **event,
                )
        else:
            add(
                "KEYSTROKE_PASTE_OR_DROP_INSERTION",
                "editing",
                "INFO",
                "PASS",
                detected_count=0,
            )

        if len(end_indices) == 1:
            events_after_end = len(keystroke_rows) - end_indices[0] - 1
            add(
                "KEYSTROKE_NO_EVENTS_AFTER_WRITING_END",
                "keystroke",
                "WARNING",
                "PASS" if events_after_end == 0 else "FAIL",
                writing_end_csv_row=end_indices[0] + 2,
                events_after_writing_end=events_after_end,
            )
        else:
            add(
                "KEYSTROKE_NO_EVENTS_AFTER_WRITING_END",
                "keystroke",
                "WARNING",
                "NOT_CHECKED",
                reason="writing_end_not_unique",
            )

        raw_metadata_time = metadata.get("actual_writing_time_ms")
        if keystroke.writing_end is None or raw_metadata_time in (None, ""):
            add(
                "KEYSTROKE_WRITING_END_TIME_MATCH_METADATA",
                "keystroke",
                "WARNING",
                "NOT_CHECKED",
                keystroke_time_ms=None
                if keystroke.writing_end is None
                else decimal_csv(keystroke.writing_end.elapsed_time_ms),
                metadata_time_ms=raw_metadata_time,
                reason="one_or_more_time_sources_unavailable",
            )
        else:
            try:
                metadata_time = Decimal(str(raw_metadata_time))
                if not metadata_time.is_finite():
                    raise InvalidOperation
                time_difference = abs(
                    metadata_time - keystroke.writing_end.elapsed_time_ms
                )
                add(
                    "KEYSTROKE_WRITING_END_TIME_MATCH_METADATA",
                    "keystroke",
                    "WARNING",
                    "PASS"
                    if time_difference <= WRITING_TIME_TOLERANCE_MS
                    else "FAIL",
                    keystroke_time_ms=decimal_csv(
                        keystroke.writing_end.elapsed_time_ms
                    ),
                    metadata_time_ms=decimal_csv(metadata_time),
                    absolute_difference_ms=decimal_csv(time_difference),
                    tolerance_ms=decimal_csv(WRITING_TIME_TOLERANCE_MS),
                    comparison="absolute_difference_ms <= tolerance_ms",
                )
            except (InvalidOperation, ValueError):
                add(
                    "KEYSTROKE_WRITING_END_TIME_MATCH_METADATA",
                    "keystroke",
                    "WARNING",
                    "NOT_CHECKED",
                    metadata_time_ms=raw_metadata_time,
                    reason="invalid_metadata_actual_writing_time_ms",
                )

        if keystroke.writing_end is None or not snapshots:
            add(
                "KEYSTROKE_WRITING_END_AFTER_LAST_SNAPSHOT",
                "keystroke",
                "WARNING",
                "NOT_CHECKED",
                writing_end_available=keystroke.writing_end is not None,
                snapshot_count=len(snapshots),
                reason="writing_end_or_snapshot_unavailable",
            )
        else:
            last_snapshot_ms = Decimal(snapshots[-1].minute * 60_000)
            endpoint_after_snapshot = (
                keystroke.writing_end.elapsed_time_ms >= last_snapshot_ms
            )
            add(
                "KEYSTROKE_WRITING_END_AFTER_LAST_SNAPSHOT",
                "keystroke",
                "WARNING",
                "PASS" if endpoint_after_snapshot else "FAIL",
                writing_end_time_ms=decimal_csv(
                    keystroke.writing_end.elapsed_time_ms
                ),
                last_snapshot_minute=snapshots[-1].minute,
                last_snapshot_time_ms=decimal_csv(last_snapshot_ms),
            )

    ai_condition = planning_mode == "a"
    chat_presence_matches = (ai_condition and chat_info is not None) or (
        not ai_condition and chat_info is None
    )
    add(
        "AI_CHAT_PRESENCE_MATCHES_CONDITION",
        "ai_chat",
        "WARNING",
        "PASS" if chat_presence_matches else "FAIL",
        planning_mode=planning_mode,
        chat_present=chat_info is not None,
        expected_chat_present=ai_condition,
    )
    if not ai_condition:
        add(
            "AI_CHAT_JSON_VALID",
            "ai_chat",
            "ERROR",
            "NOT_APPLICABLE",
            planning_mode=planning_mode,
            reason="ai_chat_content_not_applicable_to_non_ai_condition",
        )
        for rule in (
            "AI_CHAT_SUBJECT_CODE_MATCH",
            "AI_CHAT_TASK_NUMBER_MATCH",
            "AI_CHAT_TOPIC_CODE_MATCH",
            "AI_CHAT_CONDITION_LABEL_MATCH",
        ):
            add(
                rule,
                "identity",
                "FLAG",
                "NOT_APPLICABLE",
                planning_mode=planning_mode,
                reason="ai_chat_content_not_applicable_to_non_ai_condition",
            )
    elif chat_data is None:
        add(
            "AI_CHAT_JSON_VALID",
            "ai_chat",
            "ERROR",
            "NOT_CHECKED",
            filename=None,
            reason="required_ai_chat_log_unavailable",
        )
        for rule in (
            "AI_CHAT_SUBJECT_CODE_MATCH",
            "AI_CHAT_TASK_NUMBER_MATCH",
            "AI_CHAT_TOPIC_CODE_MATCH",
            "AI_CHAT_CONDITION_LABEL_MATCH",
        ):
            add(
                rule,
                "identity",
                "FLAG",
                "NOT_CHECKED",
                reason="required_ai_chat_log_unavailable",
            )
    else:
        add(
            "AI_CHAT_JSON_VALID",
            "ai_chat",
            "ERROR",
            "PASS",
            filename=chat_info.filename if chat_info is not None else None,
        )
        scalar_match(
            "AI_CHAT_SUBJECT_CODE_MATCH",
            "ai_chat",
            "subject_code",
            chat_data.get("subject_code"),
            participant,
        )
        scalar_match(
            "AI_CHAT_TASK_NUMBER_MATCH",
            "ai_chat",
            "task_number",
            chat_data.get("task_number"),
            task,
        )
        scalar_match(
            "AI_CHAT_TOPIC_CODE_MATCH",
            "ai_chat",
            "topic_code",
            chat_data.get("topic_code"),
            condition,
            normalise_lower=True,
        )
        scalar_match(
            "AI_CHAT_CONDITION_LABEL_MATCH",
            "ai_chat",
            "condition",
            chat_data.get("condition"),
            metadata.get("condition", ""),
        )

    return rows


def build_timeseries_rows(
    snapshots: list[Snapshot],
    writing_end: WritingEndObservation | None,
    participant: str,
    task: int,
    condition: str,
    planning_mode: str,
    familiarity: str,
    metadata: dict[str, Any],
    source_zip: str,
) -> list[dict[str, Any]]:
    """Create minute rows plus the final keystroke writing endpoint."""
    rows: list[dict[str, Any]] = []
    previous_word_count = 0

    for snapshot in snapshots:
        elapsed_ms = snapshot.minute * 60_000
        delta_word_count = snapshot.cumulative_word_count - previous_word_count

        rows.append(
            {
                "participant": participant,
                "task": task,
                "condition": condition,
                "planning_mode": planning_mode,
                "familiarity": familiarity,
                "topic": metadata_value(metadata, "topic"),
                "observation_type": "minute_snapshot",
                "minute": snapshot.minute,
                "elapsed_time_ms": elapsed_ms,
                "elapsed_time_seconds": snapshot.minute * 60,
                "cumulative_word_count": snapshot.cumulative_word_count,
                "delta_word_count": delta_word_count,
                "actual_writing_time_seconds": metadata_value(
                    metadata,
                    "actual_writing_time_seconds",
                ),
                "writing_end_reason": metadata_value(
                    metadata,
                    "writing_end_reason",
                ),
                "source_zip": source_zip,
                "_observation_sequence": snapshot.sequence,
                "_sort_elapsed_time_ms": Decimal(elapsed_ms),
                "_sort_observation_type": 0,
            }
        )

        previous_word_count = snapshot.cumulative_word_count

    if writing_end is not None:
        elapsed_seconds = writing_end.elapsed_time_ms / Decimal(1000)
        delta_word_count = (
            writing_end.cumulative_word_count - previous_word_count
        )
        rows.append(
            {
                "participant": participant,
                "task": task,
                "condition": condition,
                "planning_mode": planning_mode,
                "familiarity": familiarity,
                "topic": metadata_value(metadata, "topic"),
                "observation_type": "writing_end",
                "minute": "",
                "elapsed_time_ms": decimal_csv(writing_end.elapsed_time_ms),
                "elapsed_time_seconds": decimal_csv(elapsed_seconds),
                "cumulative_word_count": writing_end.cumulative_word_count,
                "delta_word_count": delta_word_count,
                "actual_writing_time_seconds": metadata_value(
                    metadata,
                    "actual_writing_time_seconds",
                ),
                "writing_end_reason": metadata_value(
                    metadata,
                    "writing_end_reason",
                ),
                "source_zip": source_zip,
                "_observation_sequence": len(snapshots) + 1,
                "_sort_elapsed_time_ms": writing_end.elapsed_time_ms,
                "_sort_observation_type": 1,
            }
        )

    return rows


def process_zip(path: Path) -> ParsedPackage:
    """Process one Gamma ZIP directly in read-only mode."""
    participant = ""
    task: int | str = ""
    condition = ""

    try:
        with zipfile.ZipFile(path, "r") as archive:
            corrupt_member = archive.testzip()

            if corrupt_member is not None:
                raise PackageError(
                    f"CRC failure in member: {corrupt_member}",
                    rule="ARCHIVE_CRC_VALID",
                    evidence={"corrupt_member": corrupt_member},
                )

            all_members = archive.infolist()
            members = [info for info in all_members if is_research_member(info)]

            metadata_matches = metadata_candidates(archive, members)

            if not metadata_matches:
                raise PackageError(
                    "Missing metadata JSON with required identity fields",
                    rule="METADATA_PRESENT_UNIQUE",
                    evidence={"candidate_count": 0},
                )

            if len(metadata_matches) > 1:
                names = ", ".join(info.filename for info, _ in metadata_matches)
                raise PackageError(
                    f"Multiple metadata candidates: {names}",
                    rule="METADATA_PRESENT_UNIQUE",
                    evidence={
                        "candidate_count": len(metadata_matches),
                        "candidates": [
                            info.filename for info, _ in metadata_matches
                        ],
                    },
                )

            metadata_info, metadata = metadata_matches[0]
            (
                participant,
                task,
                condition,
                planning_mode,
                familiarity,
            ) = validate_identity(metadata)

            final_info, essay_bytes = find_final_essay(archive, members)
            essay_text = essay_bytes.decode("utf-8")

            keystroke = find_keystroke_log(
                archive,
                members,
                participant,
                task,
                condition,
            )

            (
                snapshot_info,
                snapshots,
                snapshot_warnings,
            ) = find_snapshots(
                archive,
                members,
                final_info,
                participant,
                task,
                condition,
            )

            chat_info, chat_data = find_chat_log(
                archive,
                members,
                metadata_info,
                content_applicable=planning_mode == "a",
            )
            ai_chat_present = chat_info is not None

            qc_rows = build_qc_rows(
                archive,
                all_members,
                members,
                metadata_info,
                metadata,
                final_info,
                essay_text,
                keystroke,
                snapshot_info,
                snapshots,
                chat_info,
                chat_data,
                participant,
                int(task),
                condition,
                planning_mode,
                familiarity,
                path.name,
            )
            warnings_summary = qc_summary(qc_rows)

            raw_filename = f"{participant}_task{task}_{condition}.txt"
            fingerprint = research_fingerprint(
                archive,
                (
                    ("metadata", metadata_info),
                    ("final_essay", final_info),
                    ("keystroke_log", keystroke.info),
                    ("minute_snapshots", snapshot_info),
                    ("ai_chat", chat_info),
                ),
            )

            timeseries_rows = build_timeseries_rows(
                snapshots,
                keystroke.writing_end,
                participant,
                task,
                condition,
                planning_mode,
                familiarity,
                metadata,
                path.name,
            )

            manifest_row = {
                "participant": participant,
                "task": task,
                "condition": condition,
                "condition_label": metadata_value(metadata, "condition"),
                "planning_mode": planning_mode,
                "familiarity": familiarity,
                "topic": metadata_value(metadata, "topic"),
                "planned_planning_limit_seconds": metadata_value(
                    metadata,
                    "planned_planning_limit_seconds",
                ),
                "actual_planning_time_ms": metadata_value(
                    metadata,
                    "actual_planning_time_ms",
                ),
                "actual_planning_time_seconds": metadata_value(
                    metadata,
                    "actual_planning_time_seconds",
                ),
                "planning_end_reason": metadata_value(
                    metadata,
                    "planning_end_reason",
                ),
                "planned_writing_limit_seconds": metadata_value(
                    metadata,
                    "planned_writing_limit_seconds",
                ),
                "actual_writing_time_ms": metadata_value(
                    metadata,
                    "actual_writing_time_ms",
                ),
                "actual_writing_time_seconds": metadata_value(
                    metadata,
                    "actual_writing_time_seconds",
                ),
                "writing_end_reason": metadata_value(
                    metadata,
                    "writing_end_reason",
                ),
                "word_count": metadata_value(metadata, "final_word_count"),
                "snapshot_count": (
                    "" if snapshot_info is None else len(snapshots)
                ),
                "keystroke_rows": (
                    "" if keystroke.row_count is None else keystroke.row_count
                ),
                "ai_chat_present": ai_chat_present,
                "source_zip": path.name,
                "raw_text_file": f"texts_raw/{raw_filename}",
                "warnings": warnings_summary,
            }

            return ParsedPackage(
                source_zip=path,
                manifest_row=manifest_row,
                timeseries_rows=timeseries_rows,
                essay_bytes=essay_bytes,
                research_sha256=fingerprint,
                warnings=[warnings_summary] if warnings_summary else [],
                qc_rows=qc_rows,
            )

    except PackageError as exc:
        exc.participant = participant
        exc.task = task
        exc.condition = condition
        if not exc.qc_rows:
            exc.qc_rows = [
                make_qc_row(
                    path.name,
                    participant,
                    task,
                    condition,
                    exc.rule,
                    exc.category,
                    exc.severity,
                    "FAIL",
                    **exc.evidence,
                )
            ]
        raise
    except zipfile.BadZipFile as exc:
        error = PackageError(
            f"Invalid ZIP archive: {exc}",
            rule="ARCHIVE_READABLE",
            evidence={"error": str(exc)},
        )
        error.qc_rows = [
            make_qc_row(
                path.name,
                "",
                "",
                "",
                error.rule,
                error.category,
                error.severity,
                "FAIL",
                **error.evidence,
            )
        ]
        raise error from exc
    except OSError as exc:
        error = PackageError(
            f"Cannot read ZIP archive: {exc}",
            rule="ARCHIVE_READABLE",
            evidence={"error": str(exc)},
        )
        error.qc_rows = [
            make_qc_row(
                path.name,
                "",
                "",
                "",
                error.rule,
                error.category,
                error.severity,
                "FAIL",
                **error.evidence,
            )
        ]
        raise error from exc


def atomic_write_bytes(path: Path, content: bytes) -> None:
    """Write generated binary output atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(dir=path.parent, delete=False)
    temporary_path = Path(handle.name)

    try:
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def atomic_write_csv(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, Any]],
) -> None:
    """Write a UTF-8 CSV atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=path.parent,
        delete=False,
    )
    temporary_path = Path(handle.name)

    try:
        with handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary_path, path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise


def process_batch(
    input_dir: Path | str = DEFAULT_INPUT_DIR,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
) -> BatchSummary:
    """Process every ZIP directly inside input_dir."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        raise ValueError(
            "Input folder does not exist or is not a directory: "
            f"{input_path}"
        )

    if input_path.resolve() == output_path.resolve():
        raise ValueError("Input and output folders must be different")

    zip_paths = sorted(
        (
            path
            for path in input_path.iterdir()
            if path.is_file() and path.suffix.lower() == ".zip"
        ),
        key=lambda path: path.name.casefold(),
    )

    output_path.mkdir(parents=True, exist_ok=True)
    (output_path / "texts_raw").mkdir(parents=True, exist_ok=True)

    manifest_rows: list[dict[str, Any]] = []
    report_rows: list[dict[str, Any]] = []
    timeseries_rows: list[dict[str, Any]] = []
    qc_rows: list[dict[str, Any]] = []
    seen: dict[tuple[str, int], ParsedPackage] = {}

    processed = 0
    duplicates = 0
    failed = 0

    for zip_path in zip_paths:
        try:
            package = process_zip(zip_path)
            row = package.manifest_row
            identity = (
                str(row["participant"]),
                int(row["task"]),
            )
            previous = seen.get(identity)

            if previous is not None:
                previous_condition = str(
                    previous.manifest_row["condition"]
                )
                current_condition = str(row["condition"])
                same_condition = previous_condition == current_condition
                if (
                    same_condition
                    and previous.research_sha256 == package.research_sha256
                ):
                    duplicates += 1
                    qc_rows.append(
                        make_qc_row(
                            zip_path.name,
                            row["participant"],
                            row["task"],
                            row["condition"],
                            "DUPLICATE_PACKAGE_IDENTICAL",
                            "batch",
                            "INFO",
                            "FAIL",
                            original_source_zip=previous.source_zip.name,
                            duplicate_source_zip=zip_path.name,
                            action="no_second_manifest_or_timeseries_rows",
                        )
                    )
                    report_rows.append(
                        {
                            "source_zip": zip_path.name,
                            "status": "duplicate_identical",
                            "participant": row["participant"],
                            "task": row["task"],
                            "condition": row["condition"],
                            "raw_text_file": previous.manifest_row[
                                "raw_text_file"
                            ],
                            "warnings": "",
                            "error": "",
                        }
                    )
                    continue

                failed += 1
                if same_condition:
                    conflict_rule = "DUPLICATE_PACKAGE_CONFLICT"
                    conflict_message = (
                        "Conflicting packages share participant-task-condition "
                        "but have different research contents: "
                        f"{previous.source_zip.name} and {zip_path.name}"
                    )
                else:
                    conflict_rule = "PARTICIPANT_TASK_CONDITION_CONFLICT"
                    conflict_message = (
                        "A participant-task pair appears under more than one "
                        "condition: "
                        f"{previous.source_zip.name} ({previous_condition}) "
                        f"and {zip_path.name} ({current_condition})"
                    )
                qc_rows.append(
                    make_qc_row(
                        zip_path.name,
                        row["participant"],
                        row["task"],
                        row["condition"],
                        conflict_rule,
                        "batch",
                        "ERROR",
                        "FAIL",
                        original_source_zip=previous.source_zip.name,
                        conflicting_source_zip=zip_path.name,
                        original_condition=previous_condition,
                        conflicting_condition=current_condition,
                        uniqueness_key=[row["participant"], row["task"]],
                    )
                )
                report_rows.append(
                    {
                        "source_zip": zip_path.name,
                        "status": "error",
                        "participant": row["participant"],
                        "task": row["task"],
                        "condition": row["condition"],
                        "raw_text_file": "",
                        "warnings": "qc_findings=1; qc_max_severity=ERROR",
                        "error": conflict_message,
                    }
                )
                continue

            raw_path = output_path / str(row["raw_text_file"])
            atomic_write_bytes(raw_path, package.essay_bytes)

            seen[identity] = package
            manifest_rows.append(row)
            timeseries_rows.extend(package.timeseries_rows)
            qc_rows.extend(package.qc_rows)
            processed += 1

            report_rows.append(
                {
                    "source_zip": zip_path.name,
                    "status": "processed",
                    "participant": row["participant"],
                    "task": row["task"],
                    "condition": row["condition"],
                    "raw_text_file": row["raw_text_file"],
                    "warnings": row["warnings"],
                    "error": "",
                }
            )

        except PackageError as exc:
            failed += 1
            qc_rows.extend(exc.qc_rows)
            report_rows.append(
                {
                    "source_zip": zip_path.name,
                    "status": "error",
                    "participant": exc.participant,
                    "task": exc.task,
                    "condition": exc.condition,
                    "raw_text_file": "",
                    "warnings": "qc_findings=1; qc_max_severity=ERROR",
                    "error": str(exc),
                }
            )

    manifest_rows.sort(
        key=lambda row: (
            str(row["participant"]),
            int(row["task"]),
            str(row["condition"]),
        )
    )
    timeseries_rows.sort(
        key=lambda row: (
            str(row["participant"]),
            int(row["task"]),
            row["_sort_elapsed_time_ms"],
            int(row["_sort_observation_type"]),
            int(row["_observation_sequence"]),
        )
    )
    qc_rows.sort(
        key=lambda row: (
            str(row["participant"]),
            str(row["task"]),
            str(row["condition"]),
            str(row["source_zip"]).casefold(),
            str(row["category"]),
            str(row["rule"]),
            str(row["result"]),
            str(row["evidence"]),
        )
    )

    manifest_path = output_path / "manifest.csv"
    report_path = output_path / "processing_report.csv"
    timeseries_path = output_path / "writing_timeseries.csv"
    qc_path = output_path / "qc_report.csv"

    atomic_write_csv(manifest_path, MANIFEST_FIELDS, manifest_rows)
    atomic_write_csv(report_path, REPORT_FIELDS, report_rows)
    atomic_write_csv(timeseries_path, TIMESERIES_FIELDS, timeseries_rows)
    atomic_write_csv(qc_path, QC_FIELDS, qc_rows)

    return BatchSummary(
        discovered=len(zip_paths),
        processed=processed,
        duplicates=duplicates,
        failed=failed,
        timeseries_rows=len(timeseries_rows),
        manifest_path=manifest_path,
        report_path=report_path,
        timeseries_path=timeseries_path,
        qc_path=qc_path,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mtdps",
        description=(
            "Read Gamma ZIP data packages and export manifest.csv, raw "
            "essays, writing_timeseries.csv, and qc_report.csv."
        ),
    )

    parser.add_argument(
        "input_dir",
        nargs="?",
        default=DEFAULT_INPUT_DIR,
        type=Path,
        help=(
            "Input folder containing Gamma ZIP files "
            f"(default: {DEFAULT_INPUT_DIR})"
        ),
    )
    parser.add_argument(
        "output_dir",
        nargs="?",
        default=DEFAULT_OUTPUT_DIR,
        type=Path,
        help=f"Generated output folder (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"MTDPS v{VERSION}",
    )

    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()

    try:
        summary = process_batch(args.input_dir, args.output_dir)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    print(
        f"MTDPS v{VERSION}: "
        f"discovered={summary.discovered}, "
        f"processed={summary.processed}, "
        f"duplicates={summary.duplicates}, "
        f"failed={summary.failed}, "
        f"timeseries_rows={summary.timeseries_rows}"
    )
    print(f"Input folder: {args.input_dir}")
    print(f"Manifest: {summary.manifest_path}")
    print(f"Processing report: {summary.report_path}")
    print(f"Writing time series: {summary.timeseries_path}")
    print(f"QC report: {summary.qc_path}")

    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
