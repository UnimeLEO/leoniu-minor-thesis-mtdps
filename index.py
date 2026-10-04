#!/usr/bin/env python3
"""MTDPS v0.2.1 — Minor Thesis Data Processing System."""

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
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


VERSION = "0.2.1"

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
class BatchSummary:
    discovered: int
    processed: int
    duplicates: int
    failed: int
    timeseries_rows: int
    manifest_path: Path
    report_path: Path
    timeseries_path: Path


@dataclass
class ParsedPackage:
    source_zip: Path
    manifest_row: dict[str, Any]
    timeseries_rows: list[dict[str, Any]]
    essay_bytes: bytes
    research_sha256: str
    warnings: list[str]


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
                    f"Metadata candidate is invalid JSON: {info.filename}"
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

    info = choose_single(candidates, "final essay")
    raw = archive.read(info)

    try:
        raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PackageError(
            f"Final essay is not valid UTF-8: {info.filename}"
        ) from exc

    return info, raw


def decimal_from_csv(value: str, field: str) -> Decimal:
    """Parse a finite decimal stored in a CSV field."""
    try:
        parsed = Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise PackageError(f"Invalid {field} in keystroke log: {value!r}") from exc

    if not parsed.is_finite():
        raise PackageError(f"Non-finite {field} in keystroke log: {value!r}")

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
            f"Negative writing_end time_ms in keystroke log: {elapsed_time_ms}"
        )

    try:
        word_count = int(row.get("word_count", ""))
    except (TypeError, ValueError) as exc:
        raise PackageError(
            "Invalid writing_end word_count in keystroke log: "
            f"{row.get('word_count')!r}"
        ) from exc

    if word_count < 0:
        raise PackageError(
            f"Negative writing_end word_count in keystroke log: {word_count}"
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

    for info in members:
        if not info.filename.lower().endswith(".csv"):
            continue

        try:
            text = archive.read(info).decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text, newline=""))
            columns = set(reader.fieldnames or [])

            if KEYSTROKE_REQUIRED_COLUMNS.issubset(columns):
                candidates.append((info, list(reader)))
        except (UnicodeDecodeError, csv.Error) as exc:
            if "keystroke" in info.filename.lower():
                raise PackageError(
                    f"Invalid keystroke CSV in {info.filename}: {exc}"
                ) from exc

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
        raise PackageError(f"Multiple keystroke log candidates: {names}")

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
                    f"Invalid snapshot UTF-8 in {info.filename}: {exc}"
                ) from exc

            continue

        normalised = normalise_newlines(text)

        if SNAPSHOT_HEADER_RE.search(normalised):
            valid_candidates.append((info, text))
        elif "snapshot" in basename:
            named_without_headers.append((info, text))

    if len(valid_candidates) > 1:
        names = ", ".join(info.filename for info, _ in valid_candidates)
        raise PackageError(f"Multiple minute snapshot candidates: {names}")

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
            f"Multiple unparseable minute snapshot candidates: {names}"
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
) -> tuple[zipfile.ZipInfo | None, dict[str, Any] | None]:
    """Identify an AI chat JSON by the presence of a chat array."""
    candidates: list[tuple[zipfile.ZipInfo, dict[str, Any]]] = []

    for info in members:
        if info.filename == metadata_info.filename:
            continue

        if not info.filename.lower().endswith(".json"):
            continue

        try:
            value = json.loads(archive.read(info).decode("utf-8-sig"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            if "chat" in info.filename.lower():
                raise PackageError(f"Invalid chat JSON: {info.filename}")

            continue

        if isinstance(value, dict) and isinstance(value.get("chat"), list):
            candidates.append((info, value))

    if len(candidates) > 1:
        names = ", ".join(info.filename for info, _ in candidates)
        raise PackageError(f"Multiple AI chat log candidates: {names}")

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
        raise PackageError("task_number is not an integer") from exc

    condition = str(metadata["topic_code"]).lower()
    planning_mode = str(metadata["planning_mode"]).lower()
    familiarity = str(metadata["familiarity"]).lower()

    if not SAFE_COMPONENT_RE.fullmatch(participant):
        raise PackageError(
            f"Unsafe subject_code for output filename: {participant!r}"
        )

    if task < 1:
        raise PackageError(f"Invalid task_number: {task}")

    if not SAFE_COMPONENT_RE.fullmatch(condition):
        raise PackageError(
            f"Unsafe topic_code for output filename: {condition!r}"
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
    warnings: list[str] = []

    try:
        with zipfile.ZipFile(path, "r") as archive:
            corrupt_member = archive.testzip()

            if corrupt_member is not None:
                raise PackageError(f"CRC failure in member: {corrupt_member}")

            all_members = archive.infolist()
            members = [info for info in all_members if is_research_member(info)]
            ignored_count = sum(
                1 for info in all_members if not is_research_member(info)
            )

            if ignored_count:
                warnings.append(
                    f"ignored_system_or_directory_entries={ignored_count}"
                )

            unsafe_paths = [
                info.filename
                for info in members
                if unsafe_member_path(info.filename)
            ]

            if unsafe_paths:
                warnings.append("unsafe_member_paths_present_not_extracted")

            metadata_matches = metadata_candidates(archive, members)

            if not metadata_matches:
                raise PackageError(
                    "Missing metadata JSON with required identity fields"
                )

            if len(metadata_matches) > 1:
                names = ", ".join(info.filename for info, _ in metadata_matches)
                raise PackageError(f"Multiple metadata candidates: {names}")

            metadata_info, metadata = metadata_matches[0]
            (
                participant,
                task,
                condition,
                planning_mode,
                familiarity,
            ) = validate_identity(metadata)

            expected_condition = planning_mode + familiarity

            if condition != expected_condition:
                warnings.append(
                    "condition_code_mismatch"
                    f"(topic_code={condition},derived={expected_condition})"
                )

            if planning_mode not in {"a", "i", "n"}:
                warnings.append(f"unexpected_planning_mode={planning_mode}")

            if familiarity not in {"h", "l"}:
                warnings.append(f"unexpected_familiarity={familiarity}")

            final_info, essay_bytes = find_final_essay(archive, members)
            essay_text = essay_bytes.decode("utf-8")

            keystroke = find_keystroke_log(
                archive,
                members,
                participant,
                task,
                condition,
            )
            warnings.extend(keystroke.warnings)

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
            warnings.extend(snapshot_warnings)

            chat_info, chat_data = find_chat_log(
                archive,
                members,
                metadata_info,
            )
            ai_chat_present = chat_info is not None

            if planning_mode == "a" and not ai_chat_present:
                warnings.append("ai_condition_missing_chat_log")

            if planning_mode != "a" and ai_chat_present:
                warnings.append("non_ai_condition_has_chat_log")

            metadata_word_count = metadata.get("final_word_count")
            computed_word_count = count_words(essay_text)

            if metadata_word_count is None:
                warnings.append("missing_final_word_count")
            else:
                try:
                    if int(metadata_word_count) != computed_word_count:
                        warnings.append(
                            "word_count_mismatch"
                            f"(metadata={metadata_word_count},"
                            f"computed={computed_word_count})"
                        )
                except (TypeError, ValueError):
                    warnings.append(
                        f"invalid_final_word_count={metadata_word_count!r}"
                    )

            if keystroke.writing_end is not None:
                if (
                    keystroke.writing_end.cumulative_word_count
                    != computed_word_count
                ):
                    warnings.append(
                        "keystroke_final_word_count_mismatch"
                        f"(keystroke="
                        f"{keystroke.writing_end.cumulative_word_count},"
                        f"computed={computed_word_count})"
                    )

                metadata_time = metadata.get("actual_writing_time_ms")
                if metadata_time not in (None, ""):
                    try:
                        metadata_time_decimal = Decimal(str(metadata_time))
                        if not metadata_time_decimal.is_finite():
                            raise InvalidOperation
                        difference = abs(
                            metadata_time_decimal
                            - keystroke.writing_end.elapsed_time_ms
                        )
                        if difference > Decimal("1"):
                            warnings.append(
                                "keystroke_writing_end_time_mismatch"
                                f"(keystroke_ms="
                                f"{decimal_csv(keystroke.writing_end.elapsed_time_ms)},"
                                f"metadata_ms={metadata_time},"
                                f"difference_ms={decimal_csv(difference)})"
                            )
                    except (InvalidOperation, ValueError):
                        warnings.append(
                            f"invalid_actual_writing_time_ms={metadata_time!r}"
                        )

                if snapshots:
                    last_snapshot_time = Decimal(snapshots[-1].minute * 60_000)
                    if keystroke.writing_end.elapsed_time_ms < last_snapshot_time:
                        warnings.append(
                            "keystroke_writing_end_precedes_last_snapshot"
                            f"(writing_end_ms="
                            f"{decimal_csv(keystroke.writing_end.elapsed_time_ms)},"
                            f"last_snapshot_ms={decimal_csv(last_snapshot_time)})"
                        )

            if snapshots and (
                normalise_text_for_comparison(snapshots[-1].text)
                != normalise_text_for_comparison(essay_text)
            ):
                warnings.append("last_snapshot_differs_from_final_essay")

            if chat_data is not None:
                for key, expected in (
                    ("subject_code", participant),
                    ("task_number", task),
                    ("topic_code", condition),
                ):
                    if key in chat_data and str(chat_data[key]) != str(expected):
                        warnings.append(f"chat_{key}_mismatch")

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
                "warnings": " | ".join(warnings),
            }

            return ParsedPackage(
                source_zip=path,
                manifest_row=manifest_row,
                timeseries_rows=timeseries_rows,
                essay_bytes=essay_bytes,
                research_sha256=fingerprint,
                warnings=warnings,
            )

    except zipfile.BadZipFile as exc:
        raise PackageError(f"Invalid ZIP archive: {exc}") from exc
    except OSError as exc:
        raise PackageError(f"Cannot read ZIP archive: {exc}") from exc


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
    seen: dict[tuple[str, int, str], ParsedPackage] = {}

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
                str(row["condition"]),
            )
            previous = seen.get(identity)

            if previous is not None:
                if previous.research_sha256 == package.research_sha256:
                    duplicates += 1
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
                            "warnings": (
                                "identical participant-task-condition "
                                "already processed; no second manifest "
                                "or time-series rows"
                            ),
                            "error": "",
                        }
                    )
                    continue

                raise PackageError(
                    "Conflicting packages share participant-task-condition "
                    "but have different research contents: "
                    f"{previous.source_zip.name} and {zip_path.name}"
                )

            raw_path = output_path / str(row["raw_text_file"])
            atomic_write_bytes(raw_path, package.essay_bytes)

            seen[identity] = package
            manifest_rows.append(row)
            timeseries_rows.extend(package.timeseries_rows)
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
            report_rows.append(
                {
                    "source_zip": zip_path.name,
                    "status": "error",
                    "participant": "",
                    "task": "",
                    "condition": "",
                    "raw_text_file": "",
                    "warnings": "",
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

    manifest_path = output_path / "manifest.csv"
    report_path = output_path / "processing_report.csv"
    timeseries_path = output_path / "writing_timeseries.csv"

    atomic_write_csv(manifest_path, MANIFEST_FIELDS, manifest_rows)
    atomic_write_csv(report_path, REPORT_FIELDS, report_rows)
    atomic_write_csv(timeseries_path, TIMESERIES_FIELDS, timeseries_rows)

    return BatchSummary(
        discovered=len(zip_paths),
        processed=processed,
        duplicates=duplicates,
        failed=failed,
        timeseries_rows=len(timeseries_rows),
        manifest_path=manifest_path,
        report_path=report_path,
        timeseries_path=timeseries_path,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mtdps",
        description=(
            "Read Gamma ZIP data packages and export manifest.csv, raw "
            "essays, and writing_timeseries.csv."
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

    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
