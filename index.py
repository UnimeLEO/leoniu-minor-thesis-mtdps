# MTDPS v0.1.1
# Minor Thesis Data Processing System
# Python 3.10+ / Standard library only

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
from pathlib import Path, PurePosixPath
from typing import Any, Iterable


VERSION = "0.1.1"

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
}

SYSTEM_BASENAMES = {
    ".DS_Store",
    "Thumbs.db",
    "desktop.ini",
}

SAFE_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_-]+$")
SNAPSHOT_RE = re.compile(
    r"^=== Minute (\d+) ===$",
    re.MULTILINE,
)


class PackageError(Exception):
    """A Gamma ZIP package cannot be processed reliably."""


@dataclass(frozen=True)
class BatchSummary:
    discovered: int
    processed: int
    duplicates: int
    failed: int
    manifest_path: Path
    report_path: Path


@dataclass
class ParsedPackage:
    source_zip: Path
    manifest_row: dict[str, Any]
    essay_bytes: bytes
    research_sha256: str
    warnings: list[str]


def normalise_member_name(name: str) -> str:
    """Normalise ZIP member separators without extracting the member."""
    return name.replace("\\", "/")


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
    """
    Detect absolute paths and parent traversal.

    MTDPS never extracts ZIP members, so unsafe paths are reported rather
    than followed.
    """
    normalised = normalise_member_name(name)
    path = PurePosixPath(normalised)

    return path.is_absolute() or ".." in path.parts


def metadata_candidates(
    archive: zipfile.ZipFile,
    members: Iterable[zipfile.ZipInfo],
) -> list[tuple[zipfile.ZipInfo, dict[str, Any]]]:
    """
    Find metadata by JSON content rather than the outer ZIP filename.

    A metadata object must contain all core Gamma identity fields.
    """
    candidates: list[
        tuple[zipfile.ZipInfo, dict[str, Any]]
    ] = []

    for info in members:
        if not info.filename.lower().endswith(".json"):
            continue

        try:
            raw = archive.read(info)
            value = json.loads(raw.decode("utf-8-sig"))

        except (UnicodeDecodeError, json.JSONDecodeError):
            basename = PurePosixPath(
                normalise_member_name(info.filename)
            ).name.lower()

            if "metadata" in basename:
                raise PackageError(
                    "Metadata candidate is invalid JSON: "
                    f"{info.filename}"
                )

            continue

        if (
            isinstance(value, dict)
            and METADATA_IDENTITY_KEYS.issubset(value)
        ):
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
        names = ", ".join(
            info.filename
            for info in candidates
        )
        raise PackageError(
            f"Multiple {role} candidates: {names}"
        )

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

    info = choose_single(
        candidates,
        "final essay",
    )
    raw = archive.read(info)

    try:
        raw.decode("utf-8")

    except UnicodeDecodeError as exc:
        raise PackageError(
            "Final essay is not valid UTF-8: "
            f"{info.filename}"
        ) from exc

    return info, raw


def find_keystroke_log(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
) -> tuple[
    zipfile.ZipInfo | None,
    int | None,
    list[str],
]:
    """Identify the keystroke CSV by its columns and count data rows."""
    warnings: list[str] = []
    candidates: list[
        tuple[zipfile.ZipInfo, int]
    ] = []

    for info in members:
        if not info.filename.lower().endswith(".csv"):
            continue

        try:
            raw = archive.read(info)
            text = raw.decode("utf-8-sig")

            reader = csv.DictReader(
                io.StringIO(
                    text,
                    newline="",
                )
            )

            columns = set(
                reader.fieldnames or []
            )

            if KEYSTROKE_REQUIRED_COLUMNS.issubset(
                columns
            ):
                row_count = sum(
                    1
                    for _ in reader
                )
                candidates.append(
                    (info, row_count)
                )

        except (UnicodeDecodeError, csv.Error) as exc:
            if "keystroke" in info.filename.lower():
                raise PackageError(
                    "Invalid keystroke CSV in "
                    f"{info.filename}: {exc}"
                ) from exc

    if not candidates:
        warnings.append(
            "missing_keystroke_log"
        )
        return None, None, warnings

    if len(candidates) > 1:
        names = ", ".join(
            info.filename
            for info, _ in candidates
        )
        raise PackageError(
            "Multiple keystroke log candidates: "
            f"{names}"
        )

    info, row_count = candidates[0]

    return info, row_count, warnings


def find_snapshots(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    final_info: zipfile.ZipInfo,
) -> tuple[
    zipfile.ZipInfo | None,
    int | None,
    list[str],
]:
    """Find minute snapshots and count their Minute N sections."""
    warnings: list[str] = []
    candidates: list[
        tuple[zipfile.ZipInfo, list[int]]
    ] = []

    for info in members:
        if info.filename == final_info.filename:
            continue

        if not info.filename.lower().endswith(".txt"):
            continue

        try:
            raw = archive.read(info)
            text = raw.decode("utf-8-sig")

        except UnicodeDecodeError as exc:
            if "snapshot" in info.filename.lower():
                raise PackageError(
                    "Invalid snapshot UTF-8 in "
                    f"{info.filename}: {exc}"
                ) from exc

            continue

        minutes = [
            int(value)
            for value in SNAPSHOT_RE.findall(text)
        ]

        if minutes:
            candidates.append(
                (info, minutes)
            )

    if not candidates:
        warnings.append(
            "missing_minute_snapshots"
        )
        return None, None, warnings

    if len(candidates) > 1:
        names = ", ".join(
            info.filename
            for info, _ in candidates
        )
        raise PackageError(
            "Multiple minute snapshot candidates: "
            f"{names}"
        )

    info, minutes = candidates[0]

    expected_minutes = list(
        range(
            1,
            len(minutes) + 1,
        )
    )

    if minutes != expected_minutes:
        warnings.append(
            "nonsequential_snapshot_minutes"
        )

    return info, len(minutes), warnings


def find_chat_log(
    archive: zipfile.ZipFile,
    members: list[zipfile.ZipInfo],
    metadata_info: zipfile.ZipInfo,
) -> tuple[
    zipfile.ZipInfo | None,
    dict[str, Any] | None,
]:
    """Identify an AI chat JSON by the presence of a chat array."""
    candidates: list[
        tuple[
            zipfile.ZipInfo,
            dict[str, Any],
        ]
    ] = []

    for info in members:
        if info.filename == metadata_info.filename:
            continue

        if not info.filename.lower().endswith(".json"):
            continue

        try:
            raw = archive.read(info)
            value = json.loads(
                raw.decode("utf-8-sig")
            )

        except (
            UnicodeDecodeError,
            json.JSONDecodeError,
        ):
            if "chat" in info.filename.lower():
                raise PackageError(
                    "Invalid chat JSON: "
                    f"{info.filename}"
                )

            continue

        if (
            isinstance(value, dict)
            and isinstance(
                value.get("chat"),
                list,
            )
        ):
            candidates.append(
                (info, value)
            )

    if len(candidates) > 1:
        names = ", ".join(
            info.filename
            for info, _ in candidates
        )
        raise PackageError(
            "Multiple AI chat log candidates: "
            f"{names}"
        )

    if candidates:
        return candidates[0]

    return None, None


def count_words(text: str) -> int:
    """Reproduce the observed Gamma whitespace-based word count."""
    return len(
        re.findall(
            r"\S+",
            text,
        )
    )


def validate_identity(
    metadata: dict[str, Any],
) -> tuple[
    str,
    int,
    str,
    str,
    str,
]:
    """Read and validate the identity needed for output filenames."""
    participant = str(
        metadata["subject_code"]
    )

    try:
        task = int(
            metadata["task_number"]
        )

    except (TypeError, ValueError) as exc:
        raise PackageError(
            "task_number is not an integer"
        ) from exc

    condition = str(
        metadata["topic_code"]
    ).lower()

    planning_mode = str(
        metadata["planning_mode"]
    ).lower()

    familiarity = str(
        metadata["familiarity"]
    ).lower()

    if not SAFE_COMPONENT_RE.fullmatch(
        participant
    ):
        raise PackageError(
            "Unsafe subject_code for output filename: "
            f"{participant!r}"
        )

    if task < 1:
        raise PackageError(
            f"Invalid task_number: {task}"
        )

    if not SAFE_COMPONENT_RE.fullmatch(
        condition
    ):
        raise PackageError(
            "Unsafe topic_code for output filename: "
            f"{condition!r}"
        )

    return (
        participant,
        task,
        condition,
        planning_mode,
        familiarity,
    )


def metadata_value(
    metadata: dict[str, Any],
    key: str,
) -> Any:
    """Return a CSV-safe metadata value."""
    value = metadata.get(
        key,
        "",
    )

    if value is None:
        return ""

    return value


def research_fingerprint(
    archive: zipfile.ZipFile,
    role_members: Iterable[
        tuple[
            str,
            zipfile.ZipInfo | None,
        ]
    ],
) -> str:
    """
    Hash all identified research files by role.

    Filenames and ZIP timestamps are excluded, so duplicate downloads
    with different outer names can still be recognised as identical.
    """
    digest = hashlib.sha256()

    for role, info in role_members:
        role_bytes = role.encode("ascii")

        content = (
            b""
            if info is None
            else archive.read(info)
        )

        digest.update(
            len(role_bytes).to_bytes(
                4,
                "big",
            )
        )
        digest.update(role_bytes)

        digest.update(
            len(content).to_bytes(
                8,
                "big",
            )
        )
        digest.update(content)

    return digest.hexdigest()


def process_zip(path: Path) -> ParsedPackage:
    """
    Process one Gamma ZIP directly in read-only mode.

    Nothing is extracted into or written back to the source location.
    """
    warnings: list[str] = []

    try:
        with zipfile.ZipFile(
            path,
            "r",
        ) as archive:
            corrupt_member = archive.testzip()

            if corrupt_member is not None:
                raise PackageError(
                    "CRC failure in member: "
                    f"{corrupt_member}"
                )

            all_members = archive.infolist()

            members = [
                info
                for info in all_members
                if is_research_member(info)
            ]

            ignored_count = sum(
                1
                for info in all_members
                if not is_research_member(info)
            )

            if ignored_count:
                warnings.append(
                    "ignored_system_or_directory_entries="
                    f"{ignored_count}"
                )

            unsafe_paths = [
                info.filename
                for info in members
                if unsafe_member_path(
                    info.filename
                )
            ]

            if unsafe_paths:
                warnings.append(
                    "unsafe_member_paths_present_"
                    "not_extracted"
                )

            metadata_matches = metadata_candidates(
                archive,
                members,
            )

            if not metadata_matches:
                raise PackageError(
                    "Missing metadata JSON with "
                    "required identity fields"
                )

            if len(metadata_matches) > 1:
                names = ", ".join(
                    info.filename
                    for info, _ in metadata_matches
                )

                raise PackageError(
                    "Multiple metadata candidates: "
                    f"{names}"
                )

            (
                metadata_info,
                metadata,
            ) = metadata_matches[0]

            (
                participant,
                task,
                condition,
                planning_mode,
                familiarity,
            ) = validate_identity(metadata)

            expected_condition = (
                planning_mode + familiarity
            )

            if condition != expected_condition:
                warnings.append(
                    "condition_code_mismatch"
                    f"(topic_code={condition},"
                    f"derived={expected_condition})"
                )

            if planning_mode not in {
                "a",
                "i",
                "n",
            }:
                warnings.append(
                    "unexpected_planning_mode="
                    f"{planning_mode}"
                )

            if familiarity not in {
                "h",
                "l",
            }:
                warnings.append(
                    "unexpected_familiarity="
                    f"{familiarity}"
                )

            (
                final_info,
                essay_bytes,
            ) = find_final_essay(
                archive,
                members,
            )

            essay_text = essay_bytes.decode(
                "utf-8"
            )

            (
                keystroke_info,
                keystroke_rows,
                keystroke_warnings,
            ) = find_keystroke_log(
                archive,
                members,
            )

            warnings.extend(
                keystroke_warnings
            )

            (
                snapshot_info,
                snapshot_count,
                snapshot_warnings,
            ) = find_snapshots(
                archive,
                members,
                final_info,
            )

            warnings.extend(
                snapshot_warnings
            )

            (
                chat_info,
                chat_data,
            ) = find_chat_log(
                archive,
                members,
                metadata_info,
            )

            ai_chat_present = (
                chat_info is not None
            )

            if (
                planning_mode == "a"
                and not ai_chat_present
            ):
                warnings.append(
                    "ai_condition_missing_chat_log"
                )

            if (
                planning_mode != "a"
                and ai_chat_present
            ):
                warnings.append(
                    "non_ai_condition_has_chat_log"
                )

            metadata_word_count = metadata.get(
                "final_word_count"
            )

            computed_word_count = count_words(
                essay_text
            )

            if metadata_word_count is None:
                warnings.append(
                    "missing_final_word_count"
                )

            else:
                try:
                    if (
                        int(metadata_word_count)
                        != computed_word_count
                    ):
                        warnings.append(
                            "word_count_mismatch"
                            f"(metadata="
                            f"{metadata_word_count},"
                            f"computed="
                            f"{computed_word_count})"
                        )

                except (TypeError, ValueError):
                    warnings.append(
                        "invalid_final_word_count="
                        f"{metadata_word_count!r}"
                    )

            if chat_data is not None:
                chat_identity_fields = (
                    (
                        "subject_code",
                        participant,
                    ),
                    (
                        "task_number",
                        task,
                    ),
                    (
                        "topic_code",
                        condition,
                    ),
                )

                for (
                    key,
                    expected,
                ) in chat_identity_fields:
                    if (
                        key in chat_data
                        and str(chat_data[key])
                        != str(expected)
                    ):
                        warnings.append(
                            f"chat_{key}_mismatch"
                        )

            raw_filename = (
                f"{participant}_"
                f"task{task}_"
                f"{condition}.txt"
            )

            fingerprint = research_fingerprint(
                archive,
                (
                    (
                        "metadata",
                        metadata_info,
                    ),
                    (
                        "final_essay",
                        final_info,
                    ),
                    (
                        "keystroke_log",
                        keystroke_info,
                    ),
                    (
                        "minute_snapshots",
                        snapshot_info,
                    ),
                    (
                        "ai_chat",
                        chat_info,
                    ),
                ),
            )

            manifest_row = {
                "participant":
                    participant,

                "task":
                    task,

                "condition":
                    condition,

                "condition_label":
                    metadata_value(
                        metadata,
                        "condition",
                    ),

                "planning_mode":
                    planning_mode,

                "familiarity":
                    familiarity,

                "topic":
                    metadata_value(
                        metadata,
                        "topic",
                    ),

                "planned_planning_limit_seconds":
                    metadata_value(
                        metadata,
                        "planned_planning_limit_seconds",
                    ),

                "actual_planning_time_ms":
                    metadata_value(
                        metadata,
                        "actual_planning_time_ms",
                    ),

                "actual_planning_time_seconds":
                    metadata_value(
                        metadata,
                        "actual_planning_time_seconds",
                    ),

                "planning_end_reason":
                    metadata_value(
                        metadata,
                        "planning_end_reason",
                    ),

                "planned_writing_limit_seconds":
                    metadata_value(
                        metadata,
                        "planned_writing_limit_seconds",
                    ),

                "actual_writing_time_ms":
                    metadata_value(
                        metadata,
                        "actual_writing_time_ms",
                    ),

                "actual_writing_time_seconds":
                    metadata_value(
                        metadata,
                        "actual_writing_time_seconds",
                    ),

                "writing_end_reason":
                    metadata_value(
                        metadata,
                        "writing_end_reason",
                    ),

                "word_count":
                    metadata_value(
                        metadata,
                        "final_word_count",
                    ),

                "snapshot_count":
                    (
                        ""
                        if snapshot_count is None
                        else snapshot_count
                    ),

                "keystroke_rows":
                    (
                        ""
                        if keystroke_rows is None
                        else keystroke_rows
                    ),

                "ai_chat_present":
                    ai_chat_present,

                "source_zip":
                    path.name,

                "raw_text_file":
                    f"texts_raw/{raw_filename}",

                "warnings":
                    " | ".join(warnings),
            }

            return ParsedPackage(
                source_zip=path,
                manifest_row=manifest_row,
                essay_bytes=essay_bytes,
                research_sha256=fingerprint,
                warnings=warnings,
            )

    except zipfile.BadZipFile as exc:
        raise PackageError(
            f"Invalid ZIP archive: {exc}"
        ) from exc

    except OSError as exc:
        raise PackageError(
            f"Cannot read ZIP archive: {exc}"
        ) from exc


def atomic_write_bytes(
    path: Path,
    content: bytes,
) -> None:
    """Write generated binary output atomically."""
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    handle = tempfile.NamedTemporaryFile(
        dir=path.parent,
        delete=False,
    )

    temporary_path = Path(
        handle.name
    )

    try:
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(
                handle.fileno()
            )

        os.replace(
            temporary_path,
            path,
        )

    except Exception:
        temporary_path.unlink(
            missing_ok=True
        )
        raise


def atomic_write_csv(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, Any]],
) -> None:
    """Write a UTF-8 CSV atomically."""
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="",
        dir=path.parent,
        delete=False,
    )

    temporary_path = Path(
        handle.name
    )

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
            os.fsync(
                handle.fileno()
            )

        os.replace(
            temporary_path,
            path,
        )

    except Exception:
        temporary_path.unlink(
            missing_ok=True
        )
        raise


def process_batch(
    input_dir: Path | str = DEFAULT_INPUT_DIR,
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
) -> BatchSummary:
    """
    Process every ZIP directly inside input_dir.

    Failures are isolated per ZIP and recorded in
    processing_report.csv.
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)

    if not input_path.is_dir():
        raise ValueError(
            "Input folder does not exist or is not "
            f"a directory: {input_path}"
        )

    if (
        input_path.resolve()
        == output_path.resolve()
    ):
        raise ValueError(
            "Input and output folders must be different"
        )

    zip_paths = sorted(
        (
            path
            for path in input_path.iterdir()
            if (
                path.is_file()
                and path.suffix.lower() == ".zip"
            )
        ),
        key=lambda path: path.name.casefold(),
    )

    output_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    (
        output_path
        / "texts_raw"
    ).mkdir(
        parents=True,
        exist_ok=True,
    )

    manifest_rows: list[
        dict[str, Any]
    ] = []

    report_rows: list[
        dict[str, Any]
    ] = []

    seen: dict[
        tuple[str, int, str],
        ParsedPackage,
    ] = {}

    processed = 0
    duplicates = 0
    failed = 0

    for zip_path in zip_paths:
        try:
            package = process_zip(
                zip_path
            )

            row = package.manifest_row

            identity = (
                str(
                    row["participant"]
                ),
                int(
                    row["task"]
                ),
                str(
                    row["condition"]
                ),
            )

            previous = seen.get(
                identity
            )

            if previous is not None:
                if (
                    previous.research_sha256
                    == package.research_sha256
                ):
                    duplicates += 1

                    report_rows.append(
                        {
                            "source_zip":
                                zip_path.name,

                            "status":
                                "duplicate_identical",

                            "participant":
                                row["participant"],

                            "task":
                                row["task"],

                            "condition":
                                row["condition"],

                            "raw_text_file":
                                previous.manifest_row[
                                    "raw_text_file"
                                ],

                            "warnings":
                                "identical "
                                "participant-task-condition "
                                "already processed; "
                                "no second manifest row",

                            "error":
                                "",
                        }
                    )

                    continue

                raise PackageError(
                    "Conflicting packages share "
                    "participant-task-condition but "
                    "have different research contents: "
                    f"{previous.source_zip.name} and "
                    f"{zip_path.name}"
                )

            raw_path = (
                output_path
                / str(
                    row["raw_text_file"]
                )
            )

            # The final essay is exported byte-for-byte.
            atomic_write_bytes(
                raw_path,
                package.essay_bytes,
            )

            seen[identity] = package
            manifest_rows.append(row)
            processed += 1

            report_rows.append(
                {
                    "source_zip":
                        zip_path.name,

                    "status":
                        "processed",

                    "participant":
                        row["participant"],

                    "task":
                        row["task"],

                    "condition":
                        row["condition"],

                    "raw_text_file":
                        row["raw_text_file"],

                    "warnings":
                        row["warnings"],

                    "error":
                        "",
                }
            )

        except PackageError as exc:
            failed += 1

            report_rows.append(
                {
                    "source_zip":
                        zip_path.name,

                    "status":
                        "error",

                    "participant":
                        "",

                    "task":
                        "",

                    "condition":
                        "",

                    "raw_text_file":
                        "",

                    "warnings":
                        "",

                    "error":
                        str(exc),
                }
            )

    manifest_rows.sort(
        key=lambda row: (
            str(
                row["participant"]
            ),
            int(
                row["task"]
            ),
            str(
                row["condition"]
            ),
        )
    )

    manifest_path = (
        output_path
        / "manifest.csv"
    )

    report_path = (
        output_path
        / "processing_report.csv"
    )

    atomic_write_csv(
        manifest_path,
        MANIFEST_FIELDS,
        manifest_rows,
    )

    atomic_write_csv(
        report_path,
        REPORT_FIELDS,
        report_rows,
    )

    return BatchSummary(
        discovered=len(zip_paths),
        processed=processed,
        duplicates=duplicates,
        failed=failed,
        manifest_path=manifest_path,
        report_path=report_path,
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mtdps",
        description=(
            "Read Gamma ZIP data packages and "
            "export manifest.csv plus raw essays."
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
        help=(
            "Generated output folder "
            f"(default: {DEFAULT_OUTPUT_DIR})"
        ),
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
        summary = process_batch(
            args.input_dir,
            args.output_dir,
        )

    except ValueError as exc:
        parser.error(
            str(exc)
        )
        return 2

    print(
        f"MTDPS v{VERSION}: "
        f"discovered={summary.discovered}, "
        f"processed={summary.processed}, "
        f"duplicates={summary.duplicates}, "
        f"failed={summary.failed}"
    )

    print(
        f"Input folder: {args.input_dir}"
    )

    print(
        f"Manifest: {summary.manifest_path}"
    )

    print(
        "Processing report: "
        f"{summary.report_path}"
    )

    # Successfully processed data remain available even if another
    # ZIP in the same batch fails.
    return 1 if summary.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
