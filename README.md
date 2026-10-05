# leoniu-minor-thesis-mtdps
Minor Thesis Data Processing System

## 0.1.1 - Oct 4, 2026
Implement the Minor Thesis Data Processing System (MTDPS) in Python, version 0.1.1. This includes functions for processing ZIP files containing participant data, generating reports, and managing metadata.

## 0.2.0 - Oct 4, 2026
Extract writing time series.

## 0.2.1 - Oct 4, 2026
Completed task-level and raw-text extraction, and established a closed-loop word-count process series spanning from minute-by-minute snapshots to the actual point of writing completion.

---

## 0.3 - Quality Control Version Series
### 0.3.0 - Oct 4, 2026
- Added `output/qc_report.csv`;
- Each QC item occupies a single row;
- Fields include:
source_zip
participant
task
condition
rule
category
severity
result
evidence

- severity: INFO / WARNING / FLAG / ERROR
- result: PASS / FAIL / NOT_CHECKED
- evidence is provided as JSON for further parsing;
- severity indicates the level of severity only and does not automatically trigger exclusion;
- The original "warnings" column now retains only a brief summary rather than aggregating specific check results.
Checks now covered:
- ZIP, CRC, system files, file paths, and mandatory file checks;
- Cross-file identity verification across metadata, snapshots, keystrokes, and AI chat;
- Verification of `planning_mode` + `familiarity` against the condition code;
- Pairwise cross-validation of the three word counts;
- Flagging for 200–250 word protocols;
- Snapshot header, start minute, duplicates, missing entries, continuity, and embedded word counts;
- Verification of snapshot count against `floor(actual_writing_time / 60)`;
- Keystroke start/end events, timing validity, monotonicity, and duplicate events;
- Detection of events occurring after the writing-end timestamp;
- Verification of the writing-end timestamp against metadata.

### 0.3.1 - Oct 5, 2026
Time consistency tolerance updated: ≤10 ms = PASS, >10 ms = WARNING; evidence records the actual difference and the tolerance value.
Added `NOT_APPLICABLE` status; used for AI chat content and identity checks in non-AI conditions.
Replay the full `beforeinput`/`input` editing chain starting from empty text and compare the result precisely against `final_text.txt`.
Browser cursor positions interpreted via UTF-16 offsets to ensure compatibility with non-BMP characters.
Added QC checks for anomalous editing:
- Large insertions: default ≥50 characters
- Batch replacements: default removal of ≥20 characters in a single operation
- `input` events lacking a matching `beforeinput`
- `paste`, `drop`, or `yank` events
- Pasted content already present in the essay: `possibly_internal_copy`
- Otherwise: `source_unknown_or_external` (avoids assuming the source is definitely external)
Duplicate detection logic updated to first ensure uniqueness of the `(participant, task)` pair; encountering different conditions for the same task triggers `PARTICIPANT_TASK_CONDITION_CONFLICT` / `ERROR`.
`LAST_SNAPSHOT_MATCHES_FINAL_ESSAY` reclassified as `INFO` (excluded from the legacy warnings summary); other technical anomalies regarding snapshots or time-series data remain classified as `WARNING`.
Maintained compatibility with version 0.3.0 time-series and raw essay output formats.

---

## 0.4 - Writing Trajectory Visualiser (WTV)
Modules: matplotlib, pandas
**Copy following codes to the Terminal before you run the code.**
```
python -m pip install matplotlib pandas
```

### 0.4.0 - Oct 5, 2026
- Generate a 2×3 panel layout based on `AH | IH | NH / AL | IL | NL`.
- Plot each participant-task combination as a separate line, adding only the derived (0,0) point in memory.
- Use `elapsed_time_seconds` / 60; retain the rise, fall, and negative delta at the endpoint.
- Define the `writing_end` strictly as the trajectory endpoint; issue an explicit warning if data is missing or duplicated.
- Output as 300 dpi PNG and vector PDF.

### 0.4.0b - Oct 5, 2026

