# leoniu-minor-thesis-mtdps
Minor Thesis Data Processing System

## 0.1.1 - Oct 4, 2026
Implement the Minor Thesis Data Processing System (MTDPS) in Python, version 0.1.1. This includes functions for processing ZIP files containing participant data, generating reports, and managing metadata.

## 0.2.0 - Oct 4, 2026
Extract writing time series.

## 0.2.1 - Oct 4, 2026
Completed task-level and raw-text extraction, and established a closed-loop word-count process series spanning from minute-by-minute snapshots to the actual point of writing completion.

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

### 0.3.1 - Oct 4, 2026
