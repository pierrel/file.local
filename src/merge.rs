use std::ops::Range;

use serde::{Deserialize, Serialize};
use similar::{Algorithm, DiffOp, capture_diff_slices};

pub const MAX_INPUT_BYTES: usize = 256 * 1024;
pub const MAX_INPUT_LINES: usize = 4_096;
pub const MAX_WORK: usize = 8_000_000;
pub const MAX_HUNKS: usize = 1_024;
pub const MAX_OUTPUT_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FallbackReason {
    AbsentBase,
    UnequalBase,
    IncompatibleKind,
    Legacy,
    InvalidUtf8,
    ContainsNul,
    InputBytes,
    InputLines,
    ComparisonWork,
    TooManyHunks,
    OutputBytes,
    RoundMergeBudget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConflictHunk {
    pub base: Range<usize>,
    pub a: Range<usize>,
    pub b: Range<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeResult {
    pub bytes: Vec<u8>,
    pub hunks: Vec<ConflictHunk>,
}

#[derive(Clone, Debug)]
struct Edit {
    base: Range<usize>,
    side: Range<usize>,
}

pub fn merge(
    base: &[u8],
    a: &[u8],
    b: &[u8],
    a_wins_overlap: bool,
) -> Result<MergeResult, FallbackReason> {
    let work = comparison_work(base, a, b)?;
    let base = lines(base);
    let a = lines(a);
    let b = lines(b);
    if work > MAX_WORK {
        return Err(FallbackReason::ComparisonWork);
    }

    let a_edits = edits(&base, &a);
    let b_edits = edits(&base, &b);
    compose(&base, &a, &b, &a_edits, &b_edits, a_wins_overlap)
}

pub fn comparison_work(base: &[u8], a: &[u8], b: &[u8]) -> Result<usize, FallbackReason> {
    for input in [base, a, b] {
        if input.len() > MAX_INPUT_BYTES {
            return Err(FallbackReason::InputBytes);
        }
        if input.contains(&0) {
            return Err(FallbackReason::ContainsNul);
        }
        std::str::from_utf8(input).map_err(|_| FallbackReason::InvalidUtf8)?;
    }
    let counts = [lines(base).len(), lines(a).len(), lines(b).len()];
    if counts.into_iter().any(|count| count > MAX_INPUT_LINES) {
        return Err(FallbackReason::InputLines);
    }
    comparison_work_for_lines(counts[0], counts[1], counts[2])
}

fn comparison_work_for_lines(base: usize, a: usize, b: usize) -> Result<usize, FallbackReason> {
    base.checked_add(a)
        .and_then(|value| value.checked_mul(value))
        .and_then(|left| {
            base.checked_add(b)
                .and_then(|value| value.checked_mul(value))
                .and_then(|right| left.checked_add(right))
        })
        .ok_or(FallbackReason::ComparisonWork)
}

fn lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut result = Vec::new();
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b'\n' {
            result.push(&bytes[start..=index]);
            start = index + 1;
        }
    }
    if start < bytes.len() {
        result.push(&bytes[start..]);
    }
    result
}

fn edits(base: &[&[u8]], side: &[&[u8]]) -> Vec<Edit> {
    capture_diff_slices(Algorithm::Myers, base, side)
        .into_iter()
        .flat_map(|operation| match operation {
            DiffOp::Equal { .. } => Vec::new(),
            operation => {
                let base_range = operation.old_range();
                let side_range = operation.new_range();
                if base_range.len() == side_range.len() && !base_range.is_empty() {
                    (0..base_range.len())
                        .filter(|offset| {
                            base[base_range.start + offset] != side[side_range.start + offset]
                        })
                        .map(|offset| Edit {
                            base: base_range.start + offset..base_range.start + offset + 1,
                            side: side_range.start + offset..side_range.start + offset + 1,
                        })
                        .collect()
                } else {
                    vec![Edit {
                        base: base_range,
                        side: side_range,
                    }]
                }
            }
        })
        .collect()
}

fn compose(
    base: &[&[u8]],
    a: &[&[u8]],
    b: &[&[u8]],
    a_edits: &[Edit],
    b_edits: &[Edit],
    a_wins_overlap: bool,
) -> Result<MergeResult, FallbackReason> {
    let mut output = Vec::new();
    let mut hunks = Vec::new();
    let mut ai = 0;
    let mut bi = 0;
    let mut cursor = 0;

    while ai < a_edits.len() || bi < b_edits.len() {
        let next_start = match (a_edits.get(ai), b_edits.get(bi)) {
            (Some(a), Some(b)) => a.base.start.min(b.base.start),
            (Some(a), None) => a.base.start,
            (None, Some(b)) => b.base.start,
            (None, None) => unreachable!(),
        };
        append(&mut output, &base[cursor..next_start])?;

        let start = next_start;
        let mut end = start;
        let a_start = ai;
        let b_start = bi;
        loop {
            let mut extended = false;
            while let Some(edit) = a_edits.get(ai) {
                if joins(edit, start, end) {
                    end = end.max(edit.base.end);
                    ai += 1;
                    extended = true;
                } else {
                    break;
                }
            }
            while let Some(edit) = b_edits.get(bi) {
                if joins(edit, start, end) {
                    end = end.max(edit.base.end);
                    bi += 1;
                    extended = true;
                } else {
                    break;
                }
            }
            if !extended {
                break;
            }
        }

        let a_group = &a_edits[a_start..ai];
        let b_group = &b_edits[b_start..bi];
        if a_group.is_empty() {
            append_rendered(&mut output, base, b, start..end, b_group)?;
        } else if b_group.is_empty() {
            append_rendered(&mut output, base, a, start..end, a_group)?;
        } else {
            let base_bytes = flatten(&base[start..end])?;
            let a_bytes = render(base, a, start..end, a_group)?;
            let b_bytes = render(base, b, start..end, b_group)?;
            if a_bytes == b_bytes {
                append_bytes(&mut output, &a_bytes)?;
            } else if a_bytes == base_bytes {
                append_bytes(&mut output, &b_bytes)?;
            } else if b_bytes == base_bytes || a_wins_overlap {
                append_bytes(&mut output, &a_bytes)?;
                if b_bytes != base_bytes {
                    push_hunk(&mut hunks, start..end, a_group, b_group)?;
                }
            } else {
                append_bytes(&mut output, &b_bytes)?;
                push_hunk(&mut hunks, start..end, a_group, b_group)?;
            }
        }
        cursor = end;
    }
    append(&mut output, &base[cursor..])?;
    Ok(MergeResult {
        bytes: output,
        hunks,
    })
}

fn joins(edit: &Edit, start: usize, end: usize) -> bool {
    if end == start {
        edit.base.start == start
    } else {
        edit.base.start < end
    }
}

fn render(
    base: &[&[u8]],
    side: &[&[u8]],
    range: Range<usize>,
    edits: &[Edit],
) -> Result<Vec<u8>, FallbackReason> {
    let mut output = Vec::new();
    let mut cursor = range.start;
    for edit in edits {
        append(&mut output, &base[cursor..edit.base.start])?;
        append(&mut output, &side[edit.side.clone()])?;
        cursor = edit.base.end;
    }
    append(&mut output, &base[cursor..range.end])?;
    Ok(output)
}

fn append_rendered(
    output: &mut Vec<u8>,
    base: &[&[u8]],
    side: &[&[u8]],
    range: Range<usize>,
    edits: &[Edit],
) -> Result<(), FallbackReason> {
    append_bytes(output, &render(base, side, range, edits)?)
}

fn flatten(lines: &[&[u8]]) -> Result<Vec<u8>, FallbackReason> {
    let mut output = Vec::new();
    append(&mut output, lines)?;
    Ok(output)
}

fn append(output: &mut Vec<u8>, lines: &[&[u8]]) -> Result<(), FallbackReason> {
    for line in lines {
        append_bytes(output, line)?;
    }
    Ok(())
}

fn append_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), FallbackReason> {
    let size = output
        .len()
        .checked_add(bytes.len())
        .ok_or(FallbackReason::OutputBytes)?;
    if size > MAX_OUTPUT_BYTES {
        return Err(FallbackReason::OutputBytes);
    }
    output.extend_from_slice(bytes);
    Ok(())
}

fn push_hunk(
    hunks: &mut Vec<ConflictHunk>,
    base: Range<usize>,
    a: &[Edit],
    b: &[Edit],
) -> Result<(), FallbackReason> {
    if hunks.len() == MAX_HUNKS {
        return Err(FallbackReason::TooManyHunks);
    }
    hunks.push(ConflictHunk {
        base,
        a: side_span(a),
        b: side_span(b),
    });
    Ok(())
}

fn side_span(edits: &[Edit]) -> Range<usize> {
    let start = edits.first().map_or(0, |edit| edit.side.start);
    let end = edits.last().map_or(start, |edit| edit.side.end);
    start..end
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged(base: &str, a: &str, b: &str) -> MergeResult {
        merge(base.as_bytes(), a.as_bytes(), b.as_bytes(), true).unwrap()
    }

    #[test]
    fn combines_nonoverlapping_and_identical_changes() {
        let result = merged(
            "one\ntwo\nthree\n",
            "ONE\ntwo\nthree\n",
            "one\ntwo\nTHREE\n",
        );
        assert_eq!(result.bytes, b"ONE\ntwo\nTHREE\n");
        assert!(result.hunks.is_empty());

        let result = merged("one\n", "ONE\n", "ONE\n");
        assert_eq!(result.bytes, b"ONE\n");
        assert!(result.hunks.is_empty());
    }

    #[test]
    fn ordered_overlap_has_no_markers_and_records_the_hunk() {
        let result = merged("one\ntwo\n", "A\ntwo\n", "B\ntwo\n");
        assert_eq!(result.bytes, b"A\ntwo\n");
        assert_eq!(result.hunks.len(), 1);
        assert_eq!(result.hunks[0].base, 0..1);

        let result = merged(
            "top: base\nmiddle: base\nbottom: base\n",
            "top: a\nmiddle: a\nbottom: base\n",
            "top: base\nmiddle: b\nbottom: b\n",
        );
        assert_eq!(result.bytes, b"top: a\nmiddle: a\nbottom: b\n");
        assert_eq!(result.hunks.len(), 1);
        assert_eq!(result.hunks[0].base, 1..2);
    }

    #[test]
    fn handles_inserts_deletes_empty_and_final_newlines() {
        assert_eq!(merged("", "a\n", "").bytes, b"a\n");
        assert_eq!(merged("a\n", "", "a\nb\n").bytes, b"b\n");
        assert_eq!(merged("a\nb", "A\nb", "a\nb\n").bytes, b"A\nb\n");
        assert_eq!(
            merged("a\r\nb\r\n", "A\r\nb\r\n", "a\r\nB\r\n").bytes,
            b"A\r\nB\r\n"
        );
    }

    #[test]
    fn preserves_anchors_above_two_hundred_lines_and_repeated_lines() {
        let base = (0..250).map(|i| format!("line {i}\n")).collect::<String>();
        let a = base.replace("line 10\n", "line ten\n");
        let b = base.replace("line 240\n", "line two-forty\n");
        let result = merged(&base, &a, &b);
        assert!(
            String::from_utf8(result.bytes)
                .unwrap()
                .contains("line ten\n")
        );
        assert!(
            merged("x\nx\ny\n", "a\nx\ny\n", "x\nx\nb\n")
                .hunks
                .is_empty()
        );
    }

    #[test]
    fn rejects_binary_and_resource_excess() {
        assert_eq!(
            merge(b"\0", b"", b"", true),
            Err(FallbackReason::ContainsNul)
        );
        assert_eq!(
            merge(b"\xff", b"", b"", true),
            Err(FallbackReason::InvalidUtf8)
        );
        assert_eq!(
            comparison_work(b"\0", b"", b""),
            Err(FallbackReason::ContainsNul)
        );
        let too_many = "x\n".repeat(MAX_INPUT_LINES + 1);
        assert_eq!(
            merge(too_many.as_bytes(), b"", b"", true),
            Err(FallbackReason::InputLines)
        );
    }
}
