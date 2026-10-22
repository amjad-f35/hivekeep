//! Reading per-test results from a JUnit report, in the shape cargo-nextest
//! writes to `target/nextest/<profile>/junit.xml`:
//!
//! ```text
//! <testsuites name="nextest-run" ...>
//!   <testsuite name="<binary id>" ...>
//!     <testcase name="<test path>" classname="<binary id>" time="0.01">
//!       <failure type="test failure" message="...">panic report</failure>
//!       <system-out>captured stdout</system-out>
//!       <system-err>captured stderr</system-err>
//!     </testcase>
//!   </testsuite>
//! </testsuites>
//! ```
//!
//! A test that passed only on a retry carries `<flakyFailure>` children and no
//! `<failure>`; it counts as passed. Output nested inside retry elements is
//! ignored, since the final attempt's output is what explains the result.

use quick_xml::escape::resolve_predefined_entity;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

use super::excerpt::{lines_of, panic_block, tail, MAX_CONTEXT_LINES, MAX_MESSAGE_LINES};
use super::{Failure, Run};

#[derive(Debug, Default)]
struct Case {
    name: String,
    classname: String,
    time: f64,
    failed: bool,
    skipped: bool,
    /// The failure element's `type`, e.g. `test failure` or `test timeout`.
    kind: String,
    /// The failure element's `message` attribute.
    summary: String,
    body: String,
    out: String,
    err: String,
}

/// Adds the testcases in the JUnit document `raw` to `run`.
pub(super) fn parse(raw: &str, run: &mut Run) -> Result<(), String> {
    let mut reader = Reader::from_str(raw);
    let mut suite = String::new();
    let mut case: Option<Case> = None;
    // Element names from the root down to the current element.
    let mut stack: Vec<Vec<u8>> = Vec::new();

    loop {
        let ev = reader.read_event().map_err(|e| e.to_string())?;
        match ev {
            Event::Start(e) => {
                open(&e, &stack, &mut suite, &mut case)?;
                stack.push(e.local_name().as_ref().to_vec());
            }
            Event::Empty(e) => {
                open(&e, &stack, &mut suite, &mut case)?;
                if e.local_name().as_ref() == b"testcase" {
                    finish(run, &suite, case.take());
                }
            }
            Event::End(e) => {
                stack.pop();
                if e.local_name().as_ref() == b"testcase" {
                    finish(run, &suite, case.take());
                }
            }
            Event::Text(t) => {
                let s = t.xml_content().map_err(|e| e.to_string())?;
                append(&stack, case.as_mut(), &s);
            }
            Event::CData(t) => {
                let s = t.decode().map_err(|e| e.to_string())?;
                append(&stack, case.as_mut(), &s);
            }
            Event::GeneralRef(r) => {
                let s = match r.resolve_char_ref().map_err(|e| e.to_string())? {
                    Some(c) => c.to_string(),
                    None => {
                        let name = r.decode().map_err(|e| e.to_string())?;
                        resolve_predefined_entity(&name)
                            .ok_or_else(|| format!("unknown entity &{name};"))?
                            .to_string()
                    }
                };
                append(&stack, case.as_mut(), &s);
            }
            Event::Eof => {
                return if stack.is_empty() {
                    Ok(())
                } else {
                    Err("unexpected EOF".into())
                };
            }
            _ => {}
        }
    }
}

/// Handles an opening (or self-closing) element at depth `stack.len()`.
fn open(
    e: &BytesStart<'_>,
    stack: &[Vec<u8>],
    suite: &mut String,
    case: &mut Option<Case>,
) -> Result<(), String> {
    let name = e.local_name();
    let parent = stack.last().map(Vec::as_slice);
    match (name.as_ref(), parent) {
        (b"testsuite", _) => *suite = attr(e, b"name")?,
        (b"testcase", _) => {
            *case = Some(Case {
                name: attr(e, b"name")?,
                classname: attr(e, b"classname")?,
                time: attr(e, b"time")?.trim().parse().unwrap_or_default(),
                ..Case::default()
            });
        }
        (b"failure" | b"error", Some(b"testcase")) => {
            if let Some(c) = case.as_mut() {
                c.failed = true;
                c.kind = attr(e, b"type")?;
                c.summary = attr(e, b"message")?;
            }
        }
        (b"skipped", Some(b"testcase")) => {
            if let Some(c) = case.as_mut() {
                c.skipped = true;
            }
        }
        _ => {}
    }
    Ok(())
}

fn attr(e: &BytesStart<'_>, key: &[u8]) -> Result<String, String> {
    for a in e.attributes() {
        let a = a.map_err(|e| e.to_string())?;
        if a.key.local_name().as_ref() == key {
            return Ok(a.unescape_value().map_err(|e| e.to_string())?.into_owned());
        }
    }
    Ok(String::new())
}

/// Adds text to the case slot the innermost element feeds, if any. Only
/// direct children of the testcase count, which leaves out retry attempts.
fn append(stack: &[Vec<u8>], case: Option<&mut Case>, s: &str) {
    let Some(c) = case else {
        return;
    };
    let [.., parent, top] = stack else {
        return;
    };
    if parent.as_slice() != b"testcase" {
        return;
    }
    match top.as_slice() {
        b"failure" | b"error" => c.body.push_str(s),
        b"system-out" => c.out.push_str(s),
        b"system-err" => c.err.push_str(s),
        _ => {}
    }
}

fn finish(run: &mut Run, suite: &str, case: Option<Case>) {
    let Some(c) = case else {
        return;
    };
    if c.skipped && !c.failed {
        run.skipped += 1;
        return;
    }
    if !c.failed {
        run.passed += 1;
        return;
    }
    run.failed += 1;

    let body = lines_of(&c.body);
    let captured: Vec<String> = lines_of(&c.out)
        .into_iter()
        .chain(lines_of(&c.err))
        .collect();
    let mut message = Vec::new();
    // A crash or timeout says more in its type than a panic report would.
    if !c.kind.is_empty() && c.kind != "test failure" {
        message.push(format!("({})", c.kind));
    }
    let explained = panic_block(&body, MAX_MESSAGE_LINES)
        .or_else(|| panic_block(&captured, MAX_MESSAGE_LINES))
        .or_else(|| (!body.is_empty()).then(|| tail(&body, MAX_MESSAGE_LINES)))
        .or_else(|| (!c.summary.is_empty()).then(|| lines_of(&c.summary)))
        .unwrap_or_else(|| tail(&captured, MAX_MESSAGE_LINES));
    message.extend(explained);
    message.truncate(MAX_MESSAGE_LINES);

    let context = tail(&captured, MAX_CONTEXT_LINES);
    run.failures.push(Failure {
        pkg: if c.classname.is_empty() {
            suite.to_string()
        } else {
            c.classname
        },
        test: c.name,
        elapsed: c.time,
        context: if context == message {
            Vec::new()
        } else {
            context
        },
        message,
        ..Failure::default()
    });
}
