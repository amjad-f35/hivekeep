//! Building blocks for declarative field validation.
//!
//! Each builder checks one field. Rules run in the order they are chained
//! and some stop the chain when they fail ("short-circuit"): update rules,
//! presence and length/size bounds. The conventional order is
//!
//! 1. update rules (`immutable`, `no_modify`, `no_unset`),
//! 2. presence (`required`, `optional`, `forbidden`),
//! 3. bounds and formats,
//! 4. custom checks.
//!
//! On an update, a field whose value is unchanged from the old object is not
//! revalidated at all ("ratcheting"), so tightening a rule never strands
//! stored objects.
//!
//! `old` arguments follow one convention throughout: `None` means there is
//! no corresponding old value (a create, or a list element with no match),
//! `Some(..)` carries the old value of the same field.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use crate::field::{ErrorList, FieldError, Path};
use crate::names;
use crate::preview::{Gate, PreviewGates};

/// Whether a value is being created or replaces an existing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    /// No previous value exists (also used to validate request messages).
    Create,
    /// A previous value exists and is passed as `old`.
    Update,
}

/// The context a validation runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Operation {
    kind: OpKind,
    gates: PreviewGates,
    opted_out: bool,
}

impl Operation {
    /// A create with no preview gates.
    pub fn create() -> Self {
        Operation {
            kind: OpKind::Create,
            gates: PreviewGates::none(),
            opted_out: false,
        }
    }

    /// An update with no preview gates.
    pub fn update() -> Self {
        Operation {
            kind: OpKind::Update,
            ..Self::create()
        }
    }

    /// Sets the gates enabled on the server.
    pub fn with_gates(mut self, gates: PreviewGates) -> Self {
        self.gates = gates;
        self
    }

    /// Marks the request as not having opted in to preview features, which
    /// turns every gate off for it.
    pub fn with_opt_out(mut self, opted_out: bool) -> Self {
        self.opted_out = opted_out;
        self
    }

    /// The operation kind.
    pub fn kind(&self) -> OpKind {
        self.kind
    }

    /// Whether this is an update.
    pub fn is_update(&self) -> bool {
        self.kind == OpKind::Update
    }

    /// Whether `gate` is in effect for this request: enabled on the server
    /// and not opted out of.
    pub fn gate_enabled(&self, gate: Gate) -> bool {
        !self.opted_out && self.gates.is_enabled(gate)
    }

    /// Whether `gate` is enabled on the server, regardless of the request.
    pub fn server_gate_enabled(&self, gate: Gate) -> bool {
        self.gates.is_enabled(gate)
    }

    /// Whether the request opted out of preview features.
    pub fn opted_out(&self) -> bool {
        self.opted_out
    }

    /// The same context as a create, for validating a request message.
    pub fn as_create(&self) -> Self {
        Operation {
            kind: OpKind::Create,
            ..self.clone()
        }
    }

    /// The same context as an update, for validating a value against the
    /// stored one.
    pub fn as_update(&self) -> Self {
        Operation {
            kind: OpKind::Update,
            ..self.clone()
        }
    }
}

impl Default for Operation {
    fn default() -> Self {
        Self::create()
    }
}

/// A standard string format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// DNS-1123 label (a resource name).
    DnsLabel,
    /// DNS-1123 subdomain.
    DnsSubdomain,
    /// Lowercase canonical UUID.
    Uuid,
    /// Label key.
    LabelKey,
    /// Label value.
    LabelValue,
}

impl Format {
    /// Origin recorded on errors from this format.
    pub fn origin(self) -> &'static str {
        match self {
            Format::DnsLabel => "format=dns-label",
            Format::DnsSubdomain => "format=dns-subdomain",
            Format::Uuid => "format=uuid",
            Format::LabelKey => "format=label-key",
            Format::LabelValue => "format=label-value",
        }
    }

    /// The reasons `value` does not match.
    pub fn check(self, value: &str) -> Vec<String> {
        match self {
            Format::DnsLabel => names::check_dns_label(value),
            Format::DnsSubdomain => names::check_dns_subdomain(value),
            Format::Uuid => names::check_uuid(value),
            Format::LabelKey => names::check_label_key(value),
            Format::LabelValue => names::check_label_value(value),
        }
    }

    /// Validates `value`, reporting at `path`.
    pub fn validate(self, path: &Path, value: &str) -> ErrorList {
        let msgs = self.check(value);
        if msgs.is_empty() {
            ErrorList::new()
        } else {
            FieldError::invalid(path, value, msgs.join("; "))
                .with_origin(self.origin())
                .into()
        }
    }
}

fn immutable_err(path: &Path) -> FieldError {
    FieldError::invalid_opaque(path, "field is immutable").with_origin("immutable")
}

fn no_modify_err(path: &Path) -> FieldError {
    FieldError::invalid_opaque(path, "field cannot be modified once set").with_origin("update")
}

fn no_unset_err(path: &Path) -> FieldError {
    FieldError::invalid_opaque(path, "field cannot be cleared once set").with_origin("update")
}

/// Shared state of every field builder.
///
/// Short-circuiting rules (update rules, presence, size bounds) all run
/// unless the field is ratcheted, so a missing and changed field reports
/// both problems; once any of them fails, the remaining rules are skipped.
struct Base<'a> {
    op: &'a Operation,
    path: Path,
    errs: ErrorList,
    /// The field is unchanged on update and is not revalidated.
    skip: bool,
    /// A short-circuiting rule failed, or an optional value is absent.
    stop: bool,
}

impl<'a> Base<'a> {
    fn new(op: &'a Operation, path: &Path, unchanged: bool) -> Self {
        Base {
            op,
            path: path.clone(),
            errs: ErrorList::new(),
            skip: op.is_update() && unchanged,
            stop: false,
        }
    }

    /// Whether short-circuiting rules still run.
    fn armed(&self) -> bool {
        !self.skip
    }

    /// Whether ordinary rules still run.
    fn live(&self) -> bool {
        !self.skip && !self.stop
    }

    fn fail(&mut self, e: FieldError, short_circuit: bool) {
        self.errs.push(e);
        if short_circuit {
            self.stop = true;
        }
    }
}

// ---------------------------------------------------------------- strings

/// Rules for a string field. Start with [`string`].
pub struct StrRule<'a> {
    b: Base<'a>,
    new: &'a str,
    old: Option<&'a str>,
}

/// Starts validating a string field.
pub fn string<'a>(
    op: &'a Operation,
    path: &Path,
    new: &'a str,
    old: Option<&'a str>,
) -> StrRule<'a> {
    StrRule {
        b: Base::new(op, path, old == Some(new)),
        new,
        old,
    }
}

impl<'a> StrRule<'a> {
    /// On update, any change is an error.
    pub fn immutable(mut self) -> Self {
        if self.b.armed() && self.b.op.is_update() {
            let e = immutable_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a set value may not change to another set value.
    pub fn no_modify(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(o) if !o.is_empty())
            && !self.new.is_empty()
        {
            let e = no_modify_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a set value may not be cleared.
    pub fn no_unset(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(o) if !o.is_empty())
            && self.new.is_empty()
        {
            let e = no_unset_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// The value must be non-empty.
    pub fn required(mut self) -> Self {
        if self.b.armed() && self.new.is_empty() {
            let e = FieldError::required(&self.b.path, "");
            self.b.fail(e, true);
        }
        self
    }

    /// An empty value skips the remaining rules.
    pub fn optional(mut self) -> Self {
        if self.new.is_empty() {
            self.b.stop = true;
        }
        self
    }

    /// The value must be empty.
    pub fn forbidden(mut self) -> Self {
        if self.b.armed() {
            if !self.new.is_empty() {
                let e = FieldError::forbidden(&self.b.path, "");
                self.b.fail(e, true);
            }
            self.b.stop = true;
        }
        self
    }

    /// At most `max` bytes. Not short-circuiting: format and custom checks
    /// still run, so an over-long value also reports what else is wrong.
    pub fn max_len(mut self, max: usize) -> Self {
        if self.b.live() && self.new.len() > max {
            let e = FieldError::too_long(&self.b.path, max).with_origin("maxLength");
            self.b.fail(e, false);
        }
        self
    }

    /// At least `min` bytes.
    pub fn min_len(mut self, min: usize) -> Self {
        if self.b.live() && self.new.len() < min {
            let e = FieldError::invalid(
                &self.b.path,
                self.new,
                format!("must be at least {min} bytes long"),
            )
            .with_origin("minLength");
            self.b.fail(e, false);
        }
        self
    }

    /// Must match `f`.
    pub fn format(mut self, f: Format) -> Self {
        if self.b.live() {
            self.b.errs.extend(f.validate(&self.b.path, self.new));
        }
        self
    }

    /// Runs a custom check.
    pub fn custom(mut self, check: impl FnOnce(&Path, &str) -> ErrorList) -> Self {
        if self.b.live() {
            self.b.errs.extend(check(&self.b.path, self.new));
        }
        self
    }

    /// The collected errors.
    pub fn finish(self) -> ErrorList {
        self.b.errs
    }
}

// ---------------------------------------------------------------- integers

/// Rules for an integer or enum field. Start with [`int`].
pub struct IntRule<'a> {
    b: Base<'a>,
    new: i64,
    old: Option<i64>,
}

/// Starts validating an integer (or proto enum) field.
pub fn int<'a>(
    op: &'a Operation,
    path: &Path,
    new: impl Into<i64>,
    old: Option<impl Into<i64>>,
) -> IntRule<'a> {
    let new = new.into();
    let old = old.map(Into::into);
    IntRule {
        b: Base::new(op, path, old == Some(new)),
        new,
        old,
    }
}

impl IntRule<'_> {
    /// On update, any change is an error.
    pub fn immutable(mut self) -> Self {
        if self.b.armed() && self.b.op.is_update() {
            let e = immutable_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a non-zero value may not change to another non-zero value.
    pub fn no_modify(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(o) if o != 0)
            && self.new != 0
        {
            let e = no_modify_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a non-zero value may not return to zero.
    pub fn no_unset(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(o) if o != 0)
            && self.new == 0
        {
            let e = no_unset_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, the value may not decrease.
    pub fn monotonic(mut self) -> Self {
        if self.b.live() && self.b.op.is_update() {
            if let Some(old) = self.old {
                if self.new < old {
                    let e = FieldError::invalid(
                        &self.b.path,
                        self.new,
                        format!("may not be decreased from {old}"),
                    )
                    .with_origin("monotonic");
                    self.b.fail(e, false);
                }
            }
        }
        self
    }

    /// The value must be non-zero.
    pub fn required(mut self) -> Self {
        if self.b.armed() && self.new == 0 {
            let e = FieldError::required(&self.b.path, "");
            self.b.fail(e, true);
        }
        self
    }

    /// Zero skips the remaining rules.
    pub fn optional(mut self) -> Self {
        if self.new == 0 {
            self.b.stop = true;
        }
        self
    }

    /// At least `min`.
    pub fn min(mut self, min: i64) -> Self {
        if self.b.live() && self.new < min {
            let e = FieldError::invalid(
                &self.b.path,
                self.new,
                format!("must be greater than or equal to {min}"),
            )
            .with_origin("minimum");
            self.b.fail(e, false);
        }
        self
    }

    /// At most `max`.
    pub fn max(mut self, max: i64) -> Self {
        if self.b.live() && self.new > max {
            let e = FieldError::invalid(
                &self.b.path,
                self.new,
                format!("must be less than or equal to {max}"),
            )
            .with_origin("maximum");
            self.b.fail(e, false);
        }
        self
    }

    /// The collected errors.
    pub fn finish(self) -> ErrorList {
        self.b.errs
    }
}

// ---------------------------------------------------------------- bytes

/// Validates a bytes field: presence and an upper bound.
pub fn bytes(path: &Path, value: &[u8], required: bool, max: usize) -> ErrorList {
    if value.is_empty() {
        return if required {
            FieldError::required(path, "").into()
        } else {
            ErrorList::new()
        };
    }
    if value.len() > max {
        return FieldError::too_long(path, max)
            .with_origin("maxBytes")
            .into();
    }
    ErrorList::new()
}

// ---------------------------------------------------------------- messages

/// Rules for a message field. Start with [`message`].
pub struct MsgRule<'a, T> {
    b: Base<'a>,
    new: Option<&'a T>,
    old: Option<Option<&'a T>>,
}

/// Starts validating a message field. `old` is `None` when there is no old
/// parent, and `Some(field)` otherwise.
pub fn message<'a, T: PartialEq>(
    op: &'a Operation,
    path: &Path,
    new: Option<&'a T>,
    old: Option<Option<&'a T>>,
) -> MsgRule<'a, T> {
    MsgRule {
        b: Base::new(op, path, old == Some(new)),
        new,
        old,
    }
}

impl<'a, T> MsgRule<'a, T> {
    /// On update, any change (including setting or clearing) is an error.
    pub fn immutable(mut self) -> Self {
        if self.b.armed() && self.b.op.is_update() {
            let e = immutable_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a set message may not be replaced by another one.
    pub fn no_modify(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(Some(_)))
            && self.new.is_some()
        {
            let e = no_modify_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// On update, a set message may not be cleared.
    pub fn no_unset(mut self) -> Self {
        if self.b.armed()
            && self.b.op.is_update()
            && matches!(self.old, Some(Some(_)))
            && self.new.is_none()
        {
            let e = no_unset_err(&self.b.path);
            self.b.fail(e, true);
        }
        self
    }

    /// The message must be set.
    pub fn required(mut self) -> Self {
        if self.b.armed() && self.new.is_none() {
            let e = FieldError::required(&self.b.path, "");
            self.b.fail(e, true);
        }
        self
    }

    /// An unset message skips the remaining rules.
    pub fn optional(mut self) -> Self {
        if self.new.is_none() {
            self.b.stop = true;
        }
        self
    }

    /// The message must not be set while `gate` is off for this request.
    pub fn needs_gate(mut self, gate: Gate) -> Self {
        if self.b.armed() && !self.b.op.gate_enabled(gate) && self.new.is_some() {
            let e =
                FieldError::forbidden(&self.b.path, format!("requires the {gate} preview gate"))
                    .with_origin("ifDisabled");
            self.b.fail(e, true);
        }
        self
    }

    /// Validates the message's own fields.
    pub fn then(mut self, f: impl FnOnce(&Path, &'a T, Option<&'a T>) -> ErrorList) -> Self {
        if self.b.live() {
            if let Some(new) = self.new {
                let old = self.old.flatten();
                self.b.errs.extend(f(&self.b.path, new, old));
            }
        }
        self
    }

    /// The collected errors.
    pub fn finish(self) -> ErrorList {
        self.b.errs
    }
}

// ---------------------------------------------------------------- lists

/// Rules for a repeated field. Start with [`list`].
pub struct ListRule<'a, T> {
    b: Base<'a>,
    new: &'a [T],
    old: Option<&'a [T]>,
}

/// Starts validating a repeated field.
pub fn list<'a, T: PartialEq>(
    op: &'a Operation,
    path: &Path,
    new: &'a [T],
    old: Option<&'a [T]>,
) -> ListRule<'a, T> {
    ListRule {
        b: Base::new(op, path, old == Some(new)),
        new,
        old,
    }
}

impl<'a, T> ListRule<'a, T> {
    /// At most `max` items.
    pub fn max_items(mut self, max: usize) -> Self {
        if self.b.armed() && self.new.len() > max {
            let e = FieldError::too_many(&self.b.path, self.new.len(), max).with_origin("maxItems");
            self.b.fail(e, true);
        }
        self
    }

    /// At least one item.
    pub fn required(mut self) -> Self {
        if self.b.armed() && self.new.is_empty() {
            let e = FieldError::required(&self.b.path, "");
            self.b.fail(e, true);
        }
        self
    }

    /// An empty list skips the remaining rules.
    pub fn optional(mut self) -> Self {
        if self.new.is_empty() {
            self.b.stop = true;
        }
        self
    }

    /// At least `min` items.
    pub fn min_items(mut self, min: usize) -> Self {
        if self.b.live() && self.new.len() < min {
            let e = FieldError::too_few(&self.b.path, self.new.len(), min).with_origin("minItems");
            self.b.fail(e, false);
        }
        self
    }

    /// Validates every item, without correlating it to an old item.
    pub fn each(mut self, f: impl Fn(&Path, &'a T, Option<&'a T>) -> ErrorList) -> Self {
        if self.b.live() {
            for (i, item) in self.new.iter().enumerate() {
                self.b.errs.extend(f(&self.b.path.index(i), item, None));
            }
        }
        self
    }

    /// Validates every item, passing the old item with the same key.
    pub fn each_keyed<K: PartialEq>(
        mut self,
        key: impl Fn(&T) -> K,
        f: impl Fn(&Path, &'a T, Option<&'a T>) -> ErrorList,
    ) -> Self {
        if self.b.live() {
            for (i, item) in self.new.iter().enumerate() {
                let k = key(item);
                let old = self.old.and_then(|o| o.iter().find(|x| key(x) == k));
                self.b.errs.extend(f(&self.b.path.index(i), item, old));
            }
        }
        self
    }

    /// No two items may share a key. Repeats are reported at the later item.
    pub fn unique_by<K: Eq + Hash + Debug>(mut self, key: impl Fn(&T) -> K) -> Self {
        if self.b.live() {
            let mut seen = std::collections::HashSet::new();
            for (i, item) in self.new.iter().enumerate() {
                let k = key(item);
                if seen.contains(&k) {
                    self.b
                        .errs
                        .push(FieldError::duplicate(&self.b.path.index(i), &k));
                } else {
                    seen.insert(k);
                }
            }
        }
        self
    }

    /// Runs a custom check over the whole list.
    pub fn custom(mut self, check: impl FnOnce(&Path, &'a [T]) -> ErrorList) -> Self {
        if self.b.live() {
            self.b.errs.extend(check(&self.b.path, self.new));
        }
        self
    }

    /// The collected errors.
    pub fn finish(self) -> ErrorList {
        self.b.errs
    }
}

impl<'a, T: Clone + Eq + Hash + Debug> ListRule<'a, T> {
    /// No value may repeat.
    pub fn unique_values(self) -> Self {
        self.unique_by(T::clone)
    }
}

// ---------------------------------------------------------------- maps

/// Rules for a `map<string, string>` field. Start with [`map`].
pub struct MapRule<'a> {
    b: Base<'a>,
    new: &'a HashMap<String, String>,
}

/// Starts validating a string map.
pub fn map<'a>(
    op: &'a Operation,
    path: &Path,
    new: &'a HashMap<String, String>,
    old: Option<&'a HashMap<String, String>>,
) -> MapRule<'a> {
    MapRule {
        b: Base::new(op, path, old == Some(new)),
        new,
    }
}

impl<'a> MapRule<'a> {
    fn sorted(&self) -> Vec<(&'a String, &'a String)> {
        let mut v: Vec<_> = self.new.iter().collect();
        v.sort();
        v
    }

    /// An empty map skips the remaining rules.
    pub fn optional(mut self) -> Self {
        if self.new.is_empty() {
            self.b.stop = true;
        }
        self
    }

    /// At least `min` entries.
    pub fn min_entries(mut self, min: usize) -> Self {
        if self.b.live() && self.new.len() < min {
            let e =
                FieldError::too_few(&self.b.path, self.new.len(), min).with_origin("minProperties");
            self.b.fail(e, true);
        }
        self
    }

    /// At most `max` entries.
    pub fn max_entries(mut self, max: usize) -> Self {
        if self.b.armed() && self.new.len() > max {
            let e = FieldError::too_many(&self.b.path, self.new.len(), max)
                .with_origin("maxProperties");
            self.b.fail(e, true);
        }
        self
    }

    /// Every key must match `f`. Errors are reported at the map's path.
    pub fn each_key(mut self, f: Format) -> Self {
        if self.b.live() {
            for (k, _) in self.sorted() {
                self.b.errs.extend(f.validate(&self.b.path, k));
            }
        }
        self
    }

    /// Every value must match `f`. Errors are reported at the entry's path.
    pub fn each_value(mut self, f: Format) -> Self {
        if self.b.live() {
            for (k, v) in self.sorted() {
                self.b.errs.extend(f.validate(&self.b.path.key(k), v));
            }
        }
        self
    }

    /// Runs a custom check over the whole map.
    pub fn custom(
        mut self,
        check: impl FnOnce(&Path, &'a HashMap<String, String>) -> ErrorList,
    ) -> Self {
        if self.b.live() {
            self.b.errs.extend(check(&self.b.path, self.new));
        }
        self
    }

    /// The collected errors.
    pub fn finish(self) -> ErrorList {
        self.b.errs
    }
}

// ---------------------------------------------------------------- unions

/// Requires exactly one of a message's members to be set. `members` pairs
/// each member name with whether it is set now; `old` gives the old set
/// flags, and an update that changes none of them is not re-checked.
pub fn union(
    op: &Operation,
    path: &Path,
    members: &[(&str, bool)],
    old: Option<&[bool]>,
) -> ErrorList {
    if op.is_update() {
        if let Some(old) = old {
            if members.iter().zip(old).all(|((_, n), o)| n == o) {
                return ErrorList::new();
            }
        }
    }
    let all: Vec<String> = members.iter().map(|(n, _)| format!("`{n}`")).collect();
    let set: Vec<&str> = members
        .iter()
        .filter(|(_, s)| *s)
        .map(|(n, _)| *n)
        .collect();
    match set.len() {
        1 => ErrorList::new(),
        0 => FieldError::invalid(path, "", format!("must specify one of: {}", all.join(", ")))
            .with_origin("union")
            .into(),
        _ => FieldError::invalid(
            path,
            format!("{{{}}}", set.join(", ")),
            format!("must specify exactly one of: {}", all.join(", ")),
        )
        .with_origin("union")
        .into(),
    }
}

// ---------------------------------------------------------------- helpers

/// Validates an `ObjectRef`-shaped `(hive, name)` pair, where `hive` is
/// required, forbidden or optional depending on the referenced kind.
pub fn hived_name(
    op: &Operation,
    path: &Path,
    hive: &str,
    name: &str,
    hive_rule: HiveRule,
) -> ErrorList {
    let mut errs = ErrorList::new();
    let hp = path.child("hive");
    errs.extend(match hive_rule {
        HiveRule::Required => string(op, &hp, hive, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
        HiveRule::Forbidden => string(op, &hp, hive, None).forbidden().finish(),
        HiveRule::Optional => string(op, &hp, hive, None)
            .optional()
            .format(Format::DnsLabel)
            .finish(),
    });
    errs.extend(
        string(op, &path.child("name"), name, None)
            .required()
            .format(Format::DnsLabel)
            .finish(),
    );
    errs
}

/// How a reference treats its `hive` part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HiveRule {
    /// The referenced kind is hive-scoped.
    Required,
    /// The referenced kind is global.
    Forbidden,
    /// Either.
    Optional,
}

/// Runs `f` and collects its errors only when `cond` holds.
pub fn when(cond: bool, f: impl FnOnce() -> ErrorList) -> ErrorList {
    if cond {
        f()
    } else {
        ErrorList::new()
    }
}

/// Concatenates several error lists.
pub fn all<I: IntoIterator<Item = ErrorList>>(lists: I) -> ErrorList {
    let mut out = ErrorList::new();
    for l in lists {
        out.extend(l);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::field::ErrorKind;

    fn kinds(e: &ErrorList) -> Vec<(ErrorKind, &str, Option<&str>)> {
        e.iter()
            .map(|e| (e.kind, e.path.as_str(), e.origin))
            .collect()
    }

    #[test]
    fn string_rules_short_circuit() {
        let op = Operation::create();
        let p = Path::from("f");
        assert_eq!(
            kinds(
                &string(&op, &p, "", None)
                    .required()
                    .format(Format::DnsLabel)
                    .finish()
            ),
            vec![(ErrorKind::Required, "f", None)]
        );
        assert!(string(&op, &p, "", None)
            .optional()
            .format(Format::DnsLabel)
            .finish()
            .is_empty());
        let long = "a".repeat(10);
        assert_eq!(
            kinds(
                &string(&op, &p, &long, None)
                    .required()
                    .max_len(5)
                    .format(Format::Uuid)
                    .finish()
            ),
            vec![
                (ErrorKind::TooLong, "f", Some("maxLength")),
                (ErrorKind::Invalid, "f", Some("format=uuid"))
            ]
        );
        assert_eq!(
            kinds(&string(&op, &p, "x", None).forbidden().finish()),
            vec![(ErrorKind::Forbidden, "f", None)]
        );
    }

    #[test]
    fn update_rules_and_ratcheting() {
        let up = Operation::update();
        let p = Path::from("f");
        // Unchanged values are not revalidated, even when invalid.
        assert!(string(&up, &p, "BAD", Some("BAD"))
            .required()
            .format(Format::DnsLabel)
            .finish()
            .is_empty());
        assert_eq!(
            kinds(&string(&up, &p, "b", Some("a")).immutable().finish()),
            vec![(ErrorKind::Invalid, "f", Some("immutable"))]
        );
        assert!(string(&up, &p, "b", Some(""))
            .no_modify()
            .finish()
            .is_empty());
        assert_eq!(
            kinds(&string(&up, &p, "b", Some("a")).no_modify().finish())[0].2,
            Some("update")
        );
        assert_eq!(
            kinds(&string(&up, &p, "", Some("a")).no_unset().finish())[0].2,
            Some("update")
        );
        assert_eq!(
            kinds(&int(&up, &p, 1, Some(2)).monotonic().finish())[0].2,
            Some("monotonic")
        );
        assert!(int(&Operation::create(), &p, 1, Some(2))
            .monotonic()
            .finish()
            .is_empty());
    }

    #[test]
    fn list_and_map_rules() {
        let op = Operation::create();
        let p = Path::from("l");
        let v = vec!["a".to_string(), "b".into(), "a".into()];
        assert_eq!(
            kinds(
                &list(&op, &p, &v, None)
                    .max_items(2)
                    .unique_values()
                    .finish()
            ),
            vec![(ErrorKind::TooMany, "l", Some("maxItems"))]
        );
        assert_eq!(
            kinds(&list(&op, &p, &v, None).unique_values().finish()),
            vec![(ErrorKind::Duplicate, "l[2]", None)]
        );
        let empty: Vec<String> = vec![];
        assert_eq!(
            kinds(&list(&op, &p, &empty, None).required().finish()),
            vec![(ErrorKind::Required, "l", None)]
        );
        let m: HashMap<String, String> = [
            ("bad key!".to_string(), "x".to_string()),
            ("k".into(), "not valid!".into()),
        ]
        .into();
        assert_eq!(
            kinds(
                &map(&op, &p, &m, None)
                    .each_key(Format::LabelKey)
                    .each_value(Format::LabelValue)
                    .finish()
            ),
            vec![
                (ErrorKind::Invalid, "l", Some("format=label-key")),
                (ErrorKind::Invalid, "l[k]", Some("format=label-value"))
            ]
        );
    }

    #[test]
    fn unions() {
        let op = Operation::create();
        let p = Path::from("u");
        assert!(union(&op, &p, &[("a", true), ("b", false)], None).is_empty());
        assert!(union(&op, &p, &[("a", false), ("b", false)], None)
            .iter()
            .all(|e| e.detail.contains("one of")));
        assert!(union(&op, &p, &[("a", true), ("b", true)], None)
            .iter()
            .all(|e| e.detail.contains("exactly one of")));
        assert!(union(&Operation::update(), &p, &[("a", false)], Some(&[false])).is_empty());
    }

    #[test]
    fn gates() {
        let op = Operation::create().with_gates(PreviewGates::all());
        assert!(op.gate_enabled(Gate::ExternalVolumes));
        let op = op.with_opt_out(true);
        assert!(
            !op.gate_enabled(Gate::ExternalVolumes)
                && op.server_gate_enabled(Gate::ExternalVolumes)
        );
        let x = 1;
        let e = message(&op, &Path::from("m"), Some(&x), None)
            .optional()
            .needs_gate(Gate::ExternalVolumes)
            .finish();
        assert_eq!(
            kinds(&e),
            vec![(ErrorKind::Forbidden, "m", Some("ifDisabled"))]
        );
    }
}
