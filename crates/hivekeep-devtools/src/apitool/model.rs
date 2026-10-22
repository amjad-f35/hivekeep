//! Data model of one proto3 file's documented surface, built by compiling its
//! source text with comments retained.
//!
//! Full names are plain dotted strings (for example `hivekeep.api.v1.Actor`).

use std::collections::{HashMap, HashSet};
use std::fmt;

use prost_reflect::{
    DescriptorPool, DynamicMessage, EnumDescriptor, FieldDescriptor, FileDescriptor,
    MessageDescriptor, ServiceDescriptor, Value,
};
use protox::file::{ChainFileResolver, File, FileResolver, GoogleFileResolver};

/// Internal label for the compiled source. Only one file is ever compiled, so
/// the name matters only for error messages.
const SOURCE_FILE_NAME: &str = "source.proto";

/// The resource-parent value that stands for the whole Hivekeep installation
/// rather than another resource.
pub const GLOBAL_PARENT: &str = "Global";

/// Full name of the message option carrying a [`ResourceAnnotation`].
pub const RESOURCE_EXTENSION: &str = "hivekeep.api.v1.resource";
/// Full name of the method option naming the method's resource.
pub const METHOD_EXTENSION: &str = "hivekeep.api.v1.method";

/// Errors building or interpreting the model.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// The proto source did not compile.
    #[error("while compiling proto source: {0}")]
    Compile(String),
    /// The model is malformed in a way no rule can check past.
    #[error("{0}")]
    Invalid(String),
}

/// The last component of a dotted full name: `Actor` for `hivekeep.api.v1.Actor`.
pub fn short_name(full_name: &str) -> &str {
    full_name.rsplit('.').next().unwrap_or(full_name)
}

/// Appends `name` to `full_name` as a new dotted component.
pub fn append_name(full_name: &str, name: &str) -> String {
    if full_name.is_empty() {
        name.to_string()
    } else {
        format!("{full_name}.{name}")
    }
}

/// A field's kind, named the way `.proto` source spells scalar types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Kind {
    /// Not set; used for fields built by hand without a kind.
    #[default]
    Unset,
    Double,
    Float,
    Int64,
    Uint64,
    Int32,
    Fixed64,
    Fixed32,
    Bool,
    String,
    Group,
    Message,
    Bytes,
    Uint32,
    Enum,
    Sfixed32,
    Sfixed64,
    Sint32,
    Sint64,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Kind::Unset => "<unset>",
            Kind::Double => "double",
            Kind::Float => "float",
            Kind::Int64 => "int64",
            Kind::Uint64 => "uint64",
            Kind::Int32 => "int32",
            Kind::Fixed64 => "fixed64",
            Kind::Fixed32 => "fixed32",
            Kind::Bool => "bool",
            Kind::String => "string",
            Kind::Group => "group",
            Kind::Message => "message",
            Kind::Bytes => "bytes",
            Kind::Uint32 => "uint32",
            Kind::Enum => "enum",
            Kind::Sfixed32 => "sfixed32",
            Kind::Sfixed64 => "sfixed64",
            Kind::Sint32 => "sint32",
            Kind::Sint64 => "sint64",
        };
        f.write_str(s)
    }
}

/// Whether a top-level resource belongs to a Hive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Scope {
    #[default]
    Unspecified,
    Hive,
    Global,
}

impl Scope {
    fn from_number(n: i32) -> Self {
        match n {
            1 => Scope::Hive,
            2 => Scope::Global,
            _ => Scope::Unspecified,
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Scope::Unspecified => "SCOPE_UNSPECIFIED",
            Scope::Hive => "SCOPE_HIVE",
            Scope::Global => "SCOPE_GLOBAL",
        })
    }
}

/// The whole documented surface of one proto file.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Api {
    /// Every RPC service declared in the file.
    pub services: Vec<Service>,
    /// Every message, top-level and nested, flattened.
    pub messages: Vec<Message>,
    /// Every enum, top-level and nested, flattened.
    pub enums: Vec<Enum>,
}

impl Api {
    /// Indexes the messages by full name.
    pub fn messages_by_full_name(&self) -> HashMap<&str, &Message> {
        self.messages
            .iter()
            .map(|m| (m.full_name.as_str(), m))
            .collect()
    }
}

/// One RPC service.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Service {
    pub full_name: String,
    pub name: String,
    /// Leading doc comment, trimmed.
    pub comment: String,
    /// Methods in declaration order.
    pub methods: Vec<Method>,
}

/// One RPC declared on a service.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Method {
    pub name: String,
    pub comment: String,
    /// Full name of the request message.
    pub input_name: String,
    /// Full name of the response message.
    pub output_name: String,
    /// The resource the method's annotation names, or empty when it has none.
    pub resource: String,
    pub service_full_name: String,
    pub service_name: String,
}

/// One message, top-level or nested.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Message {
    pub full_name: String,
    /// Short name, with enclosing message names dotted in front for nested types.
    pub name: String,
    /// Enclosing message's full name, or empty for a top-level message.
    pub parent_full_name: String,
    pub comment: String,
    /// Fields in declaration order.
    pub fields: Vec<Field>,
    /// The message's resource annotation, if it is an API resource.
    pub resource_annotation: Option<ResourceAnnotation>,
}

impl Message {
    /// Whether this message is an API resource.
    pub fn is_resource(&self) -> bool {
        self.resource_annotation.is_some()
    }

    fn annotation(&self) -> &ResourceAnnotation {
        self.resource_annotation
            .as_ref()
            .expect("resource message carries an annotation")
    }
}

/// A resource message's annotation.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ResourceAnnotation {
    /// Resource types this is a subresource of: another resource's message name,
    /// or [`GLOBAL_PARENT`]. Empty for a top-level resource.
    pub parents: Vec<String>,
    /// Each parent has at most one instance of this resource.
    pub singleton: bool,
    /// Set only on a top-level resource; a subresource has its parent's scope.
    pub scope: Scope,
    pub plural: String,
}

/// One field declared on a message.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Field {
    pub name: String,
    pub number: i32,
    pub comment: String,
    /// A `repeated` field (not a map).
    pub repeated: bool,
    /// Ready-to-render type, e.g. `string`, `repeated Volume`, `map<string, string>`.
    pub type_display: String,
    /// A map field is [`Kind::Message`]; see `map_value_kind` for its value.
    pub kind: Kind,
    /// Set only for a singular or repeated message/enum field.
    pub type_full_name: String,
    /// The type is declared outside this proto file (e.g. a well-known type).
    pub type_is_external: bool,
    pub map_value_kind: Kind,
    pub map_value_full_name: String,
    /// Set only for a member of a real, explicit `oneof`.
    pub oneof_name: String,
    /// A proto3 `optional` field (a synthetic oneof, not a real one).
    pub proto3_optional: bool,
}

/// One enum, top-level or nested.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Enum {
    pub full_name: String,
    pub name: String,
    pub parent_full_name: String,
    pub comment: String,
    pub values: Vec<EnumValue>,
}

impl Enum {
    /// The value numbered `number`, if any.
    pub fn value_by_number(&self, number: i32) -> Option<&EnumValue> {
        self.values.iter().find(|v| v.number == number)
    }
}

/// One value of an enum.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct EnumValue {
    pub name: String,
    pub number: i32,
    pub comment: String,
}

/// Serves the single in-memory source file.
struct SourceResolver {
    source: String,
}

impl FileResolver for SourceResolver {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        if name == SOURCE_FILE_NAME {
            File::from_source(name, &self.source)
        } else {
            Err(protox::Error::file_not_found(name))
        }
    }
}

/// Compiles `source` (one self-contained proto3 file; well-known imports are
/// available) with source info retained, and walks it into an [`Api`].
pub fn build(source: &str) -> Result<Api, ModelError> {
    let mut resolver = ChainFileResolver::new();
    resolver.add(SourceResolver {
        source: source.to_string(),
    });
    resolver.add(GoogleFileResolver::new());
    let mut compiler = protox::Compiler::with_file_resolver(resolver);
    compiler.include_source_info(true);
    compiler
        .open_file(SOURCE_FILE_NAME)
        .map_err(|e| ModelError::Compile(e.to_string()))?;
    let pool = compiler.descriptor_pool();
    let file = pool
        .get_file_by_name(SOURCE_FILE_NAME)
        .ok_or_else(|| ModelError::Compile("compiled file missing from pool".into()))?;
    Ok(Builder::new(&pool, file).build())
}

struct Builder {
    file: FileDescriptor,
    package: String,
    comments: HashMap<Vec<i32>, String>,
    resource_ext: Option<prost_reflect::ExtensionDescriptor>,
    method_ext: Option<prost_reflect::ExtensionDescriptor>,
}

impl Builder {
    fn new(pool: &DescriptorPool, file: FileDescriptor) -> Self {
        let comments = file
            .file_descriptor_proto()
            .source_code_info
            .as_ref()
            .map(|info| {
                info.location
                    .iter()
                    .map(|l| (l.path.clone(), l.leading_comments().trim().to_string()))
                    .collect()
            })
            .unwrap_or_default();
        Builder {
            package: file.package_name().to_string(),
            file,
            comments,
            resource_ext: pool.get_extension_by_name(RESOURCE_EXTENSION),
            method_ext: pool.get_extension_by_name(METHOD_EXTENSION),
        }
    }

    fn comment(&self, path: &[i32]) -> String {
        self.comments.get(path).cloned().unwrap_or_default()
    }

    /// Strips the package prefix: `Actor.Status` for `hivekeep.api.v1.Actor.Status`.
    fn short(&self, full: &str) -> String {
        full.strip_prefix(&format!("{}.", self.package))
            .unwrap_or(full)
            .to_string()
    }

    fn build(&self) -> Api {
        let mut api = Api::default();
        for svc in self.file.services() {
            api.services.push(self.service(&svc));
        }
        for md in self.file.messages() {
            self.collect_message(&md, &mut api);
        }
        for ed in self.file.enums() {
            api.enums.push(self.enumeration(&ed));
        }
        api
    }

    fn collect_message(&self, md: &MessageDescriptor, api: &mut Api) {
        if md.is_map_entry() {
            return; // synthetic per-map-field type, never a documented message.
        }
        api.messages.push(self.message(md));
        for child in md.child_messages() {
            self.collect_message(&child, api);
        }
        for ed in md.child_enums() {
            api.enums.push(self.enumeration(&ed));
        }
    }

    fn service(&self, sd: &ServiceDescriptor) -> Service {
        let full_name = sd.full_name().to_string();
        let name = sd.name().to_string();
        let methods = sd
            .methods()
            .map(|md| Method {
                name: md.name().to_string(),
                comment: self.comment(md.path()),
                input_name: md.input().full_name().to_string(),
                output_name: md.output().full_name().to_string(),
                resource: self.method_resource(&md.options()),
                service_full_name: full_name.clone(),
                service_name: name.clone(),
            })
            .collect();
        Service {
            comment: self.comment(sd.path()),
            full_name,
            name,
            methods,
        }
    }

    fn method_resource(&self, opts: &DynamicMessage) -> String {
        let Some(ext) = &self.method_ext else {
            return String::new();
        };
        if !opts.has_extension(ext) {
            return String::new();
        }
        match opts.get_extension(ext).as_ref() {
            Value::Message(m) => string_field(m, "resource"),
            _ => String::new(),
        }
    }

    fn resource_annotation(&self, opts: &DynamicMessage) -> Option<ResourceAnnotation> {
        let ext = self.resource_ext.as_ref()?;
        if !opts.has_extension(ext) {
            return None;
        }
        let value = opts.get_extension(ext);
        let Value::Message(m) = value.as_ref() else {
            return None;
        };
        let parents = match m.get_field_by_name("parents").as_deref() {
            Some(Value::List(items)) => items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect(),
            _ => Vec::new(),
        };
        let singleton = matches!(
            m.get_field_by_name("singleton").as_deref(),
            Some(Value::Bool(true))
        );
        let scope = match m.get_field_by_name("scope").as_deref() {
            Some(Value::EnumNumber(n)) => Scope::from_number(*n),
            _ => Scope::Unspecified,
        };
        Some(ResourceAnnotation {
            parents,
            singleton,
            scope,
            plural: string_field(m, "plural"),
        })
    }

    fn message(&self, md: &MessageDescriptor) -> Message {
        Message {
            full_name: md.full_name().to_string(),
            name: self.short(md.full_name()),
            parent_full_name: md
                .parent_message()
                .map(|p| p.full_name().to_string())
                .unwrap_or_default(),
            comment: self.comment(md.path()),
            fields: md.fields().map(|f| self.field(&f)).collect(),
            resource_annotation: self.resource_annotation(&md.options()),
        }
    }

    fn field(&self, field: &FieldDescriptor) -> Field {
        let mut f = Field {
            name: field.name().to_string(),
            number: field.number() as i32,
            comment: self.comment(field.path()),
            repeated: field.is_list(),
            kind: kind_of(field),
            ..Field::default()
        };
        let prefix = if field.is_list() { "repeated " } else { "" };
        if field.is_map() {
            let entry = field
                .kind()
                .as_message()
                .cloned()
                .expect("map field has an entry message");
            let key = entry.map_entry_key_field();
            let value = entry.map_entry_value_field();
            f.type_display = format!(
                "map<{}, {}>",
                self.scalar_or_type_name(&key),
                self.scalar_or_type_name(&value)
            );
            f.map_value_kind = kind_of(&value);
            match value.kind() {
                prost_reflect::Kind::Message(m) => f.map_value_full_name = m.full_name().into(),
                prost_reflect::Kind::Enum(e) => f.map_value_full_name = e.full_name().into(),
                _ => {}
            }
        } else {
            match field.kind() {
                prost_reflect::Kind::Message(m) => {
                    f.type_full_name = m.full_name().to_string();
                    f.type_is_external = m.parent_file().name() != self.file.name();
                    f.type_display = format!("{prefix}{}", self.short(m.full_name()));
                }
                prost_reflect::Kind::Enum(e) => {
                    f.type_full_name = e.full_name().to_string();
                    f.type_is_external = e.parent_file().name() != self.file.name();
                    f.type_display = format!("{prefix}{}", self.short(e.full_name()));
                }
                _ => f.type_display = format!("{prefix}{}", f.kind),
            }
        }
        if let Some(oneof) = field.containing_oneof() {
            if oneof.is_synthetic() {
                f.proto3_optional = true;
            } else {
                f.oneof_name = oneof.name().to_string();
            }
        }
        f
    }

    fn scalar_or_type_name(&self, field: &FieldDescriptor) -> String {
        match field.kind() {
            prost_reflect::Kind::Message(m) => self.short(m.full_name()),
            prost_reflect::Kind::Enum(e) => self.short(e.full_name()),
            _ => kind_of(field).to_string(),
        }
    }

    fn enumeration(&self, ed: &EnumDescriptor) -> Enum {
        Enum {
            full_name: ed.full_name().to_string(),
            name: self.short(ed.full_name()),
            parent_full_name: ed
                .parent_message()
                .map(|p| p.full_name().to_string())
                .unwrap_or_default(),
            comment: self.comment(ed.path()),
            values: ed
                .values()
                .map(|v| EnumValue {
                    name: v.name().to_string(),
                    number: v.number(),
                    comment: self.comment(v.path()),
                })
                .collect(),
        }
    }
}

fn string_field(m: &DynamicMessage, name: &str) -> String {
    m.get_field_by_name(name)
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn kind_of(field: &FieldDescriptor) -> Kind {
    use prost_reflect::Kind as K;
    if field.is_group() {
        return Kind::Group;
    }
    match field.kind() {
        K::Double => Kind::Double,
        K::Float => Kind::Float,
        K::Int32 => Kind::Int32,
        K::Int64 => Kind::Int64,
        K::Uint32 => Kind::Uint32,
        K::Uint64 => Kind::Uint64,
        K::Sint32 => Kind::Sint32,
        K::Sint64 => Kind::Sint64,
        K::Fixed32 => Kind::Fixed32,
        K::Fixed64 => Kind::Fixed64,
        K::Sfixed32 => Kind::Sfixed32,
        K::Sfixed64 => Kind::Sfixed64,
        K::Bool => Kind::Bool,
        K::String => Kind::String,
        K::Bytes => Kind::Bytes,
        K::Message(_) => Kind::Message,
        K::Enum(_) => Kind::Enum,
    }
}

/// Returns a copy of `api` holding only the service named `service_name`, plus
/// every message and enum reachable from its methods' input/output types
/// (through fields, map values, and resource parents). Empty if there is no
/// such service.
pub fn scope_to_service(api: &Api, service_name: &str) -> Api {
    let Some(svc) = api.services.iter().find(|s| s.name == service_name) else {
        return Api::default();
    };
    let by_name: HashMap<&str, &Message> = api.messages_by_full_name();
    let resources_by_name: HashMap<&str, &str> = api
        .messages
        .iter()
        .filter(|m| m.is_resource())
        .map(|m| (m.name.as_str(), m.full_name.as_str()))
        .collect();

    let mut messages = HashSet::new();
    let mut enums = HashSet::new();
    let mut stack: Vec<String> = svc
        .methods
        .iter()
        .flat_map(|m| [m.input_name.clone(), m.output_name.clone()])
        .collect();
    while let Some(name) = stack.pop() {
        if messages.contains(&name) {
            continue;
        }
        let Some(msg) = by_name.get(name.as_str()) else {
            continue;
        };
        messages.insert(name);
        if let Some(a) = &msg.resource_annotation {
            for parent in &a.parents {
                if let Some(full) = resources_by_name.get(parent.as_str()) {
                    stack.push(full.to_string());
                }
            }
        }
        for f in &msg.fields {
            for (kind, full) in [
                (f.kind, &f.type_full_name),
                (f.map_value_kind, &f.map_value_full_name),
            ] {
                match kind {
                    Kind::Message => stack.push(full.clone()),
                    Kind::Enum => {
                        enums.insert(full.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    Api {
        services: vec![svc.clone()],
        messages: api
            .messages
            .iter()
            .filter(|m| messages.contains(&m.full_name))
            .cloned()
            .collect(),
        enums: api
            .enums
            .iter()
            .filter(|e| enums.contains(&e.full_name))
            .cloned()
            .collect(),
    }
}

/// A resource message with every method (across services) whose annotation
/// names it, in encounter order.
#[derive(Debug, Clone, PartialEq)]
pub struct Resource {
    pub message: Message,
    pub methods: Vec<Method>,
}

/// Groups `api`'s methods by the resource their annotation names, one group per
/// annotated message in message order.
///
/// Fails if a method has no annotation or names a message that is not a
/// resource, if a resource names a parent that is neither [`GLOBAL_PARENT`] nor
/// another resource, if a resource has no plural, or if a top-level resource has
/// no scope or a subresource has one.
pub fn resources(api: &Api) -> Result<Vec<Resource>, ModelError> {
    let mut out: Vec<Resource> = api
        .messages
        .iter()
        .filter(|m| m.is_resource())
        .map(|m| Resource {
            message: m.clone(),
            methods: Vec::new(),
        })
        .collect();
    let index: HashMap<String, usize> = out
        .iter()
        .enumerate()
        .map(|(i, r)| (r.message.name.clone(), i))
        .collect();

    for r in &out {
        validate_annotation(&r.message, &index)?;
    }

    for svc in &api.services {
        for method in &svc.methods {
            if method.resource.is_empty() {
                return Err(ModelError::Invalid(format!(
                    "{}.{}: no resource annotation - add `option ({METHOD_EXTENSION}).resource = \"<Resource>\";`",
                    svc.name, method.name
                )));
            }
            let Some(&i) = index.get(&method.resource) else {
                return Err(ModelError::Invalid(format!(
                    "{}.{}: annotated resource {:?} is not a resource in this API - annotate its message with ({RESOURCE_EXTENSION})",
                    svc.name, method.name, method.resource
                )));
            };
            out[i].methods.push(method.clone());
        }
    }
    Ok(out)
}

fn validate_annotation(
    resource: &Message,
    index: &HashMap<String, usize>,
) -> Result<(), ModelError> {
    let a = resource.annotation();
    for parent in &a.parents {
        if !index.contains_key(parent) && parent != GLOBAL_PARENT {
            return Err(ModelError::Invalid(format!(
                "{}: parent {parent:?} is neither {GLOBAL_PARENT:?} nor a resource in this API",
                resource.full_name
            )));
        }
    }
    if a.plural.is_empty() {
        return Err(ModelError::Invalid(format!(
            "{}: resource has no plural - set `plural` in its ({RESOURCE_EXTENSION}) annotation",
            resource.full_name
        )));
    }
    if a.parents.is_empty() && a.scope == Scope::Unspecified {
        return Err(ModelError::Invalid(format!(
            "{}: top-level resource has no scope - set `scope` in its ({RESOURCE_EXTENSION}) annotation",
            resource.full_name
        )));
    }
    if !a.parents.is_empty() && a.scope != Scope::Unspecified {
        return Err(ModelError::Invalid(format!(
            "{}: subresource has scope {} - a subresource has its parent's scope, so leave `scope` unset",
            resource.full_name, a.scope
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
