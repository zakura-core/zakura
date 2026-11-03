//! Renders the generated method table as an OpenRPC document.

use serde::Serialize;
use serde_json::{json, Map, Value};

use super::{RpcSurface, METHODS};

mod schemas;

const OPENRPC_VERSION: &str = "1.3.2";
const RESULT_DESCRIPTION: &str = "An OpenRPC document.";
const RESULT_SCHEMA: &str =
    "https://raw.githubusercontent.com/open-rpc/meta-schema/master/schema.json";
const COMPONENTS_PATH: &str = "#/components/schemas/";

/// A type that describes its JSON-RPC wire format as a draft-07 JSON Schema.
///
/// The schema describes what serde accepts or produces for the type, so an
/// implementation must change together with the type's serde attributes.
pub(super) trait RpcSchema {
    /// The `#/components/schemas/` entry shared by every use of this type, or
    /// `None` to write the schema inline at each use.
    const COMPONENT: Option<&'static str> = None;

    /// Returns this type's schema, registering any component schemas it uses.
    fn schema(generator: &mut Generator) -> Value;
}

/// The placeholder result type, described by the OpenRPC meta-schema.
struct ResultSchema;

impl RpcSchema for ResultSchema {
    const COMPONENT: Option<&'static str> = Some("OpenRPC Schema");

    fn schema(_: &mut Generator) -> Value {
        json!({ "$ref": RESULT_SCHEMA })
    }
}

/// Callbacks and documentation emitted by the method-table generator.
pub(crate) struct RpcMethod {
    pub(super) description: &'static str,
    pub(super) params: fn(&mut Generator) -> Vec<ContentDescriptor>,
    pub(super) result: fn(&mut Generator) -> ContentDescriptor,
    pub(super) deprecated: bool,
}

/// Parameter schemas shared across one listener's OpenRPC document.
#[derive(Default)]
pub(super) struct Generator {
    components: Map<String, Value>,
}

impl Generator {
    /// Returns a reference to `T`'s component schema, or `T`'s inline schema
    /// if it is not a component.
    pub(super) fn subschema_for<T: RpcSchema>(&mut self) -> Value {
        let Some(name) = T::COMPONENT else {
            return T::schema(self);
        };
        if !self.components.contains_key(name) {
            // Reserve the name first, so a self-referential schema terminates.
            self.components.insert(name.to_owned(), Value::Null);
            let schema = T::schema(self);
            self.components.insert(name.to_owned(), schema);
        }
        json!({ "$ref": format!("{COMPONENTS_PATH}{}", name.replace(' ', "%20")) })
    }

    pub(super) fn param<T: RpcSchema>(
        &mut self,
        name: &'static str,
        description: &'static str,
        required: bool,
    ) -> ContentDescriptor {
        ContentDescriptor {
            name,
            summary: summary(description),
            description,
            required,
            schema: self.subschema_for::<T>(),
            deprecated: false,
        }
    }

    pub(super) fn result(&mut self, name: &'static str) -> ContentDescriptor {
        // The generated table describes untyped results with the OpenRPC
        // meta-schema.
        ContentDescriptor {
            name,
            summary: RESULT_DESCRIPTION,
            description: RESULT_DESCRIPTION,
            required: false,
            schema: self.subschema_for::<ResultSchema>(),
            deprecated: false,
        }
    }

    /// Describes a result with the schema of its declared Rust result type.
    pub(super) fn typed_result<T: RpcSchema>(
        &mut self,
        name: &'static str,
        description: &'static str,
    ) -> ContentDescriptor {
        ContentDescriptor {
            name,
            summary: summary(description),
            description,
            required: false,
            schema: self.subschema_for::<T>(),
            deprecated: false,
        }
    }
}

#[derive(Serialize)]
/// The serialized description of one parameter or result.
pub(super) struct ContentDescriptor {
    name: &'static str,
    summary: &'static str,
    description: &'static str,
    #[serde(skip_serializing_if = "is_false")]
    required: bool,
    schema: Value,
    #[serde(skip_serializing_if = "is_false")]
    deprecated: bool,
}

#[derive(Serialize)]
struct Method {
    name: &'static str,
    summary: &'static str,
    description: &'static str,
    params: Vec<ContentDescriptor>,
    result: ContentDescriptor,
    #[serde(skip_serializing_if = "is_false")]
    deprecated: bool,
}

pub(super) fn render(surface: RpcSurface) -> Value {
    let mut generator = Generator::default();
    let methods: Vec<_> = METHODS
        .iter()
        .filter(|(name, _)| surface.exposes(name))
        .map(|(name, method)| {
            let description = method.description.trim();
            Method {
                name,
                summary: summary(description),
                description,
                params: (method.params)(&mut generator),
                result: (method.result)(&mut generator),
                deprecated: method.deprecated,
            }
        })
        .collect();

    serde_json::json!({
        "openrpc": OPENRPC_VERSION,
        "info": {
            "title": env!("CARGO_PKG_NAME"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "version": env!("CARGO_PKG_VERSION"),
        },
        "methods": methods,
        "components": { "schemas": generator.components },
    })
}

fn summary(description: &str) -> &str {
    description
        .split_once('\n')
        .map_or(description, |(line, _)| line)
}

fn is_false(value: &bool) -> bool {
    !value
}

#[cfg(test)]
mod tests {
    use super::{render, RpcSurface};

    #[test]
    fn documents_match_the_previous_renderer_for_both_surfaces() {
        for (surface, fixture) in [
            (RpcSurface::Full, include_str!("tests/openrpc.json")),
            (
                RpcSurface::Restricted,
                include_str!("tests/openrpc-restricted.json"),
            ),
        ] {
            let mut expected: serde_json::Value = serde_json::from_str(fixture).unwrap();
            expected["info"]["version"] = env!("CARGO_PKG_VERSION").into();
            let mut actual = render(surface);
            // The slice uses declaration order instead of PHF iteration order.
            for document in [&mut actual, &mut expected] {
                document["methods"]
                    .as_array_mut()
                    .unwrap()
                    .sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
            }
            assert_eq!(actual, expected);
        }
    }
}
