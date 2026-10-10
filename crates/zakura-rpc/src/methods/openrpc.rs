//! Renders the generated method table as an OpenRPC document.

use std::borrow::Cow;

use schemars::{generate::SchemaSettings, JsonSchema, Schema, SchemaGenerator};
use serde::Serialize;
use serde_json::Value;

use super::{RpcSurface, METHODS};

const OPENRPC_VERSION: &str = "1.3.2";
const RESULT_DESCRIPTION: &str = "An OpenRPC document.";
const RESULT_SCHEMA: &str =
    "https://raw.githubusercontent.com/open-rpc/meta-schema/master/schema.json";

struct ResultSchema;

impl JsonSchema for ResultSchema {
    fn schema_name() -> Cow<'static, str> {
        "OpenRPC Schema".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        Schema::new_ref(RESULT_SCHEMA.into())
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
pub(super) struct Generator(SchemaGenerator);

impl Generator {
    pub(super) fn param<T: JsonSchema>(
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
            schema: self.0.subschema_for::<T>(),
            deprecated: false,
        }
    }

    pub(super) fn result(&mut self, name: &'static str) -> ContentDescriptor {
        // The existing generated table describes every result with the OpenRPC
        // meta-schema. Keep that behavior separate from this dependency change.
        ContentDescriptor {
            name,
            summary: RESULT_DESCRIPTION,
            description: RESULT_DESCRIPTION,
            required: false,
            schema: self.0.subschema_for::<ResultSchema>(),
            deprecated: false,
        }
    }

    /// Describes a result with the schema of its declared Rust result type.
    pub(super) fn typed_result<T: JsonSchema>(
        &mut self,
        name: &'static str,
        description: &'static str,
    ) -> ContentDescriptor {
        ContentDescriptor {
            name,
            summary: summary(description),
            description,
            required: false,
            schema: self.0.subschema_for::<T>(),
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
    schema: Schema,
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
    let mut generator = Generator(
        SchemaSettings::draft07()
            .with(|settings| settings.definitions_path = "#/components/schemas/".into())
            .into_generator(),
    );
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
        "components": { "schemas": generator.0.take_definitions(false) },
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
            // The fixtures describe the stable API. Check the experimental
            // method separately, while preserving the stable document comparison.
            #[cfg(zcash_unstable = "nutachyon")]
            {
                let methods = actual["methods"].as_array_mut().unwrap();
                let method = methods
                    .iter()
                    .find(|method| method["name"] == "gettachyonblock")
                    .expect("both RPC surfaces expose the public Tachyon data feed");
                assert_eq!(method["params"][0]["name"], "hash_or_height");
                assert_eq!(method["params"][0]["required"], true);
                methods.retain(|method| method["name"] != "gettachyonblock");
            }
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
