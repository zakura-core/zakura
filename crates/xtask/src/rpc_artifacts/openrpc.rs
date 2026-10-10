//! Generates the node's OpenRPC method table without a runtime Rust parser.

use std::{fmt::Write, fs, path::Path};

use quote::ToTokens;
use syn::{FnArg, GenericArgument, PathArguments, ReturnType, TraitItem, Type};

use crate::BoxError;

/// Methods whose result schema is generated from their declared `Result<T>` type.
///
/// Every other method keeps the inherited OpenRPC meta-schema result placeholder.
const TYPED_RESULT_METHODS: &[&str] = &["preciousblock"];

pub(super) fn generate(source: &Path, output_dir: &Path) -> Result<(), BoxError> {
    let source = syn::parse_file(&fs::read_to_string(source)?)?;
    let rpc_traits: Vec<_> = source
        .items
        .iter()
        .filter_map(|item| match item {
            syn::Item::Trait(item) if item.attrs.iter().any(|attr| attr.path().is_ident("rpc")) => {
                Some(item)
            }
            _ => None,
        })
        .collect();
    if !rpc_traits.iter().any(|item| item.ident == "Rpc") {
        return Err("RPC methods source must contain the Rpc trait".into());
    }
    let mut output = String::from(
        "/// JSON-RPC methods in declaration order.\n\
         pub(crate) static METHODS: &[(&str, openrpc::RpcMethod)] = &[\n",
    );

    for (rpc, item) in rpc_traits
        .iter()
        .flat_map(|rpc| rpc.items.iter().map(move |item| (rpc, item)))
    {
        let TraitItem::Fn(method) = item else {
            continue;
        };
        let Some(attribute) = method
            .attrs
            .iter()
            .find(|attr| attr.path().is_ident("method"))
        else {
            continue;
        };
        let mut name = None;
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(meta.value()?.parse::<syn::LitStr>()?.value());
                Ok(())
            } else {
                Err(meta.error("unsupported RPC method attribute"))
            }
        })?;
        let name = name.ok_or("RPC method attribute must specify a name")?;
        let mut description = String::new();
        for attr in method
            .attrs
            .iter()
            .filter(|attr| attr.path().is_ident("doc"))
        {
            if let syn::Meta::NameValue(value) = &attr.meta {
                if let syn::Expr::Lit(value) = &value.value {
                    if let syn::Lit::Str(value) = &value.lit {
                        let line = value.value();
                        writeln!(description, "{}", line.strip_prefix(' ').unwrap_or(&line))?;
                    }
                }
            }
        }

        // Discovery must expose exactly the methods compiled into the server.
        for attr in rpc.attrs.iter().filter(|attr| attr.path().is_ident("cfg")) {
            writeln!(output, "{}", attr.to_token_stream())?;
        }
        writeln!(output, "({name:?}, openrpc::RpcMethod {{")?;
        writeln!(output, "    description: {description:?},")?;
        writeln!(output, "    params: |_g| vec![")?;
        for input in &method.sig.inputs {
            let FnArg::Typed(input) = input else { continue };
            let syn::Pat::Ident(parameter) = input.pat.as_ref() else {
                return Err("RPC parameters must have identifier patterns".into());
            };
            let parameter = parameter.ident.to_string();
            let (schema, required) = parameter_type(&input.ty)?;
            let schema = schema.to_token_stream().to_string();
            let upper = parameter.to_uppercase();
            let required = match required {
                Some(required) => required.to_string(),
                None => {
                    let ReturnType::Type(_, result) = &method.sig.output else {
                        return Err("RPC methods must declare their result type".into());
                    };
                    let Type::Path(result) = result.as_ref() else {
                        return Err("RPC result types must be paths".into());
                    };
                    let module = &result
                        .path
                        .segments
                        .first()
                        .ok_or("RPC result path is empty")?
                        .ident;
                    format!("crate::methods::{module}::PARAM_{upper}_REQUIRED")
                }
            };
            writeln!(output, "        _g.param::<{schema}>({parameter:?}, crate::methods::PARAM_{upper}_DESC, {required}),")?;
        }
        writeln!(output, "    ],")?;
        if TYPED_RESULT_METHODS.contains(&name.as_str()) {
            let result = result_type(&method.sig.output)?
                .to_token_stream()
                .to_string();
            let upper = name.to_uppercase();
            writeln!(
                output,
                "    result: |g| g.typed_result::<{result}>({:?}, crate::methods::RESULT_{upper}_DESC),",
                format!("{name}_result")
            )?;
        } else {
            writeln!(
                output,
                "    result: |g| g.result({:?}),",
                format!("{name}_result")
            )?;
        }
        let deprecated = method
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("deprecated"));
        writeln!(output, "    deprecated: {deprecated},")?;
        writeln!(output, "}}),")?;
    }

    for name in TYPED_RESULT_METHODS {
        if !output.contains(&format!("({name:?}, openrpc::RpcMethod {{")) {
            return Err(format!("typed-result RPC method {name} is not in the Rpc trait").into());
        }
    }

    output.push_str("];");
    fs::write(output_dir.join("rpc_openrpc.rs"), output)?;
    Ok(())
}

/// Returns `T` from a method's declared `Result<T>` return type.
fn result_type(output: &ReturnType) -> Result<&Type, BoxError> {
    let ReturnType::Type(_, result) = output else {
        return Err("RPC methods must declare their result type".into());
    };
    let Type::Path(result) = result.as_ref() else {
        return Err("RPC result types must be paths".into());
    };
    let segment = result
        .path
        .segments
        .last()
        .ok_or("RPC result path is empty")?;
    if segment.ident != "Result" {
        return Err("typed RPC results must be declared as `Result<T>`".into());
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err("typed RPC results must have a type argument".into());
    };
    match args.args.first() {
        Some(GenericArgument::Type(inner)) => Ok(inner),
        _ => Err("typed RPC results must have a type argument".into()),
    }
}

fn parameter_type(ty: &Type) -> Result<(&Type, Option<bool>), BoxError> {
    if let Type::Path(path) = ty {
        if path.path.leading_colon.is_none() && path.path.segments.len() == 1 {
            let segment = &path.path.segments[0];
            if segment.ident == "Option" {
                if let PathArguments::AngleBracketed(args) = &segment.arguments {
                    if let Some(GenericArgument::Type(inner)) = args.args.first() {
                        return Ok((inner, Some(false)));
                    }
                }
                return Err("RPC Option parameters must have a type argument".into());
            }
            if segment.ident == "Vec" {
                return Ok((ty, None));
            }
        }
    }
    Ok((ty, Some(true)))
}
