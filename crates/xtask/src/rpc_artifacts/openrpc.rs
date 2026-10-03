//! Generates the node's OpenRPC method table without a runtime Rust parser.

use std::{fmt::Write, fs, path::Path};

use quote::ToTokens;
use syn::{FnArg, GenericArgument, PathArguments, ReturnType, TraitItem, Type};

use crate::BoxError;

pub(super) fn generate(source: &Path, output_dir: &Path) -> Result<(), BoxError> {
    let source = syn::parse_file(&fs::read_to_string(source)?)?;
    let rpc = source
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Trait(item) if item.ident == "Rpc" => Some(item),
            _ => None,
        })
        .ok_or("RPC methods source must contain the Rpc trait")?;
    let mut output = String::from(
        "/// Lookup table for JSON-RPC methods.\n\
         #[allow(unused_qualifications)]\n\
         pub(crate) static METHODS: ::phf::Map<&str, openrpc::RpcMethod> = ::phf::phf_map! {\n",
    );

    for item in &rpc.items {
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

        writeln!(output, "{name:?} => openrpc::RpcMethod {{")?;
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
        writeln!(
            output,
            "    result: |g| g.result({:?}),",
            format!("{name}_result")
        )?;
        let deprecated = method
            .attrs
            .iter()
            .any(|attr| attr.path().is_ident("deprecated"));
        writeln!(output, "    deprecated: {deprecated},")?;
        writeln!(output, "}},")?;
    }

    output.push_str("};");
    fs::write(output_dir.join("rpc_openrpc.rs"), output)?;
    Ok(())
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
