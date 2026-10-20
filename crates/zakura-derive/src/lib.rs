//! Derive macros for Zakura.
//!
//! [`OutlineDebug`] derives the same `Debug` impl as `#[derive(Debug)]`, but
//! compiles its body once. The standard derive marks `fmt` `#[inline]`, so
//! every codegen unit that formats a type compiles its own copy, and ThinLTO
//! never merges those copies. For large enums such as error types, the copies
//! add up to hundreds of KiB in `zakurad`.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    ext::IdentExt, parse_macro_input, parse_quote, spanned::Spanned, Data, DeriveInput, Fields,
    GenericParam, Ident, LitStr, Type,
};

/// Derives `Debug` with the same output as `#[derive(Debug)]`, but marks `fmt`
/// `#[inline(never)]` so the body is compiled once instead of once per
/// codegen unit that formats the type.
///
/// Like the standard derive, the impl requires `T: Debug` for each type
/// parameter `T`, and for each field type that is a projection of one, such as
/// `T::Proof`.
///
/// Use it on types whose `Debug` output only appears on cold paths, such as
/// errors and logged requests.
#[proc_macro_derive(OutlineDebug)]
pub fn derive_outline_debug(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(mut input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = input.ident.clone();
    let body = match &input.data {
        Data::Struct(data) => {
            let (pattern, format) = fields(quote!(Self), &name, &data.fields);
            quote! {
                let #pattern = self;
                #format
            }
        }
        Data::Enum(data) => {
            let arms = data.variants.iter().map(|variant| {
                let variant_name = &variant.ident;
                let (pattern, format) =
                    fields(quote!(Self::#variant_name), variant_name, &variant.fields);
                quote!(#pattern => #format,)
            });
            quote! {
                match self {
                    #(#arms)*
                }
            }
        }
        Data::Union(data) => {
            return Err(syn::Error::new(
                data.union_token.span(),
                "OutlineDebug does not support unions",
            ))
        }
    };

    add_debug_bounds(&mut input);
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics ::core::fmt::Debug for #name #type_generics #where_clause {
            #[inline(never)]
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                #body
            }
        }
    })
}

/// Returns a pattern binding every field of `path`, and the expression that
/// formats them as `#[derive(Debug)]` does.
fn fields(path: TokenStream2, name: &Ident, fields: &Fields) -> (TokenStream2, TokenStream2) {
    let name = LitStr::new(&name.unraw().to_string(), name.span());
    match fields {
        Fields::Named(fields) => {
            let idents: Vec<_> = fields
                .named
                .iter()
                .map(|field| field.ident.as_ref().expect("named fields have identifiers"))
                .collect();
            let labels = idents
                .iter()
                .map(|ident| LitStr::new(&ident.unraw().to_string(), ident.span()));
            let bindings = bindings(idents.len());
            (
                quote!(#path { #(#idents: #bindings),* }),
                quote!(f.debug_struct(#name)#(.field(#labels, #bindings))*.finish()),
            )
        }
        Fields::Unnamed(fields) => {
            let bindings = bindings(fields.unnamed.len());
            (
                quote!(#path(#(#bindings),*)),
                quote!(f.debug_tuple(#name)#(.field(#bindings))*.finish()),
            )
        }
        Fields::Unit => (path, quote!(f.write_str(#name))),
    }
}

/// Field bindings that cannot shadow the formatter `f`, whatever the fields
/// are named.
fn bindings(count: usize) -> Vec<Ident> {
    (0..count)
        .map(|index| Ident::new(&format!("__self_{index}"), Span::call_site()))
        .collect()
}

/// Adds the bounds `#[derive(Debug)]` adds: `T: Debug` for each type
/// parameter, and `T::X: Debug` for each field type projected from one.
fn add_debug_bounds(input: &mut DeriveInput) {
    let params: Vec<Ident> = input
        .generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Type(param) => Some(param.ident.clone()),
            _ => None,
        })
        .collect();
    if params.is_empty() {
        return;
    }

    // Keyed by tokens, since `syn::Type` only implements `Eq` with the
    // `extra-traits` feature.
    let mut projections: Vec<(String, Type)> = Vec::new();
    let field_types = match &input.data {
        Data::Struct(data) => data.fields.iter().map(|field| &field.ty).collect(),
        Data::Enum(data) => data
            .variants
            .iter()
            .flat_map(|variant| variant.fields.iter().map(|field| &field.ty))
            .collect(),
        Data::Union(_) => Vec::new(),
    };
    for ty in field_types {
        let key = quote!(#ty).to_string();
        if is_param_projection(ty, &params) && !projections.iter().any(|(seen, _)| *seen == key) {
            projections.push((key, ty.clone()));
        }
    }

    let where_clause = input.generics.make_where_clause();
    for param in &params {
        where_clause
            .predicates
            .push(parse_quote!(#param: ::core::fmt::Debug));
    }
    for (_, ty) in projections {
        where_clause
            .predicates
            .push(parse_quote!(#ty: ::core::fmt::Debug));
    }
}

/// Whether `ty` is `T::X` or `<T as Trait>::X` for a type parameter `T`.
fn is_param_projection(ty: &Type, params: &[Ident]) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    if let Some(qself) = &path.qself {
        return matches!(
            &*qself.ty,
            Type::Path(inner) if inner.qself.is_none() && inner.path.get_ident().is_some_and(|ident| params.contains(ident))
        );
    }
    path.path.segments.len() > 1
        && path.path.leading_colon.is_none()
        && params.contains(&path.path.segments[0].ident)
}
