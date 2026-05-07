//! Derive macros for `storemy::codec::Encode` and `storemy::codec::Decode`.
//!
//! Two derives:
//! - `#[derive(Encode)]` — emits `Encode::encode` that writes each named field in declaration
//!   order, calling `field.encode(writer)?` per field.
//! - `#[derive(Decode)]` — emits `Decode::decode` that reads each named field in declaration order,
//!   calling `<FieldType>::decode(reader)?` per field, then builds `Self { ... }`.
//!
//! Both derives only support **structs with named fields**. Tuple structs, unit
//! structs, and enums are rejected with a compile-time error pointing at the
//! offending type.
//!
//! The generated code references the trait via `::storemy::codec::*`, so the
//! consuming crate's package name must be `storemy` (the workspace's main crate).

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DataStruct, DeriveInput, Fields, FieldsNamed, parse_macro_input, spanned::Spanned,
};

/// Derives `::storemy::codec::Encode` for a struct with named fields.
///
/// Each field is encoded in declaration order via `field.encode(writer)?`.
#[proc_macro_derive(Encode)]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match named_fields(&input) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    // For each field, emit `self.<field>.encode(writer)?;`.
    let calls = fields.named.iter().map(|f| {
        let ident = f.ident.as_ref().expect("named field has ident");
        quote! { self.#ident.encode(writer)?; }
    });

    let expanded = quote! {
        impl #impl_generics ::storemy::codec::Encode for #name #ty_generics #where_clause {
            fn encode<__W: ::std::io::Write>(
                &self,
                writer: &mut __W,
            ) -> ::std::result::Result<(), ::storemy::codec::CodecError> {
                #(#calls)*
                ::std::result::Result::Ok(())
            }
        }
    };

    expanded.into()
}

/// Derives `::storemy::codec::Decode` for a struct with named fields.
///
/// Each field is decoded in declaration order via `<FieldType>::decode(reader)?`.
#[proc_macro_derive(Decode)]
pub fn derive_decode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let fields = match named_fields(&input) {
        Ok(f) => f,
        Err(e) => return e.to_compile_error().into(),
    };

    // Two parallel lists: per-field `let <name> = <Ty>::decode(reader)?;`,
    // then a final `Ok(Self { <names> })`.
    let lets = fields.named.iter().map(|f| {
        let ident = f.ident.as_ref().expect("named field has ident");
        let ty = &f.ty;
        quote! { let #ident = <#ty as ::storemy::codec::Decode>::decode(reader)?; }
    });
    let names = fields.named.iter().map(|f| f.ident.as_ref().unwrap());

    let expanded = quote! {
        impl #impl_generics ::storemy::codec::Decode for #name #ty_generics #where_clause {
            fn decode<__R: ::std::io::Read>(
                reader: &mut __R,
            ) -> ::std::result::Result<Self, ::storemy::codec::CodecError> {
                #(#lets)*
                ::std::result::Result::Ok(Self { #(#names),* })
            }
        }
    };

    expanded.into()
}

/// Pull a `FieldsNamed` out of a `DeriveInput`, with a descriptive error
/// pointing at the type's name span when the shape is wrong.
fn named_fields(input: &DeriveInput) -> Result<&FieldsNamed, syn::Error> {
    match &input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(named),
            ..
        }) => Ok(named),
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(_) | Fields::Unit,
            ..
        }) => Err(syn::Error::new(
            input.ident.span(),
            "Encode/Decode derives require a struct with named fields",
        )),
        Data::Enum(_) | Data::Union(_) => Err(syn::Error::new(
            input.span(),
            "Encode/Decode derives only support structs",
        )),
    }
}
