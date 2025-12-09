use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod to_point;
mod from_point;
mod parser;
mod util;

#[proc_macro_derive(ToPoint, attributes(influxdb))]
pub fn derive_into_point(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    to_point::derive_into_point_impl(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

#[proc_macro_derive(FromPoint, attributes(influxdb))]
pub fn derive_from_point(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    from_point::derive_from_point_impl(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}