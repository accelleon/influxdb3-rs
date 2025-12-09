use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error};

use crate::parser::{FieldType, parse_struct};

pub fn derive_into_point_impl(input: DeriveInput) -> Result<TokenStream, Error> {
    let struct_info = parse_struct(&input)?;
    let measurement = &struct_info.measurement;
    let struct_name = struct_info.struct_name;

    // Generate the implementation
    let mut field_assignments = Vec::new();
    let mut tag_assignments = Vec::new();
    let mut time_assignment = None;

    for info in struct_info.fields {
        if info.ignore {
            continue;
        }

        let field_name = &info.field_name;
        let point_name = info.rename.unwrap_or_else(|| field_name.to_string());

        match info.field_type {
            FieldType::Time => {
                time_assignment = Some(quote! {
                    point.set_timestamp(self.#field_name);
                });
            }
            FieldType::Tag => {
                tag_assignments.push(quote! {
                    point.set_tag(#point_name, &self.#field_name);
                });
            }
            FieldType::Field => {
                field_assignments.push(quote! {
                    point.set_field(#point_name, self.#field_name);
                });
            }
        }
    }

    let time_assignment = time_assignment.unwrap_or_else(|| quote! {
        point.set_timestamp(chrono::Utc::now());
    });

    let expanded = quote! {
        impl influxdb3_core::ToPoint for #struct_name {
            fn to_point(self) -> influxdb3_core::Point {
                let mut point = influxdb3_core::Point::new_with_measurement(#measurement);
                
                #time_assignment
                
                #(#tag_assignments)*
                
                #(#field_assignments)*
                
                point
            }
        }
    };

    Ok(expanded)
}
