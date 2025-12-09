use proc_macro2::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, spanned::Spanned};

use crate::parser::{FieldType, parse_struct};

pub fn derive_from_point_impl(input: DeriveInput) -> Result<TokenStream, Error> {
    let struct_info = parse_struct(&input)?;
    let struct_name = struct_info.struct_name;

    for info in &struct_info.fields {
        if info.ignore && !info.use_default {
            return Err(Error::new(
                info.field_name.span(),
                "Ignored fields must also be marked with #[influxdb(default)]"
            ));
        }
    }

    let mut field_extractions = Vec::new();
    let mut tag_extractions = Vec::new();
    let mut time_extraction = None;

    for info in struct_info.fields {
        let field_name = &info.field_name;
        let point_name = info.rename.unwrap_or_else(|| field_name.to_string());
        let field_ty = &info.ty;

        match info.field_type {
            FieldType::Time => {
                time_extraction = Some(quote! {
                    #field_name: point.time
                });
            }
            FieldType::Tag => {
                if info.use_default {
                    if info.ignore {
                        tag_extractions.push(quote! {
                            #field_name: Default::default()
                        });
                    } else {
                        tag_extractions.push(quote! {
                            #field_name: point.get_tag(#point_name)
                                .map(|s| s.parse::<#field_ty>()
                                    .map_err(|_| influxdb3_core::InfluxDBError::Other(
                                        format!("Failed to parse tag '{}' as {}", #point_name, stringify!(#field_ty))
                                    )))
                                .transpose()?
                                .unwrap_or_default()
                        });
                    }
                } else {
                    tag_extractions.push(quote! {
                        #field_name: point.get_tag(#point_name)
                            .ok_or_else(|| influxdb3_core::InfluxDBError::Other(
                                format!("Missing required tag: {}", #point_name)
                            ))?
                            .parse::<#field_ty>()
                            .map_err(|_| influxdb3_core::InfluxDBError::Other(
                                format!("Failed to parse tag '{}' as {}", #point_name, stringify!(#field_ty))
                            ))?
                    });
                }
            }
            FieldType::Field => {
                if info.use_default {
                    if info.ignore {
                        field_extractions.push(quote! {
                            #field_name: Default::default()
                        });
                    } else {
                        field_extractions.push(quote! {
                            #field_name: point.get_field(#point_name)
                                .map_err(|e| influxdb3_core::InfluxDBError::Other(
                                    format!("Failed to convert field '{}': {:?}", #point_name, e)
                                ))?
                                .unwrap_or_default()
                        });
                    }
                } else {
                    field_extractions.push(quote! {
                        #field_name: point.get_field(#point_name)
                            .map_err(|e| influxdb3_core::InfluxDBError::Other(
                                format!("Failed to convert field '{}': {:?}", #point_name, e)
                            ))?
                            .ok_or_else(|| influxdb3_core::InfluxDBError::Other(
                                format!("Missing required field: {}", #point_name)
                            ))?
                    });
                }
            }
        }
    }

    let time_extraction = time_extraction.ok_or_else(|| {
        Error::new(
            input.span(),
            "FromPoint requires a time field (either named 'time' or marked with #[influxdb(time)])"
        )
    })?;

    let expanded = quote! {
        impl influxdb3_core::FromPoint for #struct_name {
            fn from_point(point: influxdb3_core::Point) -> Result<Self, influxdb3_core::InfluxDBError> {
                Ok(Self {
                    #time_extraction,
                    #(#tag_extractions,)*
                    #(#field_extractions,)*
                })
            }
        }
    };

    Ok(expanded)
}
