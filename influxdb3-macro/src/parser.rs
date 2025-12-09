use syn::{Data, DeriveInput, Error, Fields, Lit, spanned::Spanned as _};

use crate::util::to_snake_case;

#[derive(Debug)]
pub(crate) enum FieldType {
    Time,
    Tag,
    Field,
}

#[derive(Debug)]
pub(crate) struct FieldInfo {
    pub field_name: syn::Ident,
    pub field_type: FieldType,
    pub rename: Option<String>,
    pub ty: syn::Type,
    pub use_default: bool,
    pub ignore: bool,
}

#[derive(Debug)]
pub(crate) struct StructInfo<'a> {
    pub struct_name: &'a syn::Ident,
    pub measurement: String,
    pub fields: Vec<FieldInfo>,
}

pub(crate) fn parse_struct(input: &'_ DeriveInput) -> Result<StructInfo<'_>, Error> {
    let struct_name = &input.ident;

    let data_struct = match &input.data {
        Data::Struct(data) => data,
        _ => return Err(Error::new(
            input.span(),
            "FromPoint can only be derived for structs"
        )),
    };

    let mut measurement_name = None;

    for attr in &input.attrs {
        if attr.path().is_ident("influxdb") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("measurement") {
                    if let Ok(value) = meta.value() {
                        if let Ok(lit) = value.parse::<Lit>() {
                            if let Lit::Str(s) = lit {
                                measurement_name = Some(s.value());
                            }
                        }
                    }
                }
                Ok(())
            })?;
        }
    }

    let fields = parse_fields(&data_struct.fields)?;
    let measurement = measurement_name.unwrap_or_else(|| {
        to_snake_case(&input.ident.to_string())
    });

    Ok(StructInfo {
        struct_name,
        measurement,
        fields,
    })
}

fn parse_fields(fields: &Fields) -> Result<Vec<FieldInfo>, Error> {
    let fields = match fields {
        Fields::Named(fields) => &fields.named,
        _ => return Err(Error::new(
            fields.span(),
            "ToPoint can only be derived for structs with named fields"
        )),
    };

    let mut field_infos = Vec::new();
    let mut time_field_count = 0;
    
    for field in fields {
        let field_name = field.ident.as_ref().unwrap();
        let field_name_str = field_name.to_string();
        let field_ty = field.ty.clone();
        
        let mut field_type = FieldType::Field;
        let mut rename = None;
        let mut is_time_attr = false;
        let mut is_tag_attr = false;
        let mut use_default = false;
        let mut ignore = false;

        if field_name_str == "time" {
            time_field_count += 1;
            field_type = FieldType::Time;
        }

        for attr in &field.attrs {
            if attr.path().is_ident("influxdb") {
                attr.parse_nested_meta(|meta| {
                    if meta.path.is_ident("time") {
                        is_time_attr = true;
                        time_field_count += 1;
                        Ok(())
                    } else if meta.path.is_ident("tag") {
                        is_tag_attr = true;
                        Ok(())
                    } else if meta.path.is_ident("rename") {
                        if let Ok(value) = meta.value() {
                            if let Ok(lit) = value.parse::<Lit>() {
                                if let Lit::Str(s) = lit {
                                    rename = Some(s.value());
                                }
                            }
                        }
                        Ok(())
                    } else if meta.path.is_ident("default") {
                        use_default = true;
                        Ok(())
                    } else if meta.path.is_ident("ignore") {
                        ignore = true;
                        Ok(())
                    } else {
                        Err(meta.error("Unknown influxdb attribute"))
                    }
                })?;
            }
        }

        if ignore && (is_tag_attr || is_time_attr) {
            return Err(Error::new(
                field.span(),
                "Ignored fields cannot be marked as tag or time"
            ));
        }

        if is_time_attr {
            field_type = FieldType::Time;
        } else if is_tag_attr {
            field_type = FieldType::Tag;
        }

        if matches!(field_type, FieldType::Time) && time_field_count > 1 {
            return Err(Error::new(
                field.span(),
                "Only one field can be marked as time. Found multiple time fields (either named 'time' or marked with #[influxdb(time)])"
            ));
        }

        field_infos.push(FieldInfo {
            field_name: field_name.clone(),
            field_type,
            rename,
            ty: field_ty,
            use_default,
            ignore,
        });
    }
    Ok(field_infos)
}