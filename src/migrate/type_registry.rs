use crate::common::mssql_type::MssqlType;
use crate::common::mysql_type::MySqlBaseType;
use crate::common::type_mapping_entry::TypeMappingEntry;
use crate::mappings::UserOverrides;
use std::collections::HashMap;

pub struct TypeRegistry {
    defaults: HashMap<MssqlType, TypeMappingEntry>,
    overrides: HashMap<MssqlType, TypeMappingEntry>,
    /// Column-scoped overrides keyed by lowercased "table.column" source names.
    column_overrides: HashMap<String, TypeMappingEntry>,
}

impl TypeRegistry {
    pub fn with_defaults() -> Self {
        let mut defaults = HashMap::new();

        // Integer types
        defaults.insert(MssqlType::Bit, Self::simple(MySqlBaseType::TinyInt));
        defaults.insert(MssqlType::TinyInt, Self::simple(MySqlBaseType::TinyInt));
        defaults.insert(MssqlType::SmallInt, Self::simple(MySqlBaseType::SmallInt));
        defaults.insert(MssqlType::Int, Self::simple(MySqlBaseType::Int));
        defaults.insert(MssqlType::BigInt, Self::simple(MySqlBaseType::BigInt));

        // Exact numeric
        defaults.insert(
            MssqlType::Decimal,
            Self::numeric(MySqlBaseType::Decimal, 10, 2),
        );
        defaults.insert(
            MssqlType::Numeric,
            Self::numeric(MySqlBaseType::Decimal, 18, 0),
        );
        defaults.insert(
            MssqlType::Money,
            Self::numeric(MySqlBaseType::Decimal, 19, 4),
        );
        defaults.insert(
            MssqlType::SmallMoney,
            Self::numeric(MySqlBaseType::Decimal, 10, 2),
        );

        // Approximate numeric
        defaults.insert(MssqlType::Float, Self::simple(MySqlBaseType::Float));
        defaults.insert(MssqlType::Real, Self::simple(MySqlBaseType::Real));

        // Character types
        defaults.insert(MssqlType::Char, Self::length(MySqlBaseType::Char, 1));
        defaults.insert(MssqlType::NChar, Self::length(MySqlBaseType::Char, 1));
        defaults.insert(
            MssqlType::Varchar,
            Self::length(MySqlBaseType::Varchar, 255),
        );
        defaults.insert(MssqlType::NVarchar, Self::simple(MySqlBaseType::LongText));
        defaults.insert(MssqlType::Text, Self::simple(MySqlBaseType::Text));
        defaults.insert(MssqlType::NText, Self::simple(MySqlBaseType::LongText));

        // Binary types
        defaults.insert(MssqlType::Binary, Self::length(MySqlBaseType::Binary, 1));
        defaults.insert(
            MssqlType::VarBinary,
            Self::length(MySqlBaseType::VarBinary, 255),
        );
        defaults.insert(MssqlType::Image, Self::simple(MySqlBaseType::LongBlob));

        // Date/time types
        defaults.insert(MssqlType::Date, Self::simple(MySqlBaseType::Date));
        defaults.insert(MssqlType::DateTime, Self::simple(MySqlBaseType::DateTime));
        defaults.insert(MssqlType::DateTime2, Self::simple(MySqlBaseType::DateTime));
        defaults.insert(
            MssqlType::SmallDateTime,
            Self::simple(MySqlBaseType::DateTime),
        );
        defaults.insert(
            MssqlType::DateTimeOffset,
            Self::simple(MySqlBaseType::DateTime),
        );
        defaults.insert(MssqlType::Time, Self::simple(MySqlBaseType::Time));
        defaults.insert(MssqlType::Timestamp, Self::simple(MySqlBaseType::Timestamp));

        // Special types
        defaults.insert(
            MssqlType::UniqueIdentifier,
            Self::length(MySqlBaseType::Char, 36),
        );
        defaults.insert(MssqlType::Xml, Self::simple(MySqlBaseType::LongText));

        TypeRegistry {
            defaults,
            overrides: HashMap::new(),
            column_overrides: HashMap::new(),
        }
    }

    pub fn get(&self, mssql_type: MssqlType) -> &TypeMappingEntry {
        self.overrides
            .get(&mssql_type)
            .or_else(|| self.defaults.get(&mssql_type))
            .expect("all MssqlType variants must have a default mapping")
    }

    /// Resolve the mapping for a specific column. Precedence:
    /// column override > type override > built-in default.
    /// Table/column names are matched case-insensitively against source names.
    pub fn resolve(
        &self,
        table_name: &str,
        column_name: &str,
        mssql_type: MssqlType,
    ) -> &TypeMappingEntry {
        let key = format!("{}.{}", table_name, column_name).to_lowercase();
        self.column_overrides
            .get(&key)
            .unwrap_or_else(|| self.get(mssql_type))
    }

    pub fn set_override(&mut self, mssql_type: MssqlType, entry: TypeMappingEntry) {
        self.overrides.insert(mssql_type, entry);
    }

    pub fn set_column_override(&mut self, table_column: &str, entry: TypeMappingEntry) {
        self.column_overrides
            .insert(table_column.to_lowercase(), entry);
    }

    pub fn with_user_overrides(mut self, overrides: &UserOverrides) -> Self {
        for (mssql_type, entry) in overrides.iter() {
            self.set_override(*mssql_type, entry.clone());
        }
        for (table_column, entry) in overrides.columns_iter() {
            self.set_column_override(table_column, entry.clone());
        }
        self
    }

    fn simple(mysql_type: MySqlBaseType) -> TypeMappingEntry {
        TypeMappingEntry {
            mysql_type,
            carry_length: false,
            carry_precision: false,
            default_length: None,
            default_precision: None,
            default_scale: None,
            unsigned: false,
            zerofill: false,
        }
    }

    fn numeric(mysql_type: MySqlBaseType, precision: u8, scale: u8) -> TypeMappingEntry {
        TypeMappingEntry {
            mysql_type,
            carry_length: false,
            carry_precision: true,
            default_length: None,
            default_precision: Some(precision),
            default_scale: Some(scale),
            unsigned: false,
            zerofill: false,
        }
    }

    fn length(mysql_type: MySqlBaseType, default_length: u32) -> TypeMappingEntry {
        TypeMappingEntry {
            mysql_type,
            carry_length: true,
            carry_precision: false,
            default_length: Some(default_length),
            default_precision: None,
            default_scale: None,
            unsigned: false,
            zerofill: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mssql_type::MssqlType;
    use crate::common::mysql_type::MySqlBaseType;

    #[test]
    fn test_every_mssql_type_has_mapping() {
        let registry = TypeRegistry::with_defaults();
        let all_types = vec![
            MssqlType::Bit,
            MssqlType::TinyInt,
            MssqlType::SmallInt,
            MssqlType::Int,
            MssqlType::BigInt,
            MssqlType::Decimal,
            MssqlType::Numeric,
            MssqlType::Money,
            MssqlType::SmallMoney,
            MssqlType::Float,
            MssqlType::Real,
            MssqlType::Char,
            MssqlType::NChar,
            MssqlType::Varchar,
            MssqlType::NVarchar,
            MssqlType::Text,
            MssqlType::NText,
            MssqlType::Binary,
            MssqlType::VarBinary,
            MssqlType::Image,
            MssqlType::Date,
            MssqlType::DateTime,
            MssqlType::DateTime2,
            MssqlType::SmallDateTime,
            MssqlType::DateTimeOffset,
            MssqlType::Time,
            MssqlType::UniqueIdentifier,
            MssqlType::Timestamp,
            MssqlType::Xml,
        ];
        for t in all_types {
            let entry = registry.get(t);
            assert!(
                !entry.mysql_type.as_str().is_empty(),
                "Empty mapping for {:?}",
                t
            );
        }
    }

    #[test]
    fn test_int_mapping() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Int);
        assert_eq!(m.mysql_type, MySqlBaseType::Int);
        assert!(!m.carry_precision);
    }

    #[test]
    fn test_decimal_mapping() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Decimal);
        assert_eq!(m.mysql_type, MySqlBaseType::Decimal);
        assert!(m.carry_precision);
        assert_eq!(m.default_precision, Some(10));
        assert_eq!(m.default_scale, Some(2));
    }

    #[test]
    fn test_money_mapping() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Money);
        assert_eq!(m.mysql_type, MySqlBaseType::Decimal);
        assert_eq!(m.default_precision, Some(19));
        assert_eq!(m.default_scale, Some(4));
    }

    #[test]
    fn test_nvarchar_is_longtext() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::NVarchar);
        assert_eq!(m.mysql_type, MySqlBaseType::LongText);
        assert!(!m.carry_length);
    }

    #[test]
    fn test_varchar_carries_length() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Varchar);
        assert_eq!(m.mysql_type, MySqlBaseType::Varchar);
        assert!(m.carry_length);
        assert_eq!(m.default_length, Some(255));
    }

    #[test]
    fn test_image_is_longblob() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Image);
        assert_eq!(m.mysql_type, MySqlBaseType::LongBlob);
    }

    #[test]
    fn test_xml_is_longtext() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.get(MssqlType::Xml);
        assert_eq!(m.mysql_type, MySqlBaseType::LongText);
    }

    #[test]
    fn test_override_replaces_default() {
        let mut registry = TypeRegistry::with_defaults();
        let entry = TypeMappingEntry {
            mysql_type: MySqlBaseType::Varchar,
            carry_length: true,
            carry_precision: false,
            default_length: Some(500),
            default_precision: None,
            default_scale: None,
            unsigned: false,
            zerofill: false,
        };
        registry.set_override(MssqlType::NVarchar, entry);
        let m = registry.get(MssqlType::NVarchar);
        assert_eq!(m.mysql_type, MySqlBaseType::Varchar);
        assert_eq!(m.default_length, Some(500));

        // Other defaults unaffected
        let int_m = registry.get(MssqlType::Int);
        assert_eq!(int_m.mysql_type, MySqlBaseType::Int);
    }

    #[test]
    fn test_with_user_overrides() {
        let toml_val: toml::Value = r#"
        [mappings]
        nvarchar = "varchar(500)"
        "#
        .parse()
        .unwrap();

        let overrides = crate::mappings::UserOverrides::from_toml(toml_val).unwrap();
        let registry = TypeRegistry::with_defaults().with_user_overrides(&overrides);

        let m = registry.get(MssqlType::NVarchar);
        assert_eq!(m.mysql_type, MySqlBaseType::Varchar);
        assert_eq!(m.default_length, Some(500));

        assert_eq!(registry.get(MssqlType::Int).mysql_type, MySqlBaseType::Int);
    }

    #[test]
    fn test_resolve_column_override_beats_type_override() {
        let toml_val: toml::Value = r#"
        [mappings]
        int = "bigint"

        [mappings.columns]
        "Orders.ID" = "int unsigned"
        "#
        .parse()
        .unwrap();

        let overrides = crate::mappings::UserOverrides::from_toml(toml_val).unwrap();
        let registry = TypeRegistry::with_defaults().with_user_overrides(&overrides);

        // Column override wins for the matching column
        let m = registry.resolve("Orders", "ID", MssqlType::Int);
        assert_eq!(m.mysql_type, MySqlBaseType::Int);
        assert!(m.unsigned);

        // Other columns of the same type fall back to the type override
        let m = registry.resolve("Orders", "ParentID", MssqlType::Int);
        assert_eq!(m.mysql_type, MySqlBaseType::BigInt);
        assert!(!m.unsigned);

        // Other tables unaffected by the column override
        let m = registry.resolve("Accounts", "ID", MssqlType::Int);
        assert_eq!(m.mysql_type, MySqlBaseType::BigInt);
        assert!(!m.unsigned);
    }

    #[test]
    fn test_resolve_falls_back_to_default_without_overrides() {
        let registry = TypeRegistry::with_defaults();
        let m = registry.resolve("AnyTable", "AnyColumn", MssqlType::Int);
        assert_eq!(m.mysql_type, MySqlBaseType::Int);
        assert!(!m.unsigned);
    }

    #[test]
    fn test_resolve_case_insensitive_match() {
        let toml_val: toml::Value = r#"
        [mappings.columns]
        "Orders.ID" = "int unsigned"
        "#
        .parse()
        .unwrap();

        let overrides = crate::mappings::UserOverrides::from_toml(toml_val).unwrap();
        let registry = TypeRegistry::with_defaults().with_user_overrides(&overrides);

        assert!(registry.resolve("orders", "id", MssqlType::Int).unsigned);
        assert!(registry.resolve("ORDERS", "ID", MssqlType::Int).unsigned);
    }
}
