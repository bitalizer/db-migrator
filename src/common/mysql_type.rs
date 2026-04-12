use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MySqlBaseType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Float,
    Real,
    Char,
    Varchar,
    Text,
    LongText,
    Binary,
    VarBinary,
    LongBlob,
    DateTime,
    Timestamp,
    Date,
    Time,
}

impl MySqlBaseType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "tinyint" => Some(Self::TinyInt),
            "smallint" => Some(Self::SmallInt),
            "int" => Some(Self::Int),
            "bigint" => Some(Self::BigInt),
            "decimal" => Some(Self::Decimal),
            "float" => Some(Self::Float),
            "real" => Some(Self::Real),
            "char" => Some(Self::Char),
            "varchar" => Some(Self::Varchar),
            "text" => Some(Self::Text),
            "longtext" => Some(Self::LongText),
            "binary" => Some(Self::Binary),
            "varbinary" => Some(Self::VarBinary),
            "longblob" => Some(Self::LongBlob),
            "datetime" => Some(Self::DateTime),
            "timestamp" => Some(Self::Timestamp),
            "date" => Some(Self::Date),
            "time" => Some(Self::Time),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::TinyInt => "tinyint",
            Self::SmallInt => "smallint",
            Self::Int => "int",
            Self::BigInt => "bigint",
            Self::Decimal => "decimal",
            Self::Float => "float",
            Self::Real => "real",
            Self::Char => "char",
            Self::Varchar => "varchar",
            Self::Text => "text",
            Self::LongText => "longtext",
            Self::Binary => "binary",
            Self::VarBinary => "varbinary",
            Self::LongBlob => "longblob",
            Self::DateTime => "datetime",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Time => "time",
        }
    }

    pub fn accepts_length(&self) -> bool {
        matches!(self, Self::Varchar | Self::Char | Self::Binary | Self::VarBinary)
    }

    pub fn accepts_precision(&self) -> bool {
        matches!(self, Self::Decimal | Self::Float | Self::Real)
    }

    pub fn accepts_unsigned(&self) -> bool {
        matches!(
            self,
            Self::TinyInt | Self::SmallInt | Self::Int | Self::BigInt
                | Self::Decimal | Self::Float | Self::Real
        )
    }

    pub fn max_length(&self) -> Option<u32> {
        match self {
            Self::Char => Some(255),
            Self::Varchar => Some(65535),
            Self::Binary => Some(255),
            Self::VarBinary => Some(65535),
            _ => None,
        }
    }
}

impl fmt::Display for MySqlBaseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MySqlType {
    pub base_type: MySqlBaseType,
    pub length: Option<u32>,
    pub precision: Option<u8>,
    pub scale: Option<u8>,
    pub unsigned: bool,
    pub zerofill: bool,
}

impl MySqlType {
    pub fn to_sql(&self) -> String {
        let mut s = self.base_type.as_str().to_string();

        if self.base_type.accepts_length() {
            if let Some(len) = self.length {
                s.push_str(&format!("({})", len));
            }
        } else if self.base_type.accepts_precision() {
            if let Some(prec) = self.precision {
                if let Some(scale) = self.scale {
                    s.push_str(&format!("({}, {})", prec, scale));
                } else {
                    s.push_str(&format!("({})", prec));
                }
            }
        }

        if self.base_type.accepts_unsigned() {
            if self.unsigned {
                s.push_str(" unsigned");
            }
            if self.zerofill {
                s.push_str(" zerofill");
            }
        }

        s
    }
}

impl fmt::Display for MySqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_sql())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // MySqlBaseType tests

    #[test]
    fn test_base_type_as_str() {
        assert_eq!(MySqlBaseType::Int.as_str(), "int");
        assert_eq!(MySqlBaseType::BigInt.as_str(), "bigint");
        assert_eq!(MySqlBaseType::Varchar.as_str(), "varchar");
        assert_eq!(MySqlBaseType::LongText.as_str(), "longtext");
        assert_eq!(MySqlBaseType::DateTime.as_str(), "datetime");
    }

    #[test]
    fn test_from_str_valid() {
        assert_eq!(MySqlBaseType::from_str("int"), Some(MySqlBaseType::Int));
        assert_eq!(MySqlBaseType::from_str("INT"), Some(MySqlBaseType::Int));
        assert_eq!(MySqlBaseType::from_str("varchar"), Some(MySqlBaseType::Varchar));
        assert_eq!(MySqlBaseType::from_str("longtext"), Some(MySqlBaseType::LongText));
    }

    #[test]
    fn test_from_str_unknown() {
        assert_eq!(MySqlBaseType::from_str("geometry"), None);
        assert_eq!(MySqlBaseType::from_str(""), None);
    }

    #[test]
    fn test_accepts_length() {
        assert!(MySqlBaseType::Varchar.accepts_length());
        assert!(MySqlBaseType::Char.accepts_length());
        assert!(MySqlBaseType::Binary.accepts_length());
        assert!(MySqlBaseType::VarBinary.accepts_length());
        assert!(!MySqlBaseType::Int.accepts_length());
        assert!(!MySqlBaseType::LongText.accepts_length());
    }

    #[test]
    fn test_accepts_precision() {
        assert!(MySqlBaseType::Decimal.accepts_precision());
        assert!(MySqlBaseType::Float.accepts_precision());
        assert!(MySqlBaseType::Real.accepts_precision());
        assert!(!MySqlBaseType::Int.accepts_precision());
        assert!(!MySqlBaseType::Varchar.accepts_precision());
    }

    #[test]
    fn test_accepts_unsigned() {
        assert!(MySqlBaseType::Int.accepts_unsigned());
        assert!(MySqlBaseType::BigInt.accepts_unsigned());
        assert!(MySqlBaseType::Decimal.accepts_unsigned());
        assert!(!MySqlBaseType::Varchar.accepts_unsigned());
        assert!(!MySqlBaseType::DateTime.accepts_unsigned());
        assert!(!MySqlBaseType::LongText.accepts_unsigned());
    }

    #[test]
    fn test_max_length() {
        assert_eq!(MySqlBaseType::Char.max_length(), Some(255));
        assert_eq!(MySqlBaseType::Varchar.max_length(), Some(65535));
        assert_eq!(MySqlBaseType::Binary.max_length(), Some(255));
        assert_eq!(MySqlBaseType::VarBinary.max_length(), Some(65535));
        assert_eq!(MySqlBaseType::Int.max_length(), None);
        assert_eq!(MySqlBaseType::LongText.max_length(), None);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", MySqlBaseType::Int), "int");
        assert_eq!(format!("{}", MySqlBaseType::Varchar), "varchar");
        assert_eq!(format!("{}", MySqlBaseType::LongText), "longtext");
    }

    // MySqlType tests

    #[test]
    fn test_to_sql_base_only() {
        let t = MySqlType {
            base_type: MySqlBaseType::LongText,
            length: None,
            precision: None,
            scale: None,
            unsigned: false,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "longtext");
    }

    #[test]
    fn test_to_sql_with_length() {
        let t = MySqlType {
            base_type: MySqlBaseType::Varchar,
            length: Some(255),
            precision: None,
            scale: None,
            unsigned: false,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "varchar(255)");
    }

    #[test]
    fn test_to_sql_with_precision_and_scale() {
        let t = MySqlType {
            base_type: MySqlBaseType::Decimal,
            length: None,
            precision: Some(10),
            scale: Some(2),
            unsigned: false,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "decimal(10, 2)");
    }

    #[test]
    fn test_to_sql_with_precision_only() {
        let t = MySqlType {
            base_type: MySqlBaseType::Float,
            length: None,
            precision: Some(24),
            scale: None,
            unsigned: false,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "float(24)");
    }

    #[test]
    fn test_to_sql_unsigned() {
        let t = MySqlType {
            base_type: MySqlBaseType::Int,
            length: None,
            precision: None,
            scale: None,
            unsigned: true,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "int unsigned");
    }

    #[test]
    fn test_to_sql_unsigned_zerofill() {
        let t = MySqlType {
            base_type: MySqlBaseType::BigInt,
            length: None,
            precision: None,
            scale: None,
            unsigned: true,
            zerofill: true,
        };
        assert_eq!(t.to_sql(), "bigint unsigned zerofill");
    }

    #[test]
    fn test_to_sql_ignores_unsigned_on_non_numeric() {
        let t = MySqlType {
            base_type: MySqlBaseType::LongText,
            length: None,
            precision: None,
            scale: None,
            unsigned: true,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "longtext");
    }

    #[test]
    fn test_to_sql_ignores_length_on_non_length_type() {
        let t = MySqlType {
            base_type: MySqlBaseType::LongText,
            length: Some(255),
            precision: None,
            scale: None,
            unsigned: false,
            zerofill: false,
        };
        assert_eq!(t.to_sql(), "longtext");
    }
}
