use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MssqlType {
    Bit,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Decimal,
    Numeric,
    Money,
    SmallMoney,
    Float,
    Real,
    Char,
    NChar,
    Varchar,
    NVarchar,
    Text,
    NText,
    Binary,
    VarBinary,
    Image,
    Date,
    DateTime,
    DateTime2,
    SmallDateTime,
    DateTimeOffset,
    Time,
    UniqueIdentifier,
    Timestamp,
    Xml,
}

impl MssqlType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "bit" => Some(Self::Bit),
            "tinyint" => Some(Self::TinyInt),
            "smallint" => Some(Self::SmallInt),
            "int" => Some(Self::Int),
            "bigint" => Some(Self::BigInt),
            "decimal" => Some(Self::Decimal),
            "numeric" => Some(Self::Numeric),
            "money" => Some(Self::Money),
            "smallmoney" => Some(Self::SmallMoney),
            "float" => Some(Self::Float),
            "real" => Some(Self::Real),
            "char" => Some(Self::Char),
            "nchar" => Some(Self::NChar),
            "varchar" => Some(Self::Varchar),
            "nvarchar" => Some(Self::NVarchar),
            "text" => Some(Self::Text),
            "ntext" => Some(Self::NText),
            "binary" => Some(Self::Binary),
            "varbinary" => Some(Self::VarBinary),
            "image" => Some(Self::Image),
            "date" => Some(Self::Date),
            "datetime" => Some(Self::DateTime),
            "datetime2" => Some(Self::DateTime2),
            "smalldatetime" => Some(Self::SmallDateTime),
            "datetimeoffset" => Some(Self::DateTimeOffset),
            "time" => Some(Self::Time),
            "uniqueidentifier" => Some(Self::UniqueIdentifier),
            "timestamp" => Some(Self::Timestamp),
            "xml" => Some(Self::Xml),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Bit => "bit",
            Self::TinyInt => "tinyint",
            Self::SmallInt => "smallint",
            Self::Int => "int",
            Self::BigInt => "bigint",
            Self::Decimal => "decimal",
            Self::Numeric => "numeric",
            Self::Money => "money",
            Self::SmallMoney => "smallmoney",
            Self::Float => "float",
            Self::Real => "real",
            Self::Char => "char",
            Self::NChar => "nchar",
            Self::Varchar => "varchar",
            Self::NVarchar => "nvarchar",
            Self::Text => "text",
            Self::NText => "ntext",
            Self::Binary => "binary",
            Self::VarBinary => "varbinary",
            Self::Image => "image",
            Self::Date => "date",
            Self::DateTime => "datetime",
            Self::DateTime2 => "datetime2",
            Self::SmallDateTime => "smalldatetime",
            Self::DateTimeOffset => "datetimeoffset",
            Self::Time => "time",
            Self::UniqueIdentifier => "uniqueidentifier",
            Self::Timestamp => "timestamp",
            Self::Xml => "xml",
        }
    }
}

impl fmt::Display for MssqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_standard_types() {
        assert_eq!(MssqlType::from_str("int"), Some(MssqlType::Int));
        assert_eq!(MssqlType::from_str("INT"), Some(MssqlType::Int));
        assert_eq!(MssqlType::from_str("nvarchar"), Some(MssqlType::NVarchar));
        assert_eq!(MssqlType::from_str("datetime2"), Some(MssqlType::DateTime2));
        assert_eq!(
            MssqlType::from_str("uniqueidentifier"),
            Some(MssqlType::UniqueIdentifier)
        );
    }

    #[test]
    fn test_from_str_unknown() {
        assert_eq!(MssqlType::from_str("geometry"), None);
        assert_eq!(MssqlType::from_str(""), None);
        assert_eq!(MssqlType::from_str("varchat"), None);
    }

    #[test]
    fn test_as_str_roundtrip() {
        let all_variants = [
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

        for variant in all_variants {
            assert_eq!(
                MssqlType::from_str(variant.as_str()),
                Some(variant),
                "roundtrip failed for {:?}",
                variant
            );
        }
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", MssqlType::Int), "int");
        assert_eq!(format!("{}", MssqlType::NVarchar), "nvarchar");
        assert_eq!(format!("{}", MssqlType::DateTime2), "datetime2");
        assert_eq!(
            format!("{}", MssqlType::UniqueIdentifier),
            "uniqueidentifier"
        );
        assert_eq!(format!("{}", MssqlType::VarBinary), "varbinary");
    }
}
