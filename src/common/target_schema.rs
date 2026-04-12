use crate::common::constraints::Constraint;
use crate::common::mysql_type::MySqlType;

/// A column in the MySQL target schema.
#[derive(Debug, Clone)]
pub struct TargetColumn {
    pub column_name: String,
    pub data_type: MySqlType,
    pub is_nullable: bool,
    pub constraints: Option<Constraint>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::mysql_type::{MySqlBaseType, MySqlType};
    use crate::common::constraints::Constraint;

    #[test]
    fn test_target_column_creation() {
        let col = TargetColumn {
            column_name: "user_id".to_string(),
            data_type: MySqlType {
                base_type: MySqlBaseType::Int,
                length: None,
                precision: None,
                scale: None,
                unsigned: true,
                zerofill: false,
            },
            is_nullable: false,
            constraints: Some(Constraint::PrimaryKey),
        };
        assert_eq!(col.column_name, "user_id");
        assert_eq!(col.data_type.to_sql(), "int unsigned");
        assert!(!col.is_nullable);
        assert_eq!(col.constraints, Some(Constraint::PrimaryKey));
    }
}
