pub struct ColumnSchema {
    pub column_name: String,
    pub data_type: String,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<u8>,
    pub numeric_scale: Option<i32>,
}