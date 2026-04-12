#[cfg(test)]
mod mock_tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use futures::StreamExt;
    use futures::stream::{self, BoxStream};

    use crate::common::constraints::Constraint;
    use crate::common::mssql_type::MssqlType;
    use crate::common::schema::ColumnSchema;
    use crate::common::target_schema::TargetColumn;
    use crate::extract::traits::Extractor;
    use crate::insert::table_action::TableAction;
    use crate::insert::traits::Inserter;
    use crate::migrate::migration_options::MigrationOptions;
    use crate::migrate::migrator::DatabaseMigrator;
    use crate::migrate::table_migrator::TableMigrator;
    use crate::migrate::type_registry::TypeRegistry;

    // -------------------------------------------------------
    // Mock Extractor
    // -------------------------------------------------------

    #[derive(Clone)]
    struct MockExtractor {
        tables: Vec<String>,
        schemas: HashMap<String, Vec<ColumnSchema>>,
        rows: HashMap<String, Vec<Vec<String>>>,
        fail_on_schema: Option<String>,
    }

    impl MockExtractor {
        fn new() -> Self {
            MockExtractor {
                tables: Vec::new(),
                schemas: HashMap::new(),
                rows: HashMap::new(),
                fail_on_schema: None,
            }
        }

        fn with_table(
            mut self,
            name: &str,
            schema: Vec<ColumnSchema>,
            rows: Vec<Vec<String>>,
        ) -> Self {
            self.tables.push(name.to_string());
            self.schemas.insert(name.to_string(), schema);
            self.rows.insert(name.to_string(), rows);
            self
        }

        fn with_schema_failure(mut self, table: &str) -> Self {
            self.fail_on_schema = Some(table.to_string());
            self
        }
    }

    #[async_trait]
    impl Extractor for MockExtractor {
        async fn fetch_tables(&self) -> Result<Vec<String>> {
            Ok(self.tables.clone())
        }

        async fn get_table_schema(&self, table: &str) -> Result<Vec<ColumnSchema>> {
            if self.fail_on_schema.as_deref() == Some(table) {
                return Err(anyhow!("Schema fetch failed for {}", table));
            }
            self.schemas
                .get(table)
                .cloned()
                .ok_or_else(|| anyhow!("No schema for table {}", table))
        }

        async fn stream_rows(
            &self,
            table: &str,
        ) -> Result<BoxStream<'static, Result<Vec<String>>>> {
            let rows = self.rows.get(table).cloned().unwrap_or_default();
            Ok(stream::iter(rows.into_iter().map(Ok)).boxed())
        }
    }

    // -------------------------------------------------------
    // Mock Inserter
    // -------------------------------------------------------

    #[derive(Clone)]
    struct MockInserter {
        max_packet: usize,
        existing_tables: Arc<Mutex<HashMap<String, i64>>>,
        created_tables: Arc<Mutex<Vec<String>>>,
        executed_queries: Arc<Mutex<Vec<String>>>,
        constraints_created: Arc<Mutex<Vec<String>>>,
        reset_called: Arc<Mutex<bool>>,
        fail_on_create: Option<String>,
    }

    impl MockInserter {
        fn new() -> Self {
            MockInserter {
                max_packet: 1_048_576,
                existing_tables: Arc::new(Mutex::new(HashMap::new())),
                created_tables: Arc::new(Mutex::new(Vec::new())),
                executed_queries: Arc::new(Mutex::new(Vec::new())),
                constraints_created: Arc::new(Mutex::new(Vec::new())),
                reset_called: Arc::new(Mutex::new(false)),
                fail_on_create: None,
            }
        }

        fn with_max_packet(mut self, size: usize) -> Self {
            self.max_packet = size;
            self
        }

        fn with_existing_table(self, name: &str, row_count: i64) -> Self {
            self.existing_tables
                .lock()
                .unwrap()
                .insert(name.to_string(), row_count);
            self
        }

        fn with_create_failure(mut self, table: &str) -> Self {
            self.fail_on_create = Some(table.to_string());
            self
        }
    }

    #[async_trait]
    impl Inserter for MockInserter {
        async fn create_table(&self, name: &str, _schema: &[TargetColumn]) -> Result<()> {
            if self.fail_on_create.as_deref() == Some(name) {
                return Err(anyhow!("Failed to create table {}", name));
            }
            self.created_tables.lock().unwrap().push(name.to_string());
            Ok(())
        }

        async fn create_constraints(
            &self,
            name: &str,
            _schema: &[TargetColumn],
            _tables: &[String],
        ) -> Result<()> {
            self.constraints_created
                .lock()
                .unwrap()
                .push(name.to_string());
            Ok(())
        }

        async fn execute_transactional_query(&self, query: &str) -> Result<()> {
            self.executed_queries
                .lock()
                .unwrap()
                .push(query.to_string());
            Ok(())
        }

        async fn get_max_allowed_packet(&self) -> Result<usize> {
            Ok(self.max_packet)
        }

        async fn reset_tables(&self, _tables: &[String], _action: TableAction) -> Result<()> {
            *self.reset_called.lock().unwrap() = true;
            Ok(())
        }

        async fn table_exists(&self, name: &str) -> Result<bool> {
            Ok(self.existing_tables.lock().unwrap().contains_key(name))
        }

        async fn table_rows_count(&self, name: &str) -> Result<i64> {
            Ok(*self.existing_tables.lock().unwrap().get(name).unwrap_or(&0))
        }
    }

    // -------------------------------------------------------
    // Helpers
    // -------------------------------------------------------

    fn make_column(name: &str, data_type: MssqlType) -> ColumnSchema {
        ColumnSchema {
            column_name: name.to_string(),
            data_type,
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
            is_nullable: true,
            constraints: None,
        }
    }

    fn default_registry() -> TypeRegistry {
        TypeRegistry::with_defaults()
    }

    fn make_options(whitelisted: Vec<&str>) -> MigrationOptions {
        MigrationOptions {
            drop: true,
            constraints: false,
            format_snake_case: false,
            max_concurrent_tasks: 4,
            max_packet_bytes: 1_048_576,
            whitelisted_tables: whitelisted.into_iter().map(String::from).collect(),
        }
    }

    // -------------------------------------------------------
    // DatabaseMigrator tests
    // -------------------------------------------------------

    #[tokio::test]
    async fn test_migrator_single_table_success() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![
                make_column("id", MssqlType::Int),
                make_column("name", MssqlType::Varchar),
            ],
            vec![
                vec!["1".into(), "'Alice'".into()],
                vec!["2".into(), "'Bob'".into()],
            ],
        );

        let inserter = MockInserter::new();
        let registry = default_registry();
        let options = make_options(vec!["Users"]);

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        assert!(
            inserter
                .created_tables
                .lock()
                .unwrap()
                .contains(&"Users".to_string())
        );
        assert!(*inserter.reset_called.lock().unwrap());
        assert!(!inserter.executed_queries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_migrator_multiple_tables() {
        let extractor = MockExtractor::new()
            .with_table(
                "Users",
                vec![make_column("id", MssqlType::Int)],
                vec![vec!["1".into()]],
            )
            .with_table(
                "Orders",
                vec![make_column("id", MssqlType::Int)],
                vec![vec!["10".into()]],
            );

        let inserter = MockInserter::new();
        let registry = default_registry();
        let options = make_options(vec!["Users", "Orders"]);

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        let created = inserter.created_tables.lock().unwrap();
        assert_eq!(created.len(), 2);
        assert!(created.contains(&"Users".to_string()));
        assert!(created.contains(&"Orders".to_string()));
    }

    #[tokio::test]
    async fn test_migrator_whitelist_filters_tables() {
        let extractor = MockExtractor::new()
            .with_table("Users", vec![make_column("id", MssqlType::Int)], vec![])
            .with_table("Orders", vec![make_column("id", MssqlType::Int)], vec![])
            .with_table("Logs", vec![make_column("id", MssqlType::Int)], vec![]);

        let inserter = MockInserter::new();
        let registry = default_registry();
        let options = make_options(vec!["Users", "Orders"]);

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        let created = inserter.created_tables.lock().unwrap();
        assert_eq!(created.len(), 2);
        assert!(!created.contains(&"Logs".to_string()));
    }

    #[tokio::test]
    async fn test_migrator_no_tables_returns_error() {
        let extractor = MockExtractor::new();
        let inserter = MockInserter::new();
        let registry = default_registry();
        let options = make_options(vec!["Users"]);

        let migrator = DatabaseMigrator::new(extractor, inserter, registry, options);
        let result = migrator.run().await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No tables"));
    }

    #[tokio::test]
    async fn test_migrator_no_whitelisted_tables_match() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![],
        );

        let inserter = MockInserter::new();
        let registry = default_registry();
        let options = make_options(vec!["Orders"]);

        let migrator = DatabaseMigrator::new(extractor, inserter, registry, options);
        let result = migrator.run().await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("filtering"));
    }

    #[tokio::test]
    async fn test_migrator_packet_size_too_large() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![],
        );

        let inserter = MockInserter::new().with_max_packet(1000);
        let registry = default_registry();
        let mut options = make_options(vec!["Users"]);
        options.max_packet_bytes = 2000;

        let migrator = DatabaseMigrator::new(extractor, inserter, registry, options);
        let result = migrator.run().await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("packet"));
    }

    #[tokio::test]
    async fn test_migrator_fail_fast_on_table_error() {
        let extractor = MockExtractor::new()
            .with_table(
                "Users",
                vec![make_column("id", MssqlType::Int)],
                vec![vec!["1".into()]],
            )
            .with_table(
                "Orders",
                vec![make_column("id", MssqlType::Int)],
                vec![vec!["1".into()]],
            );

        let inserter = MockInserter::new().with_create_failure("Orders");
        let registry = default_registry();
        let options = make_options(vec!["Users", "Orders"]);

        let migrator = DatabaseMigrator::new(extractor, inserter, registry, options);
        let result = migrator.run().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_migrator_table_already_has_rows() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![],
        );

        let inserter = MockInserter::new().with_existing_table("Users", 100);
        let registry = default_registry();
        let options = make_options(vec!["Users"]);

        let migrator = DatabaseMigrator::new(extractor, inserter, registry, options);
        let result = migrator.run().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_chain: String = err
            .chain()
            .map(|e| e.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert!(
            err_chain.contains("already contains"),
            "Error: {}",
            err_chain
        );
    }

    #[tokio::test]
    async fn test_migrator_existing_empty_table_reuses() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![vec!["1".into()]],
        );

        let inserter = MockInserter::new().with_existing_table("Users", 0);
        let registry = default_registry();
        let options = make_options(vec!["Users"]);

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        assert!(inserter.created_tables.lock().unwrap().is_empty());
        assert!(!inserter.executed_queries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_migrator_snake_case_formatting() {
        let extractor = MockExtractor::new().with_table(
            "UserAccounts",
            vec![make_column("AccountId", MssqlType::Int)],
            vec![vec!["1".into()]],
        );

        let inserter = MockInserter::new();
        let registry = default_registry();
        let mut options = make_options(vec!["UserAccounts"]);
        options.format_snake_case = true;

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        let created = inserter.created_tables.lock().unwrap();
        assert!(created.contains(&"user_accounts".to_string()));
    }

    #[tokio::test]
    async fn test_migrator_with_constraints() {
        let schema = vec![{
            let mut col = make_column("id", MssqlType::Int);
            col.constraints = Some(Constraint::PrimaryKey);
            col
        }];

        let extractor = MockExtractor::new().with_table("Users", schema, vec![vec!["1".into()]]);

        let inserter = MockInserter::new();
        let registry = default_registry();
        let mut options = make_options(vec!["Users"]);
        options.constraints = true;

        let migrator = DatabaseMigrator::new(extractor, inserter.clone(), registry, options);
        let result = migrator.run().await;

        assert!(result.is_ok());
        let constraints = inserter.constraints_created.lock().unwrap();
        assert!(constraints.contains(&"Users".to_string()));
    }

    // -------------------------------------------------------
    // TableMigrator batching tests
    // -------------------------------------------------------

    #[tokio::test]
    async fn test_table_migrator_empty_table() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![],
        );

        let inserter = MockInserter::new();
        let registry = Arc::new(default_registry());
        let options = make_options(vec!["Users"]);

        let tm = TableMigrator::new(extractor, inserter.clone(), registry, options);
        let result = tm.migrate_table("Users").await;

        assert!(result.is_ok());
        let mr = result.unwrap();
        assert_eq!(mr.table_name, "Users");
        assert!(mr.created);
        assert!(inserter.executed_queries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_table_migrator_single_batch() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![
                make_column("id", MssqlType::Int),
                make_column("name", MssqlType::Varchar),
            ],
            vec![
                vec!["1".into(), "'Alice'".into()],
                vec!["2".into(), "'Bob'".into()],
                vec!["3".into(), "'Charlie'".into()],
            ],
        );

        let inserter = MockInserter::new();
        let registry = Arc::new(default_registry());
        let options = make_options(vec!["Users"]);

        let tm = TableMigrator::new(extractor, inserter.clone(), registry, options);
        let result = tm.migrate_table("Users").await;

        assert!(result.is_ok());
        let queries = inserter.executed_queries.lock().unwrap();
        assert_eq!(queries.len(), 1);
        assert!(queries[0].contains("INSERT INTO"));
        assert!(queries[0].contains("'Alice'"));
        assert!(queries[0].contains("'Charlie'"));
    }

    #[tokio::test]
    async fn test_table_migrator_multiple_batches() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![
                vec!["1".into()],
                vec!["2".into()],
                vec!["3".into()],
                vec!["4".into()],
                vec!["5".into()],
            ],
        );

        let inserter = MockInserter::new().with_max_packet(50);
        let registry = Arc::new(default_registry());
        let mut options = make_options(vec!["Users"]);
        options.max_packet_bytes = 50;

        let tm = TableMigrator::new(extractor, inserter.clone(), registry, options);
        let result = tm.migrate_table("Users").await;

        assert!(result.is_ok());
        let queries = inserter.executed_queries.lock().unwrap();
        assert!(
            queries.len() > 1,
            "Expected multiple batches, got {}",
            queries.len()
        );
    }

    #[tokio::test]
    async fn test_table_migrator_schema_error() {
        let extractor = MockExtractor::new()
            .with_table("Users", vec![make_column("id", MssqlType::Int)], vec![])
            .with_schema_failure("Users");

        let inserter = MockInserter::new();
        let registry = Arc::new(default_registry());
        let options = make_options(vec!["Users"]);

        let tm = TableMigrator::new(extractor, inserter, registry, options);
        let result = tm.migrate_table("Users").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema"));
    }

    #[tokio::test]
    async fn test_table_migrator_create_failure() {
        let extractor = MockExtractor::new().with_table(
            "Users",
            vec![make_column("id", MssqlType::Int)],
            vec![],
        );

        let inserter = MockInserter::new().with_create_failure("Users");
        let registry = Arc::new(default_registry());
        let options = make_options(vec!["Users"]);

        let tm = TableMigrator::new(extractor, inserter, registry, options);
        let result = tm.migrate_table("Users").await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("create table"));
    }
}
