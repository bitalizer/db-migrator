use std::sync::Arc;

use anyhow::{Context, Error, Result};
use futures::TryStreamExt;
use log::info;
use tokio::time::Instant;

use crate::common::errors::MigrationError;
use crate::common::helpers::format_snake_case;
use crate::common::target_schema::TargetColumn;
use crate::extract::traits::Extractor;
use crate::insert::query::build_insert_statement;
use crate::insert::traits::Inserter;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migration_result::MigrationResult;
use crate::migrate::table_schema_mapper::TableSchemaMapper;
use crate::migrate::type_registry::TypeRegistry;

const RESERVED_BYTES: usize = 10;

pub struct TableMigrator<E: Extractor, I: Inserter> {
    extractor: E,
    inserter: I,
    registry: Arc<TypeRegistry>,
    options: MigrationOptions,
}

impl<E: Extractor, I: Inserter> TableMigrator<E, I> {
    pub fn new(extractor: E, inserter: I, registry: Arc<TypeRegistry>, options: MigrationOptions) -> Self {
        TableMigrator {
            extractor,
            inserter,
            registry,
            options,
        }
    }

    pub async fn migrate_table(&self, table_name: &str) -> Result<MigrationResult> {
        let output_table_name = if self.options.format_snake_case {
            format_snake_case(table_name)
        } else {
            table_name.to_string()
        };

        info!("Migrating table: {}", &output_table_name);

        let start_time = Instant::now();

        let table_schema = self
            .extractor
            .get_table_schema(table_name)
            .await
            .with_context(|| format!("Failed to get schema for table '{}'", table_name))?;

        let mapped_schema = TableSchemaMapper::map_schema(
            &self.registry,
            &table_schema,
            self.options.format_snake_case,
        )
        .with_context(|| format!("Failed to map schema for table '{}'", table_name))?;

        let table_exists = self
            .inserter
            .table_exists(&output_table_name)
            .await
            .with_context(|| {
                format!("Failed to check existence of table '{}'", output_table_name)
            })?;

        if table_exists {
            let count = self.inserter.table_rows_count(&output_table_name).await?;

            if count > 0 {
                return Err(MigrationError::TableAlreadyHasRows {
                    table: output_table_name,
                    count,
                }
                .into());
            }
        }

        if !table_exists {
            self.inserter
                .create_table(&output_table_name, &mapped_schema)
                .await
                .with_context(|| format!("Failed to create table '{}'", output_table_name))?;
        }

        let migrated_count = self
            .migrate_table_rows(table_name, &output_table_name, &mapped_schema)
            .await
            .with_context(|| format!("Failed to migrate rows for table '{}'", output_table_name))?;

        let end_time = Instant::now();
        info!(
            "Table {} migrated, rows: {}, took: {}s",
            &output_table_name,
            migrated_count,
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(MigrationResult {
            table_name: output_table_name,
            schema: mapped_schema,
            created: !table_exists,
        })
    }

    async fn migrate_table_rows(
        &self,
        input_table: &str,
        output_table: &str,
        mapped_schema: &[TargetColumn],
    ) -> Result<usize> {
        info!("Migrating {} rows", output_table);

        let insert_statement = build_insert_statement(output_table, mapped_schema);

        let mut stream = self.extractor.stream_rows(input_table).await?;

        let mut insert_query = String::with_capacity(self.options.max_packet_bytes);
        let mut total_bytes = insert_statement.len();
        let mut transaction_count = 0;
        let mut total_transaction_count = 0;

        while let Some(row_values) = stream.try_next().await? {
            let values = row_values.join(", ");
            let value_set = format!("({}) ", values);
            let value_set_bytes = value_set.len();

            if RESERVED_BYTES + total_bytes + value_set_bytes > self.options.max_packet_bytes {
                execute_batch(&self.inserter, &insert_query, transaction_count).await?;

                total_transaction_count += transaction_count;
                insert_query.clear();
                total_bytes = insert_statement.len();
                transaction_count = 0;
            }

            if !insert_query.is_empty() {
                insert_query.push(',');
                total_bytes += 1;
            }

            if transaction_count == 0 {
                insert_query.push_str(&insert_statement);
            }

            insert_query.push_str(&value_set);
            total_bytes += value_set_bytes;
            transaction_count += 1;
        }

        if transaction_count > 0 {
            execute_batch(&self.inserter, &insert_query, transaction_count).await?;
            total_transaction_count += transaction_count;
        }

        Ok(total_transaction_count)
    }
}

async fn execute_batch<I: Inserter>(
    inserter: &I,
    insert_query: &str,
    transaction_count: usize,
) -> Result<(), Error> {
    if !insert_query.is_empty() {
        let start_time = Instant::now();

        debug!(
            "Sending {} bytes batch with {} rows",
            insert_query.len(),
            transaction_count
        );

        inserter
            .execute_transactional_query(insert_query)
            .await
            .with_context(|| "Failed to execute transactional query batch")?;

        let end_time = Instant::now();

        debug!(
            "Executed batch with {} rows, bytes: {}, took: {}s",
            transaction_count,
            insert_query.len(),
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );
    }

    Ok(())
}
