use std::sync::Arc;

use anyhow::{anyhow, Context, Error, Result};
use futures::TryStreamExt;
use log::info;
use tokio::time::Instant;

use crate::common::helpers::format_snake_case;
use crate::common::schema::ColumnSchema;
use crate::extract::extractor::{open_row_stream, DatabaseExtractor};
use crate::insert::inserter::DatabaseInserter;
use crate::insert::query::build_insert_statement;
use crate::mappings::Mappings;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migration_result::MigrationResult;
use crate::migrate::table_schema_mapper::TableSchemaMapper;

const RESERVED_BYTES: usize = 10;

pub struct TableMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    mappings: Mappings,
    options: MigrationOptions,
}

impl TableMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        mappings: Mappings,
        options: MigrationOptions,
    ) -> Self {
        TableMigrator {
            extractor,
            inserter,
            mappings,
            options,
        }
    }

    pub async fn migrate_table(&mut self, table_name: &str) -> Result<MigrationResult> {
        let output_table_name = if self.options.format_snake_case {
            format_snake_case(table_name)
        } else {
            table_name.to_string()
        };

        info!("Migrating table: {}", &output_table_name);

        let start_time = Instant::now();

        // Fetch and map table schema
        let table_schema = self
            .extractor
            .get_table_schema(table_name)
            .await
            .with_context(|| "Failed to get table schema".to_string())?;

        let mapped_schema = TableSchemaMapper::map_schema(
            &self.mappings,
            &table_schema,
            self.options.format_snake_case,
        );

        let table_exists = self
            .inserter
            .table_exists(&output_table_name)
            .await
            .with_context(|| "Failed to check table existence".to_string())?;

        if table_exists {
            let count = self.inserter.table_rows_count(&output_table_name).await?;

            if count > 0 {
                return Err(anyhow!(
                    "Rows already exists in table {}",
                    &output_table_name
                ));
            }
        }

        if !table_exists {
            // Create table in the output database
            self.inserter
                .create_table(&output_table_name, &mapped_schema)
                .await
                .with_context(|| "Failed to create table".to_string())?;
        }

        // Migrate rows from input table to output table
        let migrated_count = self
            .migrate_table_rows(table_name, &output_table_name, &mapped_schema)
            .await
            .with_context(|| "Failed to migrate rows".to_string())?;

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
        &mut self,
        input_table: &str,
        output_table: &str,
        mapped_schema: &[ColumnSchema],
    ) -> Result<usize> {
        info!("Migrating {} rows", output_table);

        let insert_statement = build_insert_statement(output_table, mapped_schema);

        let mut conn = self.extractor.pool.get().await?;
        let mut stream = open_row_stream(&mut conn, input_table).await?;

        let mut insert_query = String::with_capacity(self.options.max_packet_bytes);
        let mut total_bytes = insert_statement.len();
        let mut transaction_count = 0;
        let mut total_transaction_count = 0;

        while let Some(row_values) = stream.try_next().await? {
            let values = row_values.join(", ");
            let value_set = format!("({}) ", values);
            let value_set_bytes = value_set.len();

            if RESERVED_BYTES + total_bytes + value_set_bytes > self.options.max_packet_bytes {
                execute_batch(&mut self.inserter, &insert_query, transaction_count).await?;

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
            // If there are remaining rows in the insert_query, execute them
            execute_batch(&mut self.inserter, &insert_query, transaction_count).await?;
            total_transaction_count += transaction_count;
        }

        Ok(total_transaction_count)
    }
}

async fn execute_batch(
    inserter: &mut DatabaseInserter,
    insert_query: &String,
    transaction_count: usize,
) -> Result<(), Error> {
    if !insert_query.is_empty() {
        let cloned_insert_query = Arc::new(insert_query.clone());

        let start_time = Instant::now();

        let query_str = cloned_insert_query.as_str();

        debug!(
            "Sending {} bytes batch with {} transactions",
            query_str.len(),
            transaction_count
        );

        inserter
            .execute_transactional_query(query_str)
            .await
            .with_context(|| "Failed to execute transactional query batch".to_string())?;

        let end_time = Instant::now();

        debug!(
            "Executed batch with {} transactions, bytes: {}, took: {}s",
            transaction_count,
            query_str.len(),
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );
    }

    Ok(())
}
