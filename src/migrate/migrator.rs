use std::sync::Arc;

use anyhow::{bail, Context, Error, Result};
use futures::future::join_all;
use log::info;
use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use crate::common::helpers::{format_snake_case, print_error_chain};
use crate::extract::extractor::DatabaseExtractor;
use crate::insert::inserter::DatabaseInserter;
use crate::insert::table_action::TableAction;
use crate::mappings::Mappings;
use crate::migrate::constraints_creator::ConstraintsCreator;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migration_result::MigrationResult;
use crate::migrate::table_migrator::TableMigrator;

pub struct DatabaseMigrator {
    extractor: DatabaseExtractor,
    inserter: DatabaseInserter,
    mappings: Mappings,
    options: MigrationOptions,
}

impl DatabaseMigrator {
    pub fn new(
        extractor: DatabaseExtractor,
        inserter: DatabaseInserter,
        mappings: Mappings,
        options: MigrationOptions,
    ) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            mappings,
            options,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Running table migrator");

        let config_send_packet_size = self.options.max_packet_bytes;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet).await?;

        self.migrate_tables().await?;

        Ok(())
    }

    pub async fn migrate_tables(&mut self) -> Result<()> {
        let start_time = Instant::now();

        let (tables, formatted_tables) = self.fetch_and_format_tables().await?;

        let action = if self.options.drop {
            TableAction::Drop
        } else {
            TableAction::Truncate
        };

        self.inserter
            .reset_tables(&formatted_tables, action)
            .await?;

        let migration_results = self.run_migration(tables).await;
        let (successful_results, errors) = process_migration_results(migration_results).await;

        // Handle errors
        for err in errors {
            print_error_chain(&err);
        }

        if self.options.constraints {
            let mut constraints_creator = ConstraintsCreator::new(self.inserter.clone());
            constraints_creator
                .run(successful_results, formatted_tables)
                .await;
        }

        let end_time = Instant::now();

        info!(
            "Migration finished, total time took: {}s",
            end_time.saturating_duration_since(start_time).as_secs_f32()
        );

        Ok(())
    }

    async fn fetch_and_format_tables(&mut self) -> Result<(Vec<String>, Vec<String>)> {
        let mut tables = self.extractor.fetch_tables().await?; // Fetch the list of tables from input database
        let formatted_tables = format_table_names(&tables, self.options.format_snake_case); // Format to snake case if required

        if tables.is_empty() {
            bail!("No tables to process");
        }

        check_missing_tables(&tables, &self.options.whitelisted_tables);

        // Filter and keep only the whitelisted tables
        tables.retain(|table| self.options.whitelisted_tables.contains(table));

        if tables.is_empty() {
            bail!("No tables to process after filtering whitelisted tables");
        }

        info!("Tables to migrate: {}", tables.join(", "));

        Ok((tables, formatted_tables))
    }

    async fn run_migration(&mut self, tables: Vec<String>) -> Vec<Result<MigrationResult, Error>> {
        // Create a semaphore to limit the number of concurrent tasks
        let semaphore = Arc::new(Semaphore::new(self.options.max_concurrent_tasks));

        // Create a Vec to store the JoinHandles for tasks
        let mut migration_tasks = Vec::new();

        // Spawn a task for each table to fetch the rows concurrently
        for table in tables {
            // Clone the shared semaphore for each task
            let semaphore_clone = Arc::clone(&semaphore);

            let extractor = self.extractor.clone();
            let inserter = self.inserter.clone();
            let mappings = self.mappings.clone();
            let options = self.options.clone();

            // Spawn a task for each table
            let task = spawn(async move {
                // Acquire a semaphore permit before starting the task
                let permit = semaphore_clone
                    .acquire()
                    .await
                    .expect("Failed to acquire semaphore permit");

                let mut table_migrator = TableMigrator::new(extractor, inserter, mappings, options);

                let result = table_migrator
                    .migrate_table(&table)
                    .await
                    .with_context(|| format!("Error while migrating table: {}", table));

                // Release the semaphore permit when the task is done (whether successful or not)
                drop(permit);
                result
            });

            migration_tasks.push(task);
        }

        let migration_results: Vec<Result<MigrationResult, Error>> = join_all(migration_tasks)
            .await
            .into_iter()
            .map(|join_handle_result| join_handle_result.expect("Error in JoinHandle"))
            .collect();

        migration_results
    }
}

async fn check_packet_size(
    config_send_packet_size: usize,
    max_allowed_packet: usize,
) -> Result<()> {
    debug!(
        "Max allowed packet size - Current: {} MB | Maximum {} MB",
        config_send_packet_size as f64 / 1_048_576.0,
        max_allowed_packet as f64 / 1_048_576.0
    );

    if config_send_packet_size > max_allowed_packet {
        bail!("Configured send packet size exceeds maximum allowed packet size")
    }

    Ok(())
}

fn check_missing_tables(tables: &[String], whitelisted_tables: &[String]) {
    // Check for missing tables in whitelisted_tables
    let missing_tables: Vec<_> = whitelisted_tables
        .iter()
        .filter(|table| !tables.contains(table))
        .cloned()
        .collect();

    // If there are missing tables, print a warning
    if !missing_tables.is_empty() {
        let missing_tables_str = missing_tables.join(", ");
        warn!(
            "The following whitelisted tables were not found in the database: {}",
            missing_tables_str
        );
    }
}

fn format_table_names(tables: &[String], format: bool) -> Vec<String> {
    if format {
        tables
            .iter()
            .map(|table_name| format_snake_case(table_name))
            .collect()
    } else {
        tables.to_vec()
    }
}

// Helper function to process migration results and separate successful results from errors
async fn process_migration_results(
    migration_results: Vec<Result<MigrationResult, Error>>,
) -> (Vec<MigrationResult>, Vec<Error>) {
    let (successful_results, errors): (Vec<_>, Vec<_>) =
        migration_results.into_iter().partition(Result::is_ok);

    let successful_results: Vec<MigrationResult> =
        successful_results.into_iter().map(Result::unwrap).collect();

    (
        successful_results,
        errors.into_iter().map(Result::unwrap_err).collect(),
    )
}
