use std::sync::Arc;

use anyhow::{Context, Error, Result, bail};
use log::info;
use tokio::spawn;
use tokio::sync::{Semaphore, watch};
use tokio::time::Instant;

use crate::common::errors::MigrationError;
use crate::common::helpers::format_snake_case;
use crate::extract::traits::Extractor;
use crate::insert::table_action::TableAction;
use crate::insert::traits::Inserter;
use crate::mappings::Mappings;
use crate::migrate::constraints_creator::ConstraintsCreator;
use crate::migrate::migration_options::MigrationOptions;
use crate::migrate::migration_result::MigrationResult;
use crate::migrate::table_migrator::TableMigrator;

pub struct DatabaseMigrator<E: Extractor, I: Inserter> {
    extractor: E,
    inserter: I,
    mappings: Mappings,
    options: MigrationOptions,
}

impl<E: Extractor, I: Inserter> DatabaseMigrator<E, I> {
    pub fn new(extractor: E, inserter: I, mappings: Mappings, options: MigrationOptions) -> Self {
        DatabaseMigrator {
            extractor,
            inserter,
            mappings,
            options,
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Running table migrator");

        let config_send_packet_size = self.options.max_packet_bytes;
        let max_allowed_packet = self.inserter.get_max_allowed_packet().await?;

        check_packet_size(config_send_packet_size, max_allowed_packet)?;

        self.migrate_tables().await?;

        Ok(())
    }

    pub async fn migrate_tables(&self) -> Result<()> {
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

        let successful_results = self.run_migration(&tables).await?;

        if self.options.constraints {
            let constraints_creator = ConstraintsCreator::new(self.inserter.clone());
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

    async fn fetch_and_format_tables(&self) -> Result<(Vec<String>, Vec<String>)> {
        let mut tables = self.extractor.fetch_tables().await?;

        if tables.is_empty() {
            bail!("No tables to process");
        }

        check_missing_tables(&tables, &self.options.whitelisted_tables);

        tables.retain(|table| self.options.whitelisted_tables.contains(table));

        if tables.is_empty() {
            bail!("No tables to process after filtering whitelisted tables");
        }

        let formatted_tables = format_table_names(&tables, self.options.format_snake_case);

        info!("Tables to migrate: {}", tables.join(", "));

        Ok((tables, formatted_tables))
    }

    async fn run_migration(&self, tables: &[String]) -> Result<Vec<MigrationResult>> {
        let semaphore = Arc::new(Semaphore::new(self.options.max_concurrent_tasks));

        let (cancel_tx, cancel_rx) = watch::channel(false);

        let mut migration_tasks = Vec::new();
        let mut successful_results = Vec::new();

        for table in tables {
            let semaphore_clone = Arc::clone(&semaphore);
            let mut cancel_rx = cancel_rx.clone();

            let extractor = self.extractor.clone();
            let inserter = self.inserter.clone();
            let mappings = self.mappings.clone();
            let options = self.options.clone();
            let table = table.clone();
            let table_name = table.clone();

            let task = spawn(async move {
                let permit = tokio::select! {
                    permit = semaphore_clone.acquire() => {
                        permit.map_err(|_| anyhow::anyhow!("Semaphore closed"))?
                    }
                    _ = cancel_rx.changed() => {
                        return Ok(None);
                    }
                };

                if *cancel_rx.borrow() {
                    return Ok(None);
                }

                let table_migrator = TableMigrator::new(extractor, inserter, mappings, options);

                let result = table_migrator
                    .migrate_table(&table)
                    .await
                    .with_context(|| format!("Error while migrating table: {}", table));

                drop(permit);
                result.map(Some)
            });

            migration_tasks.push((table_name, task));
        }

        let mut first_error: Option<Error> = None;
        let mut skipped_tables: Vec<String> = Vec::new();

        for (table_name, task) in migration_tasks {
            let join_result = task.await;

            match join_result {
                Ok(Ok(Some(migration_result))) => {
                    successful_results.push(migration_result);
                }
                Ok(Ok(None)) => {
                    skipped_tables.push(table_name);
                }
                Ok(Err(err)) => {
                    if first_error.is_none() {
                        let _ = cancel_tx.send(true);
                        first_error = Some(err);
                    }
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        let _ = cancel_tx.send(true);
                        first_error = Some(anyhow::anyhow!(MigrationError::TaskPanicked {
                            table: join_err.to_string(),
                        }));
                    }
                }
            }
        }

        if let Some(err) = first_error {
            if !skipped_tables.is_empty() {
                warn!(
                    "Migration aborted. Skipped {} remaining table(s): {}",
                    skipped_tables.len(),
                    skipped_tables.join(", ")
                );
            }
            return Err(err);
        }

        Ok(successful_results)
    }
}

fn check_packet_size(config_send_packet_size: usize, max_allowed_packet: usize) -> Result<()> {
    debug!(
        "Max allowed packet size - Current: {} MB | Maximum {} MB",
        config_send_packet_size as f64 / 1_048_576.0,
        max_allowed_packet as f64 / 1_048_576.0
    );

    if config_send_packet_size > max_allowed_packet {
        bail!(MigrationError::PacketSizeTooLarge {
            configured: config_send_packet_size,
            maximum: max_allowed_packet,
        })
    }

    Ok(())
}

fn check_missing_tables(tables: &[String], whitelisted_tables: &[String]) {
    let missing_tables: Vec<_> = whitelisted_tables
        .iter()
        .filter(|table| !tables.contains(table))
        .cloned()
        .collect();

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
