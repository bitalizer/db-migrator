use anyhow::Context;
use futures::future::join_all;
use tokio::spawn;

use crate::common::helpers::print_error_chain;
use crate::insert::inserter::DatabaseInserter;
use crate::migrate::migration_result::MigrationResult;

pub struct ConstraintsCreator {
    inserter: DatabaseInserter,
}

impl ConstraintsCreator {
    pub fn new(inserter: DatabaseInserter) -> Self {
        ConstraintsCreator { inserter }
    }

    pub async fn run(
        &mut self,
        successful_results: Vec<MigrationResult>,
        formatted_tables: Vec<String>,
    ) {
        let tasks = successful_results
            .into_iter()
            .filter(|migration_result| migration_result.created)
            .map(|migration_result| {
                let mut inserter = self.inserter.clone();
                let formatted_tables = formatted_tables.clone();
                let table_name = migration_result.table_name.clone();
                let schema = migration_result.schema;

                spawn(async move {
                    if let Err(err) = inserter
                        .create_constraints(&table_name, &schema, &formatted_tables)
                        .await
                        .with_context(|| {
                            format!("Error while creating constraints for table: {}", table_name)
                        })
                    {
                        print_error_chain(&err);
                    }
                })
            })
            .collect::<Vec<_>>();

        join_all(tasks).await;
    }
}
