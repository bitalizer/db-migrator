use std::num::NonZeroUsize;
use std::sync::LazyLock;
use std::thread::available_parallelism;

use clap::Parser;

static DEFAULT_PARALLELISM: LazyLock<String> =
    LazyLock::new(|| get_default_parallelism().to_string());

#[derive(Debug, Parser)]
#[command(
    name = "DBMigrator",
    version = env!("CARGO_PKG_VERSION"),
    about = "A Rust project to migrate MSSQL databases to MySQL, including table structures, column data types, constraints and table data rows.\n\nGitHub: https://github.com/bitalizer/db-migrator",
)]
pub struct Args {
    /// Activate verbose mode
    #[arg(short = 'v', long = "verbose")]
    pub verbose: bool,

    /// Activate quiet mode
    #[arg(short = 'q', long = "quiet")]
    pub quiet: bool,

    /// Drop tables before migration
    #[arg(short = 'd', long = "drop")]
    pub drop: bool,

    /// Create constraints
    #[arg(short = 'c', long = "constraints")]
    pub constraints: bool,

    /// Format snake case table and column names
    #[arg(short = 'f', long = "format")]
    pub format: bool,

    /// Set parallelism
    #[arg(short = 'p', long = "parallelism", default_value = DEFAULT_PARALLELISM.as_str())]
    pub parallelism: usize,
}

fn get_default_parallelism() -> usize {
    available_parallelism()
        .unwrap_or(NonZeroUsize::new(4).expect("4 is non-zero"))
        .get()
}
