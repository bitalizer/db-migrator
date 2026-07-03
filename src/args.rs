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

    /// Source database URL: mssql://user:pass@host:1433/database.
    /// When --source/--target/--tables are given, config.toml is not read.
    #[arg(long = "source", value_name = "URL")]
    pub source: Option<String>,

    /// Target database URL: mysql://user:pass@host:3306/database
    #[arg(long = "target", value_name = "URL")]
    pub target: Option<String>,

    /// Comma-separated list of tables to migrate (skips config.toml)
    #[arg(long = "tables", value_name = "TABLE1,TABLE2")]
    pub tables: Option<String>,

    /// Maximum INSERT batch size in bytes (default 1048576; overrides config.toml)
    #[arg(long = "max-packet-bytes", value_name = "BYTES")]
    pub max_packet_bytes: Option<usize>,
}

fn get_default_parallelism() -> usize {
    available_parallelism()
        .unwrap_or(NonZeroUsize::new(4).expect("4 is non-zero"))
        .get()
}
