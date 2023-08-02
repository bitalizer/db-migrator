use std::thread::available_parallelism;

use structopt::clap::AppSettings;
use structopt::lazy_static::lazy_static;
use structopt::StructOpt;

lazy_static! {
    static ref DEFAULT_PARALLELISM: String = get_default_parallelism().to_string();
}

#[derive(Debug, StructOpt)]
#[structopt(
name = "DBMigrator",
version = env ! ("CARGO_PKG_VERSION"),
about = "A Rust project to migrate MSSQL databases to MySQL, including table structures, column data types, constraints and table data rows.\n\nGitHub: https://github.com/bitalizer/db-migrator",
setting = AppSettings::ColoredHelp,
)]
pub struct Args {
    /// Activate verbose mode
    #[structopt(short = "v", long = "verbose")]
    pub verbose: bool,

    /// Activate quiet mode
    #[structopt(short = "q", long = "quiet")]
    pub quiet: bool,

    /// Drop tables before migration
    #[structopt(short = "d", long = "drop")]
    pub drop: bool,

    /// Create constraints
    #[structopt(short = "c", long = "constraints")]
    pub constraints: bool,

    /// Format snake case table and column names
    #[structopt(short = "f", long = "format")]
    pub format: bool,

    /// Set parallelism
    #[structopt(short = "p", long = "parallelism", default_value = & DEFAULT_PARALLELISM.as_str())]
    pub parallelism: usize,
}

fn get_default_parallelism() -> usize {
    available_parallelism().unwrap().get()
}
