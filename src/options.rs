use structopt::clap::AppSettings;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(
    name = "DBMigrator",
    version = "0.2.0",
    about = "A Rust project to migrate MSSQL databases to MySQL, including table structures, column data types, and table data rows.\n\nGitHub: https://github.com/bitalizer/db-migrator",
    setting = AppSettings::ColoredHelp,
)]
pub struct Options {
    /// Activate verbose mode
    #[structopt(short = "v", long = "verbose")]
    pub verbose: bool,
}
