# MSSQL to MySQL Database Migration

![Rust Version](https://img.shields.io/badge/rust-1.61.0-orange.svg)
![License](https://img.shields.io/github/license/bitalizer/db-migrator)

A Rust project to migrate MSSQL databases to MySQL, including table structures, column data types, and table data rows.

## Features

- Connects to MSSQL and MySQL databases to perform the migration.
- Converts MSSQL table structures and column data types to their corresponding MySQL equivalents.
- Transfers table data rows from MSSQL to MySQL.
- Provides flexibility in configuring connection details, table mappings, and migration options.
- Handles differences in data types, constraints, and other database-specific details during the migration process.

## Dependencies

- [tokio](https://docs.rs/tokio/1) - Asynchronous runtime for Rust.
- [tokio-util](https://docs.rs/tokio-util/0.7) - Utilities for working with Tokio.
- [anyhow](https://docs.rs/anyhow/1.0) - Rust error handling library.
- [log](https://docs.rs/log/0.4) - Logging facade for Rust.
- [env_logger](https://docs.rs/env_logger/0.10) - Environment logger for Rust.
- [structopt](https://docs.rs/structopt/0.3) - Parse command line arguments in Rust.
- [chrono](https://docs.rs/chrono/0.4) - Date and time library for Rust.
- [toml](https://docs.rs/toml/0.7) - TOML parsing and serialization library for Rust.
- [async-trait](https://docs.rs/async-trait/0.1) - Async versions of Rust's trait objects.
- [hex](https://docs.rs/hex/0.4) - Hexadecimal encoding and decoding for Rust.
- [futures](https://docs.rs/futures/0.3) - Asynchronous programming using Rust's futures.
- [tiberius](https://docs.rs/tiberius/0.12) - MSSQL database driver for Rust.
- [bb8](https://docs.rs/bb8/0.8) - Connection pool for Rust.
- [bb8-tiberius](https://docs.rs/bb8-tiberius/0.15) - BB8 support for Tiberius.
- [sqlx](https://docs.rs/sqlx/0.6) - Database toolkit for Rust, including support for MySQL.

## Usage

### Option 1: Compile and Run

1. Copy the `config.example.toml` file to `config.toml`.
2. Configure the connection details and whitelisted tables for the MSSQL and MySQL databases in the `config.toml` file.
3. Customize the table mappings and migration options in the `mappings.toml` file.
4. Build and run the migration tool using Cargo: 
```shell
cargo run --release
```

### Option 2: Use Pre-compiled Binaries

1. Go to the [GitHub Releases page](https://github.com/bitalizer/db-migrator/releases) of this repository.
2. Download the appropriate pre-compiled binary for your operating system and architecture.
3. Copy the `config.example.toml` file to `config.toml`.
4. Configure the connection details and whitelisted tables for the MSSQL and MySQL databases in the `config.toml` file.

### Arguments

```shell
USAGE:
    db-migrator.exe [FLAGS] [OPTIONS]

FLAGS:
    -c, --constraints    Create constraints
    -d, --drop           Drop tables before migration
    -f, --format         Format snake case table and column names
    -h, --help           Prints help information
    -q, --quiet          Activate quiet mode
    -V, --version        Prints version information
    -v, --verbose        Activate verbose mode

OPTIONS:
    -p, --parallelism <parallelism>    Set parallelism [default: LOGICAL_CORES]


```

## Installation

Make sure you have Rust installed. You can install Rust from the official
website: https://www.rust-lang.org/tools/install

Clone the repository:

```shell
git clone https://github.com/bitalizer/db-migrator.git
```