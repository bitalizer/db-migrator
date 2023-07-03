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
- [chrono](https://docs.rs/chrono/0.4) - Date and time library for Rust.
- [toml](https://docs.rs/toml/0.7) - TOML parsing and serialization library for Rust.
- [tiberius](https://docs.rs/tiberius/0.12) - MSSQL database driver for Rust.
- [sqlx](https://docs.rs/sqlx/0.6) - Database toolkit for Rust, including support for MySQL.

## Usage

1. Copy the `config.example.toml` file to `config.toml`.
2. Configure the connection details and whitelisted tables for the MSSQL and MySQL databases in the `config.toml` file.
3. Customize the table mappings and migration options in the `mappings.toml` file.
4. Build and run the migration tool using Cargo:
```shell
cargo run --release
```
## Installation

Make sure you have Rust installed. You can install Rust from the official website: https://www.rust-lang.org/tools/install

Clone the repository:

```shell
git clone https://github.com/bitalizer/db-migrator.git
```