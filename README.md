<p align="center">
  <h1 align="center">db-migrator</h1>
  <p align="center">A fast, type-safe MSSQL to MySQL database migration tool written in Rust.</p>
</p>

<p align="center">
  <img alt="Rust" src="https://img.shields.io/badge/rust-1.85+-orange.svg">
  <a href="https://github.com/bitalizer/db-migrator/releases"><img alt="Release" src="https://img.shields.io/github/v/release/bitalizer/db-migrator"></a>
  <a href="https://github.com/bitalizer/db-migrator/blob/master/LICENSE"><img alt="License" src="https://img.shields.io/github/license/bitalizer/db-migrator"></a>
</p>

<p align="center">
  <a href="#features">Features</a> &bull;
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#installation">Installation</a> &bull;
  <a href="#configuration">Configuration</a> &bull;
  <a href="#usage">Usage</a> &bull;
  <a href="#type-mappings">Type Mappings</a>
</p>

---

```
$ db-migrator --format --constraints --verbose

Migrating table: user_accounts
Table user_accounts migrated, rows: 12840, took: 1.23s
Migrating table: orders
Table orders migrated, rows: 58201, took: 3.47s
Migration finished, total rows: 71041, total time took: 4.82s
```

## Features

- **Schema + data migration** — transfers table structures, column types, constraints, and all rows
- **29 MSSQL types mapped** — built-in defaults for every type, zero config needed
- **Concurrent** — tables migrate in parallel with configurable parallelism
- **Batch inserts** — rows streamed and batched to respect MySQL `max_allowed_packet`
- **Constraints** — primary keys and foreign keys carried over automatically
- **Snake case** — optionally convert `PascalCase` names to `snake_case`
- **Customizable** — override any type mapping with a simple config file

## Quick Start

```shell
# 1. Configure your databases
cp config.example.toml config.toml    # edit connection details

# 2. Run
cargo run --release
```

That's it. All type mappings are built in — no extra config needed unless you want to [customize them](#type-mappings).

## Installation

### Pre-compiled Binaries

Download the latest binary for your platform from [Releases](https://github.com/bitalizer/db-migrator/releases).

### Build from Source

```shell
git clone https://github.com/bitalizer/db-migrator.git
cd db-migrator
cargo build --release
```

Requires Rust 1.85+ (edition 2024).

## Configuration

### Database Connections

Create `config.toml` from the example:

```toml
[mssql_database]
host = "localhost"   # optional, defaults shown
port = 1433
username = "db_user"
password = "db_pass"
database = "source_db"

[mysql_database]
host = "localhost"   # optional, defaults shown
port = 3306
username = "db_user"
password = "db_pass"
database = "target_db"

[settings]
max_packet_bytes = 1048576
whitelisted_tables = ["Users", "Orders", "Products"]
```

> **Character sets:** text is decoded from SQL Server to UTF-8 during extraction, and created
> tables inherit the target database's default charset. Create the target database with
> `utf8mb4` (`CREATE DATABASE output CHARACTER SET utf8mb4`) so all characters survive.
> Collation semantics are not translated: data from a case-sensitive SQL Server column can
> collide on MySQL's default case-insensitive unique indexes.


## Usage

```
db-migrator [FLAGS] [OPTIONS]
```

| Flag | Description |
|---|---|
| `-c, --constraints` | Create primary key and foreign key constraints |
| `-d, --drop` | Drop target tables before migration |
| `-f, --format` | Convert table and column names to snake_case |
| `-v, --verbose` | Verbose logging |
| `-q, --quiet` | Suppress output |

| Option | Description |
|---|---|
| `-p, --parallelism <N>` | Max concurrent table migrations *(default: CPU cores)* |

### Examples

```shell
# Basic migration
db-migrator

# Full migration with snake_case and constraints
db-migrator --format --constraints

# Verbose with custom parallelism
db-migrator -v -p 4

# Drop and recreate all tables
db-migrator --drop --constraints --format
```

## Type Mappings

All 29 MSSQL types are mapped by default. No configuration required.

<details>
<summary><strong>View full type mapping table</strong></summary>

<br>

| MSSQL | MySQL | Notes |
|---|---|---|
| `bit` | `tinyint(1)` | Boolean equivalent |
| `tinyint` | `tinyint unsigned` | |
| `smallint` | `smallint` | |
| `int` | `int` | |
| `bigint` | `bigint` | |
| `decimal` / `numeric` | `decimal` | Precision and scale carried over |
| `money` | `decimal(19, 4)` | |
| `smallmoney` | `decimal(10, 2)` | |
| `float` | `float` | |
| `real` | `real` | |
| `char` / `nchar` | `char` | Length carried from source |
| `varchar` | `varchar` | Length carried, default 255 |
| `nvarchar` | `longtext` | Unicode safe |
| `text` / `ntext` | `longtext` | |
| `binary` | `binary` | Length carried from source |
| `varbinary` / `image` | `longblob` | |
| `date` | `date` | |
| `datetime` / `datetime2` / `smalldatetime` | `datetime` | |
| `datetimeoffset` | `datetime` | Offset stripped |
| `time` | `time` | |
| `uniqueidentifier` | `char(36)` | UUID as string |
| `timestamp` | `bigint unsigned` | Row version counter, not a time value |
| `xml` | `longtext` | |

</details>

### Custom Overrides *(optional)*

To override any default mapping, create a `mappings.toml` file in the project root:

```toml
[mappings]
nvarchar = "varchar(500)"
money = "decimal(10, 2)"
float = "float(53)"
xml = "longtext"
```

Supports three formats: `"type"`, `"type(length)"`, and `"type(precision, scale)"`,
each optionally followed by the modifiers `unsigned` and/or `zerofill`
(numeric types only), e.g. `"int unsigned"` or `"decimal(19, 4) unsigned"`.

#### Column-scoped overrides

Type-wide overrides apply to every column of that source type. When only
specific columns need different output (e.g. a legacy schema expecting
`int unsigned` IDs), scope the override to `"Table.Column"` source names:

```toml
[mappings.columns]
"Orders.ID" = "int unsigned"
"Orders.Notes" = "text"
```

Column overrides take precedence over type-wide overrides, which take
precedence over the built-in defaults. Names are matched case-insensitively
against the source (SQL Server) table and column names.

## Requirements

- **Source:** MSSQL Server
- **Target:** MySQL 5.7+ / MariaDB 10.3+
- **Build:** Rust 1.85+

<details>
<summary><strong>Dependencies</strong></summary>

<br>

| Crate | Purpose |
|---|---|
| [tokio](https://docs.rs/tokio/1) | Async runtime |
| [tiberius](https://docs.rs/tiberius/0.12) | MSSQL driver |
| [sqlx](https://docs.rs/sqlx/0.8) | MySQL driver |
| [bb8](https://docs.rs/bb8/0.9) | Connection pooling |
| [clap](https://docs.rs/clap/4) | CLI parsing |
| [anyhow](https://docs.rs/anyhow/1.0) | Error handling |
| [chrono](https://docs.rs/chrono/0.4) | Date/time |
| [toml](https://docs.rs/toml/0.8) | Config parsing |

</details>

## License

See [LICENSE](LICENSE) for details.
