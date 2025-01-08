# SQL Schema Generator and Foreign Key Join Test Tool

A Rust-based tool for generating random SQL database schemas and testing foreign key join queries. This tool is designed to create complex database schemas with configurable constraints and generate test cases for foreign key join operations.

## Theoretical Background

This tool implements and tests algorithms for verifying foreign key join properties in SQL queries. The core focus is on determining which relations in a query preserve two critical properties:

1. **Row Preservation (Set A)**: Tracks which relations maintain all their original rows through a sequence of joins. This is crucial for ensuring referential integrity and complete data representation.

2. **Uniqueness (Set U)**: Monitors which relations maintain uniqueness of their key columns throughout join operations. This property is essential for foreign key relationships to remain valid.

The algorithm processes each join operation sequentially, updating these sets based on:
- Join type (INNER, LEFT, RIGHT, FULL)
- Foreign key direction (→ or ←)
- Column constraints (NOT NULL, UNIQUE)

## Algorithm

The core algorithm for verifying properties of foreign key joins is as follows:

```python
def verify_derived_table(query):
    if not query:
        return set(), set()

    # The first item is a relation name (not a join tuple) and
    # begins in both sets since it has not joined with anything yet.
    A = { query[0] }
    U = { query[0] }

    # Subsequent elements are 6-tuples containing join metadata.
    for (join_type, fk_dir, fk_cols_not_null, fk_cols_unique, fk_rel, pk_rel) in query[1:]:
        match fk_dir:
            case "<-":
                existing_rel, new_rel = pk_rel, fk_rel
            case "->":
                existing_rel, new_rel = fk_rel, pk_rel

        cond = (existing_rel in A and fk_cols_not_null)
        match (join_type, fk_dir, cond):
            case ("LEFT", "<-", _):
                pass
            case ("LEFT", "->", False):
                pass
            case ("LEFT", "->", True):
                A = A | { new_rel }
            case ("FULL", _, _):
                A = A | { new_rel }
            case ("INNER", _, False):
                A = set()
            case ("INNER", "<-", True):
                A = { existing_rel }
            case ("INNER", "->", True):
                A = { new_rel }
            case ("RIGHT", "<-", False):
                A = { new_rel }
            case ("RIGHT", "<-", True):
                A = { existing_rel, new_rel }
            case ("RIGHT", "->", _):
                A = { new_rel }
        
        if (existing_rel in U) and fk_cols_unique:
            U = U | { new_rel }
        elif (existing_rel not in U) and fk_cols_unique:
            pass
        elif (fk_dir == "<-") and not ((existing_rel in U) and fk_cols_unique):
            pass
        elif (fk_dir == "->") and (existing_rel not in U) and (not fk_cols_unique):
            U = set()
        elif (fk_dir == "->") and (existing_rel in U) and (not fk_cols_unique):
            U = { new_rel }
        
    return A, U
```

The algorithm takes a query representation consisting of:
1. The first relation (table) in the query
2. A sequence of joins, each containing:
   - Join type (INNER, LEFT, RIGHT, FULL)
   - Foreign key direction (← or →)
   - Whether foreign key columns are NOT NULL
   - Whether foreign key columns are UNIQUE
   - The foreign key relation
   - The primary key relation

It maintains two sets:
- Set A: Relations whose rows are all preserved in the result
- Set U: Relations whose rows appear at most once in the result

For each join, it updates these sets based on:
1. The join type and direction
2. The NOT NULL and UNIQUE constraints on the foreign key columns
3. Whether the existing relation is in sets A and U

## Experimental Validation

The tool serves as an experimental validator for the theoretical algorithm by:
1. Generating random schemas and data that satisfy foreign key constraints
2. Building complex queries using only foreign key joins
3. Executing these queries and analyzing their result sets
4. Comparing the actual properties of relations in the results with those predicted by the theoretical model

While successful test runs cannot prove the theoretical model correct, any discrepancy between predicted and actual results would demonstrate incorrectness of the theoretical model.

### Failure Conditions

The program aborts execution when it detects:
1. A relation predicted to preserve all rows (in set A) actually loses rows in the result set
2. A relation predicted to maintain uniqueness (in set U) contains duplicates in the result set
3. A relation not in set A is found to preserve all rows
4. A relation not in set U maintains uniqueness unexpectedly

These conditions indicate potential flaws in the theoretical model and are reported with detailed information about:
- The specific query that triggered the failure
- The predicted vs. actual properties
- The data that led to the violation

## Features

- Generates random SQL tables with configurable:
  - Number of tables and columns
  - Primary key constraints
  - Foreign key relationships
  - Unique and NOT NULL constraints
- Creates test data with controlled distribution
- Generates and validates foreign key join queries
- Supports multiple join types (INNER, LEFT, RIGHT, FULL)
- Verifies query correctness through view creation

## Prerequisites

- Rust (2021 edition)
- PostgreSQL database server
- Cargo package manager

## Installation

1. Clone the repository
2. Install dependencies:
```bash
cargo build --release
```

## Configuration

The tool's behavior can be customized through constants in `src/main.rs`:

- `NUM_TABLES`: Total number of tables to generate
- `AVG_COLUMNS`: Average number of columns per table
- `MAIN_KEY_PK_PROB`: Probability of main key being PRIMARY KEY
- `FOREIGN_KEY_NOT_NULL_PROB`: Probability of foreign keys being NOT NULL
- `MAX_ROWS_PER_TABLE`: Maximum number of rows per table
- And many more configuration options

## Usage

Run the tool with default settings:

```bash
createdb joinfuzzer
cargo run
```

Or specify custom parameters:

```bash
./target/release/sql-join-fuzzer --join-syntax KEY
```

### Command Line Options

```
SQL Schema Generator and Foreign Key Join Test Tool

Usage: sql-join-fuzzer [OPTIONS]

Options:
      --num-tables <NUM_TABLES>
          Number of tables to generate [default: 5]
      --avg-columns <AVG_COLUMNS>
          Average number of columns per table [default: 10]
      --extra-columns-range <EXTRA_COLUMNS_RANGE>
          +/- range around the average number of columns [default: 3]
      --additional-unique-prob <ADDITIONAL_UNIQUE_PROB>
          Probability that any additional column is UNIQUE [default: 0.3]
      --additional-unique-not-null-prob <ADDITIONAL_UNIQUE_NOT_NULL_PROB>
          Probability that a UNIQUE column is NOT NULL [default: 0.5]
      --foreign-key-not-null-prob <FOREIGN_KEY_NOT_NULL_PROB>
          Probability that a foreign key column is NOT NULL [default: 0.5]
      --foreign-key-unique-prob <FOREIGN_KEY_UNIQUE_PROB>
          Probability that a foreign key column is UNIQUE [default: 0.1]
      --foreign-key-max-percent <FOREIGN_KEY_MAX_PERCENT>
          Maximum percentage of foreign key columns relative to previous tables [default: 50]
      --max-rows-per-table <MAX_ROWS_PER_TABLE>
          Maximum number of rows per table [default: 1000]
      --foreign-key-reuse-prob <FOREIGN_KEY_REUSE_PROB>
          Probability of reusing an existing foreign key value [default: 0.5]
      --table-reuse-prob <TABLE_REUSE_PROB>
          Probability of reusing a table with a new alias [default: 0.1]
      --foreign-key-null-prob <FOREIGN_KEY_NULL_PROB>
          Probability of NULL for nullable foreign key columns [default: 0.2]
      --db-host <DB_HOST>
          Database host [default: localhost]
      --db-port <DB_PORT>
          Database port [default: 5432]
      --db-name <DB_NAME>
          Database name [default: joinfuzzer]
      --db-user <DB_USER>
          Database user
      --db-password <DB_PASSWORD>
          Database password [default: ]
      --join-syntax <JOIN_SYNTAX>
          Join syntax to use: 'KEY' for foreign key syntax or 'ON' for traditional syntax [default: ON]
  -h, --help
          Print help
  -V, --version
          Print version
```

The tool will:
1. Generate a random schema
2. Create tables with appropriate constraints
3. Populate tables with test data
4. Generate and test foreign key join queries
5. Verify query correctness

## Dependencies

- `rand`: Random number generation
- `postgres`: PostgreSQL database connectivity

## License

PostgreSQL License

### Join Syntax Examples

The tool supports two join syntax styles:

1. Traditional ON syntax (default):
```sql
LEFT JOIN table2 AS t2 ON t2.fk_col = t1.pk_col
```

2. KEY syntax (proposed SQL feature):
```sql
LEFT JOIN table2 AS t2 KEY (fk_col) -> t1 (pk_col)
```

Select the syntax style using the `--join-syntax` option with either `ON` or `KEY`.
