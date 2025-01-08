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
cargo build
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

Run the tool with:

```bash
cargo run
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
