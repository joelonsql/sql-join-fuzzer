use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use rand::seq::SliceRandom;
use std::sync::atomic::{AtomicUsize, Ordering};
use postgres::{Client, NoTls};
use std::env;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH, Instant};
use clap::Parser;
use rusqlite::Connection;
use uuid::Uuid;
use std::fs;
use std::error::Error;
use ctrlc;
use std::str::FromStr;

mod fk_join_test;
use fk_join_test::handle_fk_join_test;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of tables to generate
    #[arg(long, default_value_t = 5)]
    num_tables: usize,

    /// Average number of columns per table
    #[arg(long, default_value_t = 10)]
    avg_columns: usize,

    /// +/- range around the average number of columns
    #[arg(long, default_value_t = 3)]
    extra_columns_range: usize,

    /// Probability that any additional column is UNIQUE
    #[arg(long, default_value_t = 0.3)]
    additional_unique_prob: f64,

    /// Probability that a UNIQUE column is NOT NULL
    #[arg(long, default_value_t = 0.5)]
    additional_unique_not_null_prob: f64,

    /// Probability that a foreign key column is NOT NULL
    #[arg(long, default_value_t = 0.5)]
    foreign_key_not_null_prob: f64,

    /// Probability that a foreign key column is UNIQUE
    #[arg(long, default_value_t = 0.1)]
    foreign_key_unique_prob: f64,

    /// Maximum percentage of foreign key columns relative to previous tables
    #[arg(long, default_value_t = 50)]
    foreign_key_max_percent: usize,

    /// Maximum number of rows per table
    #[arg(long, default_value_t = 1000)]
    max_rows_per_table: usize,

    /// Probability of reusing an existing foreign key value
    #[arg(long, default_value_t = 0.5)]
    foreign_key_reuse_prob: f64,

    /// Probability of reusing a table with a new alias
    #[arg(long, default_value_t = 0.1)]
    table_reuse_prob: f64,

    /// Probability of NULL for nullable foreign key columns
    #[arg(long, default_value_t = 0.2)]
    foreign_key_null_prob: f64,

    /// Database host
    #[arg(long, default_value = "localhost")]
    db_host: String,

    /// Database port
    #[arg(long, default_value_t = 5432)]
    db_port: u16,

    /// Database name
    #[arg(long, default_value = "joinfuzzer")]
    db_name: String,

    /// Database user
    #[arg(long)]
    db_user: Option<String>,

    /// Database password
    #[arg(long, default_value = "")]
    db_password: String,

    /// Join syntax to use: 'KEY' for foreign key syntax or
    /// 'ON' for traditional syntax
    #[arg(long, default_value = "ON")]
    join_syntax: String,

    /// Statement timeout in milliseconds
    #[arg(long, default_value_t = 5000)]
    statement_timeout_ms: u32,

    /// Minimum number of joins to generate
    #[arg(long, default_value_t = 1)]
    min_num_joins: usize,

    /// Maximum number of joins to generate
    #[arg(long, default_value_t = 5)]
    max_num_joins: usize,

    /// Databases to test (pg/postgres/postgresql, sqlite)
    #[arg(long, value_delimiter = ',', value_parser = Database::from_str, default_value = "pg")]
    test_dbs: Vec<Database>,
}

static GLOBAL_COLUMN_COUNTER: AtomicUsize = AtomicUsize::new(1);

fn get_next_column_name() -> String {
    let column_num = GLOBAL_COLUMN_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("c{}", column_num)
}

struct ColumnSequence {
    next_value: i32,
    used_values: Vec<i32>,
    used_in_current_table: Vec<i32>,  // Track values already used in current table for UNIQUE constraints
}

impl ColumnSequence {
    fn new() -> Self {
        Self {
            next_value: 1,
            used_values: Vec::new(),
            used_in_current_table: Vec::new(),
        }
    }

    fn next(&mut self) -> i32 {
        let value = self.next_value;
        self.next_value += 1;
        self.used_values.push(value);
        self.used_in_current_table.push(value);
        value
    }

    fn clear_current_table(&mut self) {
        self.used_in_current_table.clear();
    }
}

// Add this struct to track successful inserts
struct TableData {
    sequences: std::collections::HashMap<String, Vec<i32>>,  // column_name -> actual inserted values
}

fn generate_insert_statements(tables: &[Table], rng: &mut impl Rng, output: &mut Vec<String>, args: &Args) -> Result<(), String> {
    let mut sequences: std::collections::HashMap<(String, String), ColumnSequence> = std::collections::HashMap::new();
    let mut table_data: std::collections::HashMap<String, TableData> = std::collections::HashMap::new();

    // Initialize sequences and table data
    for table in tables {
        for column in &table.columns {
            if column.is_unique || column.is_primary_key {
                sequences.insert(
                    (table.name.clone(), column.name.clone()),
                    ColumnSequence::new()
                );
            }
        }
        table_data.insert(table.name.clone(), TableData {
            sequences: std::collections::HashMap::new(),
        });
    }

    for table in tables {
        output.push(format!("\n-- INSERT statements for table {}", table.name));

        for sequence in sequences.values_mut() {
            sequence.clear_current_table();
        }

        let target_rows = rng.gen_range(1..=args.max_rows_per_table);
        let mut successful_inserts = 0;

        'row_loop: for _ in 0..target_rows {
            let mut values = Vec::new();
            let mut row_data: std::collections::HashMap<String, i32> = std::collections::HashMap::new();

            for column in &table.columns {
                let value = if column.is_foreign_key {
                    if !column.is_not_null && rng.gen_bool(args.foreign_key_null_prob) {
                        -1  // Use -1 to represent NULL
                    } else {
                        let (ref_table, ref_col) = column.reference.as_ref().unwrap();
                        let ref_table_data = table_data.get(ref_table).unwrap();

                        // Create longer-lived empty vectors
                        let empty_vec = Vec::new();
                        let available_values = ref_table_data.sequences.get(ref_col).unwrap_or(&empty_vec);
                        if available_values.is_empty() {
                            continue 'row_loop;
                        }

                        if column.is_unique {
                            let empty_vec = Vec::new();
                            let used_values = table_data.get(&table.name)
                                .and_then(|td| td.sequences.get(&column.name))
                                .unwrap_or(&empty_vec);

                            let available: Vec<_> = available_values.iter()
                                .filter(|v| !used_values.contains(v))
                                .collect();

                            if available.is_empty() {
                                continue 'row_loop;
                            }
                            **available.choose(rng).unwrap()
                        } else {
                            // For non-unique foreign keys, maybe reuse an existing value
                            let current_table_data = table_data.get(&table.name);
                            let current_values = current_table_data
                                .and_then(|td| td.sequences.get(&column.name))
                                .unwrap_or(&empty_vec);

                            if !current_values.is_empty() && rng.gen_bool(args.foreign_key_reuse_prob) {
                                // Reuse an existing value
                                *current_values.choose(rng).unwrap()
                            } else {
                                // Use a new value
                                *available_values.choose(rng).unwrap()
                            }
                        }
                    }
                } else if column.is_unique || column.is_primary_key {
                    let sequence = sequences.get_mut(&(table.name.clone(), column.name.clone())).unwrap();
                    sequence.next()
                } else if column.is_not_null {
                    rng.gen_range(1..1000)
                } else {
                    if rng.gen_bool(0.2) {
                        -1
                    } else {
                        rng.gen_range(1..1000)
                    }
                };

                values.push(value);
                if value != -1 {
                    row_data.insert(column.name.clone(), value);
                }
            }

            let column_names: Vec<String> = table.columns.iter()
                .map(|c| c.name.clone())
                .collect();

            output.push(format!("INSERT INTO {} ({}) VALUES ({});",
                table.name,
                column_names.join(", "),
                values.iter()
                    .map(|&v| if v == -1 { "NULL".to_string() } else { v.to_string() })
                    .collect::<Vec<_>>()
                    .join(", ")));

            let table_data = table_data.get_mut(&table.name).unwrap();
            for (col_name, value) in row_data {
                table_data.sequences.entry(col_name)
                    .or_insert_with(Vec::new)
                    .push(value);
            }

            successful_inserts += 1;
        }

        output.push(format!("-- Generated {} out of {} attempted rows for {}",
            successful_inserts, target_rows, table.name));
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    is_not_null: bool,
    is_unique: bool,
    is_primary_key: bool,
    is_foreign_key: bool,
    // (referenced_table, referenced_column)
    reference: Option<(String, String)>,
}

impl Table {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: vec![],
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    fn as_str(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER",
            JoinType::Left => "LEFT",
            JoinType::Right => "RIGHT",
            JoinType::Full => "FULL",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FKDir {
    Backward,  // "<-"
    Forward,   // "->"
}

impl FKDir {
    fn as_str(&self) -> &'static str {
        match self {
            FKDir::Backward => "<-",
            FKDir::Forward => "->",
        }
    }
}

#[derive(Debug)]
struct JoinInfo {
    join_type: JoinType,
    new_table: String,
    new_alias: String,
    new_table_col: String,
    arrow: FKDir,
    existing_alias: String,
    existing_table_col: String,
    fk_cols_not_null: bool,
    fk_cols_unique: bool,
}

// Add this struct to track table aliases
#[derive(Debug)]
struct TableAlias {
    table_name: String,
    alias: String,
}

fn verify_derived_table(first_table: &str, joins: &[JoinInfo]) -> (std::collections::HashMap<String, std::collections::HashSet<String>>, std::collections::HashSet<String>) {
    if joins.is_empty() {
        return (std::collections::HashMap::new(), std::collections::HashSet::new());
    }

    // Initialize A and U with first table
    let mut a = std::collections::HashMap::new();
    a.insert(first_table.to_string(), std::collections::HashSet::from([first_table.to_string()]));
    let mut u = std::collections::HashSet::from([first_table.to_string()]);

    for join in joins.iter() {
        let (existing_alias, new_alias) = match join.arrow {
            FKDir::Backward => (join.existing_alias.clone(), join.new_alias.clone()),
            FKDir::Forward => (join.existing_alias.clone(), join.new_alias.clone()),
        };

        // Calculate A_inner based on conditions
        let mut a_inner = std::collections::HashMap::new();
        if join.fk_cols_not_null {
            let preserves_rows = a.contains_key(&existing_alias);
            let self_preserving = a.get(&existing_alias)
                .map(|s| s.contains(&existing_alias))
                .unwrap_or(false);

            if matches!(join.arrow, FKDir::Forward) && self_preserving {
                // For each key in A, if existing_rel in A[key], then A_inner[key] = {new_rel}
                for (key, value) in a.iter() {
                    if value.contains(&existing_alias) {
                        let mut new_set = std::collections::HashSet::new();
                        new_set.insert(new_alias.clone());
                        a_inner.insert(key.clone(), new_set);
                    }
                }
                // Also add A_inner[new_rel] = {new_rel}
                let mut new_set = std::collections::HashSet::new();
                new_set.insert(new_alias.clone());
                a_inner.insert(new_alias.clone(), new_set);
            } else if matches!(join.arrow, FKDir::Backward) && preserves_rows {
                // For each key in A, A_inner[key] = A[key] & A[existing_rel]
                let existing_set = a.get(&existing_alias).cloned().unwrap_or_default();
                for (key, value) in a.iter() {
                    let intersection: std::collections::HashSet<_> = value.intersection(&existing_set).cloned().collect();
                    a_inner.insert(key.clone(), intersection);
                }
                // Also add A_inner[new_rel] = A[existing_rel]
                a_inner.insert(new_alias.clone(), existing_set);
            }
        }

        // Update A based on join type
        match join.join_type {
            JoinType::Inner => {
                a = a_inner;
            },
            JoinType::Left => {
                // A = A | A_inner
                for (key, value) in a_inner {
                    a.entry(key).or_default().extend(value);
                }
            },
            JoinType::Right => {
                // A = A_inner | {new_rel: {new_rel}}
                // First preserve a_inner mappings
                a = a_inner;
                // Then add new_rel: {new_rel} mapping while preserving any existing mappings
                let mut new_set = std::collections::HashSet::new();
                new_set.insert(new_alias.clone());
                a.entry(new_alias.clone())
                    .and_modify(|set| { set.insert(new_alias.clone()); })
                    .or_insert(new_set);
            },
            JoinType::Full => {
                // A = A | A_inner | {new_rel: {new_rel}}
                // First preserve existing A mappings (no change needed)
                // Then add A_inner mappings
                for (key, value) in a_inner {
                    a.entry(key).or_default().extend(value);
                }
                // Finally add new_rel: {new_rel} mapping
                let mut new_set = std::collections::HashSet::new();
                new_set.insert(new_alias.clone());
                a.entry(new_alias.clone())
                    .and_modify(|set| { set.insert(new_alias.clone()); })
                    .or_insert(new_set);
            },
        }

        // Update U based on conditions
        match (join.arrow, u.contains(&existing_alias), join.fk_cols_unique) {
            (FKDir::Backward, false, _) | (FKDir::Backward, _, false) => {
                // U = U (no change)
            },
            (_, true, true) => {
                u.insert(new_alias.clone());
            },
            (_, false, true) => {
                // U = U (no change)
            },
            (FKDir::Forward, true, false) => {
                u.clear();
                u.insert(new_alias.clone());
            },
            (FKDir::Forward, false, false) => {
                u.clear();
            },
        }
    }

    (a, u)
}

// Modify generate_fk_join_query to return metadata needed for verification
fn generate_fk_join_query(tables: &[Table], num_joins: usize, rng: &mut impl Rng, args: &Args)
    -> Option<(String, Vec<JoinInfo>)>
{
    // Collect all foreign key relationships
    let mut fk_relationships: Vec<(String, String, String, String)> = Vec::new(); // (ref_table, ref_col, fk_table, fk_col)
    for table in tables {
        for column in &table.columns {
            if column.is_foreign_key {
                let (ref_table, ref_col) = column.reference.as_ref().unwrap();
                fk_relationships.push((
                    ref_table.clone(),
                    ref_col.clone(),
                    table.name.clone(),
                    column.name.clone()
                ));
            }
        }
    }

    if fk_relationships.is_empty() {
        return None;
    }

    // Pick first relationship
    let first_rel = fk_relationships.choose(rng)?;
    let join_types = [JoinType::Inner, JoinType::Left, JoinType::Right, JoinType::Full];

    let mut joins = Vec::new();  // Initialize joins vector

    // Track table aliases instead of just table names
    let mut used_aliases = Vec::new();
    let mut alias_counter: std::collections::HashMap<String, usize> = std::collections::HashMap::new();

    // Helper function to get next alias for a table
    let get_alias = |table: &str, aliases: &mut std::collections::HashMap<String, usize>| {
        let count = aliases.entry(table.to_string()).or_insert(1);
        let alias = if *count == 1 {
            table.to_string()
        } else {
            format!("{}_{}", table, count)
        };
        *count += 1;
        alias
    };

    // Decide if we start with referenced or referencing table
    let start_with_referenced = rng.gen_bool(0.5);
    let first_table = if start_with_referenced {
        first_rel.0.clone() // referenced table
    } else {
        first_rel.2.clone() // referencing table
    };
    let first_alias = get_alias(&first_table, &mut alias_counter);
    used_aliases.push(TableAlias {
        table_name: first_table,
        alias: first_alias.clone(),
    });

    // Add first join
    if start_with_referenced {
        if first_rel.2 == first_rel.0 {
            return None;
        }
        let new_alias = get_alias(&first_rel.2, &mut alias_counter);

        // Find the foreign key column to get its properties
        let fk_table = tables.iter().find(|t| t.name == first_rel.2).unwrap();
        let fk_col = fk_table.columns.iter()
            .find(|c| c.name == first_rel.3)
            .unwrap();

        joins.push(JoinInfo {
            join_type: *join_types.choose(rng).unwrap(),
            new_table: first_rel.2.clone(),
            new_alias: new_alias.clone(),
            new_table_col: first_rel.3.clone(),
            arrow: FKDir::Forward,
            existing_alias: first_alias.clone(),
            existing_table_col: first_rel.1.clone(),
            fk_cols_not_null: fk_col.is_not_null,
            fk_cols_unique: fk_col.is_unique,
        });
        used_aliases.push(TableAlias {
            table_name: first_rel.2.clone(),
            alias: new_alias,
        });
    } else {
        if first_rel.0 == first_rel.2 {
            return None;
        }
        let new_alias = get_alias(&first_rel.0, &mut alias_counter);

        // Find the foreign key column to get its properties
        let fk_table = tables.iter().find(|t| t.name == first_rel.2).unwrap();
        let fk_col = fk_table.columns.iter()
            .find(|c| c.name == first_rel.3)
            .unwrap();

        joins.push(JoinInfo {
            join_type: *join_types.choose(rng).unwrap(),
            new_table: first_rel.0.clone(),
            new_alias: new_alias.clone(),
            new_table_col: first_rel.1.clone(),
            arrow: FKDir::Backward,
            existing_alias: first_alias.clone(),
            existing_table_col: first_rel.3.clone(),
            fk_cols_not_null: fk_col.is_not_null,
            fk_cols_unique: fk_col.is_unique,
        });
        used_aliases.push(TableAlias {
            table_name: first_rel.0.clone(),
            alias: new_alias,
        });
    }

    // Try to add remaining joins
    for _ in 1..num_joins {
        // Find relationships where at least one table is in used_tables
        let valid_relationships: Vec<_> = fk_relationships.iter()
            .filter(|(ref_table, _, fk_table, _)| {
                let ref_table_used = used_aliases.iter().any(|a| &a.table_name == ref_table);
                let fk_table_used = used_aliases.iter().any(|a| &a.table_name == fk_table);

                // If both tables are used, only include with TABLE_REUSE_PROB probability
                if ref_table_used && fk_table_used {
                    rng.gen_bool(args.table_reuse_prob)
                } else {
                    // At least one table must be used
                    ref_table_used || fk_table_used
                }
            })
            .collect();

        if valid_relationships.is_empty() {
            break;
        }

        let rel = valid_relationships.choose(rng).unwrap();
        let ref_table_is_used = used_aliases.iter()
            .any(|a| a.table_name == rel.0);

        if ref_table_is_used {
            let new_alias = get_alias(&rel.2, &mut alias_counter);
            let existing_alias = used_aliases.iter()
                .find(|a| a.table_name == rel.0)
                .unwrap()
                .alias
                .clone();

            // For Forward joins (->), the foreign key is in the existing table
            let arrow = FKDir::Forward;
            // For Forward joins, look up the referenced column in the referenced table
            let (fk_table_name, fk_col_name) = if matches!(arrow, FKDir::Forward) {
                (rel.2.clone(), rel.3.clone())  // new table and column
            } else {
                (rel.0.clone(), rel.1.clone())  // existing table and column
            };

            // Find the foreign key column to get its properties
            let fk_table = tables.iter().find(|t| t.name == fk_table_name).unwrap();
            let fk_col = fk_table.columns.iter()
                .find(|c| c.name == fk_col_name)
                .unwrap();

            joins.push(JoinInfo {
                join_type: *join_types.choose(rng).unwrap(),
                new_table: rel.2.clone(),
                new_alias: new_alias.clone(),
                new_table_col: rel.3.clone(),
                arrow,
                existing_alias: existing_alias,
                existing_table_col: rel.1.clone(),
                fk_cols_not_null: fk_col.is_not_null,
                fk_cols_unique: fk_col.is_unique,
            });
            used_aliases.push(TableAlias {
                table_name: rel.2.clone(),
                alias: new_alias,
            });
        } else {
            let new_alias = get_alias(&rel.0, &mut alias_counter);
            let existing_alias = used_aliases.iter()
                .find(|a| a.table_name == rel.2)
                .unwrap()
                .alias
                .clone();

            let arrow = FKDir::Backward;
            // For Forward joins, look up the referenced column in the referenced table
            let (fk_table_name, fk_col_name) = if matches!(arrow, FKDir::Forward) {
                (rel.0.clone(), rel.1.clone())  // new table and column
            } else {
                (rel.2.clone(), rel.3.clone())  // existing table and column
            };

            // Find the foreign key column to get its properties
            let fk_table = tables.iter().find(|t| t.name == fk_table_name).unwrap();
            let fk_col = fk_table.columns.iter()
                .find(|c| c.name == fk_col_name)
                .unwrap();

            joins.push(JoinInfo {
                join_type: *join_types.choose(rng).unwrap(),
                new_table: rel.0.clone(),
                new_alias: new_alias.clone(),
                new_table_col: rel.1.clone(),
                arrow,
                existing_alias: existing_alias,
                existing_table_col: rel.3.clone(),
                fk_cols_not_null: fk_col.is_not_null,
                fk_cols_unique: fk_col.is_unique,
            });
            used_aliases.push(TableAlias {
                table_name: rel.0.clone(),
                alias: new_alias,
            });
        }
    }

    Some((first_alias, joins))
}

#[derive(Debug)]
struct ErrorContext {
    output: Vec<String>,
    query: Option<String>,
    view_sql: Option<String>,
}

#[derive(Debug)]
enum FuzzerError {
    PostgresError(postgres::Error, ErrorContext),
    SQLiteError(rusqlite::Error, ErrorContext),
    IoError(std::io::Error),
    ComparisonError(String, ErrorContext),
}

impl std::fmt::Display for FuzzerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FuzzerError::PostgresError(e, _) => write!(f, "PostgreSQL error: {}", e),
            FuzzerError::SQLiteError(e, _) => write!(f, "SQLite error: {}", e),
            FuzzerError::IoError(e) => write!(f, "IO error: {}", e),
            FuzzerError::ComparisonError(e, _) => write!(f, "Comparison error: {}", e),
        }
    }
}

impl Error for FuzzerError {}

impl From<postgres::Error> for FuzzerError {
    fn from(err: postgres::Error) -> Self {
        FuzzerError::PostgresError(err, ErrorContext {
            output: Vec::new(),
            query: None,
            view_sql: None,
        })
    }
}

impl From<rusqlite::Error> for FuzzerError {
    fn from(err: rusqlite::Error) -> Self {
        // Ignore SQLITE_OK (21) status as it's not really an error
        if let rusqlite::Error::SqliteFailure(error, _) = &err {
            if error.extended_code == 21 {
                // Return a dummy error that will be filtered out
                return FuzzerError::SQLiteError(err, ErrorContext {
                    output: Vec::new(),
                    query: None,
                    view_sql: None,
                });
            }
        }
        FuzzerError::SQLiteError(err, ErrorContext {
            output: Vec::new(),
            query: None,
            view_sql: None,
        })
    }
}

impl From<std::io::Error> for FuzzerError {
    fn from(err: std::io::Error) -> Self {
        FuzzerError::IoError(err)
    }
}

fn run_fuzzer(args: &Args) -> Result<(), Box<dyn Error>> {
    loop {
        let mut output = Vec::new();

        // Reset the column counter
        GLOBAL_COLUMN_COUNTER.store(1, Ordering::SeqCst);

        let mut rng = thread_rng();
        let mut tables: Vec<Table> = Vec::with_capacity(args.num_tables);

        // Reference candidates: PK or UNIQUE columns from previous tables
        let mut reference_candidates: Vec<(String, String)> = Vec::new();

        for i in 1..=args.num_tables {
            let table_name = format!("t{}", i);
            let mut table = Table::new(&table_name);

            // 1) Decide how many columns for this table (around AVG_COLUMNS).
            let lower = if args.avg_columns > args.extra_columns_range {
                args.avg_columns - args.extra_columns_range
            } else {
                2 // ensure at least 2
            };
            let upper = args.avg_columns + args.extra_columns_range;
            let columns_distribution = Uniform::new_inclusive(lower.max(2), upper.max(2));
            let num_columns = columns_distribution.sample(&mut rng);

            // 1.5) Create the mandatory "id" PRIMARY KEY column
            let id_column = Column {
                name: "id".to_string(),
                is_not_null: true,
                is_unique: false,
                is_primary_key: true,
                is_foreign_key: false,
                reference: None,
            };
            table.columns.push(id_column.clone());
            reference_candidates.push((table_name.clone(), "id".to_string()));

            // Create the remaining columns
            let mut regular_columns = Vec::new();
            for _ in 1..=num_columns {
                let column_name = get_next_column_name();
                let mut col = Column {
                    name: column_name,
                    is_not_null: false,
                    is_unique: false,
                    is_primary_key: false,
                    is_foreign_key: false,
                    reference: None,
                };
                // Possibly make it UNIQUE
                if rng.gen_bool(args.additional_unique_prob) {
                    col.is_unique = true;
                    col.is_not_null = rng.gen_bool(args.additional_unique_not_null_prob);
                }
                regular_columns.push(col);
            }

            // 4) Determine how many foreign key columns to add
            let foreign_key_max_count = ((i - 1) as f64 * (args.foreign_key_max_percent as f64 / 100.0)).floor() as usize;
            let min_fk_columns = if i == 1 { 0 } else { 1 }; // At least 1 FK for non-first tables
            let possible_fk_columns = foreign_key_max_count
                .min(regular_columns.len())
                .min(reference_candidates.len())
                .max(min_fk_columns);
            let fk_distribution = Uniform::new_inclusive(min_fk_columns, possible_fk_columns);
            let num_fk_columns = fk_distribution.sample(&mut rng);

            // Shuffle, then convert a subset to foreign keys
            regular_columns.shuffle(&mut rng);
            let (fk_cols, normal_cols) = regular_columns.split_at_mut(num_fk_columns);
            for fk_col in fk_cols.iter_mut() {
                fk_col.is_foreign_key = true;

                // Pick a random reference target
                let idx = rng.gen_range(0..reference_candidates.len());
                let (ref_table, ref_column) = &reference_candidates[idx];

                // If the foreign key references its own table, it must be nullable
                if ref_table == &table_name {
                    fk_col.is_not_null = false;
                } else {
                    fk_col.is_not_null = rng.gen_bool(args.foreign_key_not_null_prob);
                }

                fk_col.is_unique = rng.gen_bool(args.foreign_key_unique_prob);
                fk_col.reference = Some((ref_table.clone(), ref_column.clone()));
            }

            // Combine all columns and sort them by name
            let mut final_columns = Vec::new();
            let main_key_col = table.columns.remove(0);
            final_columns.push(main_key_col);
            final_columns.extend_from_slice(fk_cols);
            final_columns.extend_from_slice(normal_cols);

            // Sort columns by extracting the number from the column name and comparing
            final_columns.sort_by_key(|col| {
                col.name
                    .trim_start_matches('c')
                    .parse::<usize>()
                    .unwrap_or(0)
            });

            table.columns = final_columns;
            tables.push(table);

            // 5) Update reference_candidates for future foreign keys
            let last_table = tables.last().unwrap();
            for col in &last_table.columns {
                if col.is_primary_key || col.is_unique {
                    reference_candidates.push((last_table.name.clone(), col.name.clone()));
                }
            }
        }

        // 6) Generate CREATE TABLE statements
        for table in &tables {
            let mut create_stmt = format!("CREATE TABLE {} (\n", table.name);

            // Collect constraints first
            let mut constraints = Vec::new();
            for column in &table.columns {
                if column.is_primary_key {
                    constraints.push(format!(
                        "CONSTRAINT {table}_{col}_pk PRIMARY KEY ({col})",
                        table = table.name,
                        col = column.name
                    ));
                } else if column.is_unique {
                    constraints.push(format!(
                        "CONSTRAINT {table}_{col}_unique UNIQUE ({col})",
                        table = table.name,
                        col = column.name
                    ));
                }
                if column.is_foreign_key {
                    let (ref_table, ref_col) = column.reference.clone().unwrap();
                    constraints.push(format!(
                        "CONSTRAINT {table}_{col}_fk FOREIGN KEY ({col}) REFERENCES {ref_table} ({ref_col})",
                        table = table.name,
                        col = column.name,
                        ref_table = ref_table,
                        ref_col = ref_col
                    ));
                }
            }

            let total_items = table.columns.len() + constraints.len();
            let mut items_printed = 0;

            // Add columns
            for column in &table.columns {
                items_printed += 1;
                let mut col_def = format!("    {}", column.name);
                col_def.push_str(" INT");
                if column.is_not_null {
                    col_def.push_str(" NOT NULL");
                }
                if items_printed < total_items {
                    col_def.push_str(",");
                }
                create_stmt.push_str(&col_def);
                create_stmt.push_str("\n");
            }

            // Add constraints
            for constraint in constraints.iter() {
                items_printed += 1;
                if items_printed == total_items {
                    create_stmt.push_str(&format!("    {}", constraint));
                } else {
                    create_stmt.push_str(&format!("    {},\n", constraint));
                }
            }

            create_stmt.push_str("\n);");
            output.push(create_stmt);
        }

        // Generate insert statements
        if let Ok(_) = generate_insert_statements(&tables, &mut rng, &mut output, &args) {
            let num_joins = if args.max_num_joins > args.min_num_joins {
                rng.gen_range(args.min_num_joins..=args.max_num_joins)
            } else {
                args.min_num_joins
            };
            if let Some((first_table, joins)) = generate_fk_join_query(&tables, num_joins, &mut rng, &args) {
                // Track tables in order of appearance, using their aliases
                let mut table_aliases = Vec::new();
                table_aliases.push((first_table.clone(), first_table.clone()));

                for join in &joins {
                    table_aliases.push((join.new_table.clone(), join.new_alias.clone()));
                }

                // Create view and verification queries
                let (theoretical_a, theoretical_u) = verify_derived_table(&first_table, &joins);
                println!("\nTheoretical sets:");
                println!("A: {}", format!("{{{}}}", theoretical_a.iter()
                    .map(|(k, v)| format!("{}:{{{}}}", k.as_str(), v.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")))
                    .collect::<Vec<_>>()
                    .join(",")));
                println!("U: {}", format!("{{{}}}", theoretical_u.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")));
                println!("");  // Extra line for status messages
                let view_sql = create_view_sql(&first_table, &joins, &table_aliases, &args.join_syntax, &tables);
                let verify_sql = create_verify_sql(&table_aliases);
                let (valid_test, invalid_test) = create_fk_join_tests(&tables, &first_table, &joins, &theoretical_a, &theoretical_u);

                // Save current SQL to file for debugging
                let mut current_sql = File::create("join_fuzzer_current.sql")?;
                writeln!(current_sql, "-- Generated SQL for debugging purposes")?;
                writeln!(current_sql, "-- Table creation and data")?;
                for sql in &output {
                    writeln!(current_sql, "{}", sql)?;
                }
                writeln!(current_sql, "\n-- View definition")?;
                writeln!(current_sql, "{}", view_sql)?;
                writeln!(current_sql, "\n-- Foreign Key Join Tests")?;
                if let Some(test) = &valid_test {
                    writeln!(current_sql, "{}\n", test)?;
                }
                if let Some(test) = &invalid_test {
                    writeln!(current_sql, "{}\n", test)?;
                }
                writeln!(current_sql, "\n-- Verification query")?;
                writeln!(current_sql, "{}", verify_sql)?;

                let mut pg_practical = None;
                let mut sqlite_practical = None;
                let mut pg_client = None;
                let mut sqlite_conn = None;

                // Test PostgreSQL if enabled
                if args.test_dbs.contains(&Database::PostgreSQL) {
                    // Connect to PostgreSQL and execute
                    let db_user = args.db_user.clone().unwrap_or_else(|| env::var("USER").unwrap_or_else(|_| "postgres".to_string()));
                    let conn_string = format!(
                        "host='{}' port='{}' dbname='{}' user='{}' password='{}'",
                        args.db_host.replace("'", "\\'"),
                        args.db_port,
                        args.db_name.replace("'", "\\'"),
                        db_user.replace("'", "\\'"),
                        args.db_password.replace("'", "\\'")
                    );

                    match Client::connect(&conn_string, NoTls) {
                        Ok(mut client) => {
                            println!("\nExecuting queries against PostgreSQL:");
                            let start = Instant::now();

                            // Execute schema reset and SQL statements
                            if let Err(e) = client.batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA {}; SET search_path TO {};",
                                args.db_name, args.db_name, args.db_name)) {
                                eprintln!("\nError: Failed to reset database schema");
                                eprintln!("Details: {}", e);
                                eprintln!("\nPlease check:");
                                eprintln!("1. Is PostgreSQL running?");
                                eprintln!("2. Can you connect to {}:{} ?", args.db_host, args.db_port);
                                eprintln!("3. Does user '{}' have permission to create schemas?", db_user);
                                let filename = save_error_sql(&output, &e, Some(&format!("DROP SCHEMA IF EXISTS {} CASCADE; CREATE SCHEMA {}; SET search_path TO {};",
                                    args.db_name, args.db_name, args.db_name)), None);
                                eprintln!("SQL saved to {}", filename);
                                std::process::exit(1);
                            }
                            println!("Schema setup time: {:?}", start.elapsed());

                            // Set statement timeout
                            if let Err(e) = client.batch_execute(&format!("SET statement_timeout = '{}';", args.statement_timeout_ms)) {
                                let filename = save_error_sql(&output, &e, Some(&format!("SET statement_timeout = '{}';", args.statement_timeout_ms)), None);
                                eprintln!("SQL saved to {}", filename);
                                std::process::exit(1);
                            }

                            // Execute table creation and data insertion
                            let start = Instant::now();
                            for sql in &output {
                                if let Err(e) = client.batch_execute(sql) {
                                    let filename = save_error_sql(&output, &e, Some(sql), None);
                                    eprintln!("SQL saved to {}", filename);
                                    std::process::exit(1);
                                }
                            }
                            println!("Table creation and data insertion time: {:?}", start.elapsed());

                            // Execute view creation
                            let start = Instant::now();
                            if let Err(e) = client.batch_execute(&view_sql) {
                                // Check if this is a statement timeout
                                if e.as_db_error()
                                    .map(|dbe| dbe.message().contains("canceling statement due to statement timeout"))
                                    .unwrap_or(false)
                                {
                                    // Save error to file but continue
                                    let filename = save_error_sql(&output, &e, Some(&view_sql), Some(&view_sql));
                                    println!("Statement timeout occurred during view creation. SQL saved to {}", filename);
                                    return Ok(());
                                }
                                let filename = save_error_sql(&output, &e, Some(&view_sql), Some(&view_sql));
                                eprintln!("SQL saved to {}", filename);
                                std::process::exit(1);
                            }
                            println!("View creation time: {:?}", start.elapsed());

                            // Execute foreign key join tests
                            let start = Instant::now();
                            let (valid_test, invalid_test) = create_fk_join_tests(&tables, &first_table, &joins, &theoretical_a, &theoretical_u);

                            // Execute valid test if present
                            if let Some(test) = valid_test {
                                let table_sql = output.iter()
                                    .take_while(|line| !line.starts_with("CREATE VIEW"))
                                    .map(|s| s.as_str())
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                if let Err(e) = handle_fk_join_test(&test, &view_sql, &table_sql, true, &theoretical_a, &theoretical_u) {
                                    eprintln!("Error handling valid FK join test: {}", e);
                                    let filename = save_error_sql(&output, &format!("Valid foreign key join test failed: {}", e), Some(&test), Some(&view_sql));
                                    eprintln!("SQL saved to {}", filename);
                                    std::process::exit(1);
                                }
                            }

                            // Execute invalid test if present
                            if let Some(test) = invalid_test {
                                let table_sql = output.iter()
                                    .take_while(|line| !line.starts_with("CREATE VIEW"))
                                    .map(|s| s.as_str())
                                    .collect::<Vec<_>>()
                                    .join("\n");
                                if let Err(e) = handle_fk_join_test(&test, &view_sql, &table_sql, false, &theoretical_a, &theoretical_u) {
                                    eprintln!("Error handling invalid FK join test: {}", e);
                                    let filename = save_error_sql(&output, &format!("Invalid foreign key join test failed: {}", e), Some(&test), Some(&view_sql));
                                    eprintln!("SQL saved to {}", filename);
                                    std::process::exit(1);
                                }
                            }
                            println!("Foreign key join tests time: {:?}", start.elapsed());

                            // Get practical results from PostgreSQL
                            let start = Instant::now();
                            pg_practical = match client.query_one(&verify_sql, &[]) {
                                Ok(row) => {
                                    let pg_a: Option<String> = row.get(0);
                                    let pg_u: Option<String> = row.get(1);
                                    Some((pg_a, pg_u))
                                },
                                Err(e) => {
                                    // Check if this is a statement timeout
                                    if e.as_db_error()
                                        .map(|dbe| dbe.message().contains("canceling statement due to statement timeout"))
                                        .unwrap_or(false)
                                    {
                                        // Save error to file but continue
                                        let filename = save_error_sql(&output, &e, Some(&verify_sql), Some(&view_sql));
                                        println!("Statement timeout occurred. SQL saved to {}", filename);
                                        None
                                    } else {
                                        // For other errors, return as normal
                                        return Err(Box::new(FuzzerError::PostgresError(e, ErrorContext {
                                            output: output.clone(),
                                            query: Some(verify_sql.clone()),
                                            view_sql: Some(view_sql.clone()),
                                        })));
                                    }
                                },
                            };
                            println!("PostgreSQL query execution time: {:?}", start.elapsed());
                            pg_client = Some(client);

                            // Compare theoretical and practical results
                            if let Some((pg_a, pg_u)) = &pg_practical {
                                // Convert theoretical_a to simpler format - keep only self-preserving relations
                                let theoretical_a_simple: std::collections::HashSet<String> = theoretical_a.iter()
                                    .filter(|(k, v)| v.contains(*k))
                                    .map(|(k, _)| k.clone())
                                    .collect();

                                // Parse practical results into HashSets
                                let practical_a: std::collections::HashSet<String> = pg_a.as_ref()
                                    .map(|s| s.split(',').map(|s| s.to_string()).collect())
                                    .unwrap_or_default();
                                let practical_u: std::collections::HashSet<String> = pg_u.as_ref()
                                    .map(|s| s.split(',').map(|s| s.to_string()).collect())
                                    .unwrap_or_default();

                                // Check if theoretical sets are subsets of practical sets
                                let missing_a: Vec<_> = theoretical_a_simple.difference(&practical_a).collect();
                                let missing_u: Vec<_> = theoretical_u.difference(&practical_u).collect();
                                let extra_a: Vec<_> = practical_a.difference(&theoretical_a_simple).collect();
                                let extra_u: Vec<_> = practical_u.difference(&theoretical_u).collect();

                                if !missing_a.is_empty() || !missing_u.is_empty() {
                                    println!("\nTheoretical vs Practical Results Mismatch!");
                                    if !missing_a.is_empty() {
                                        println!("Missing from practical A: {}", missing_a.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
                                    }
                                    if !missing_u.is_empty() {
                                        println!("Missing from practical U: {}", missing_u.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
                                    }
                                    return Err(Box::new(FuzzerError::ComparisonError(
                                        "Theoretical relations missing from practical results".to_string(),
                                        ErrorContext {
                                            output: output.clone(),
                                            query: Some(verify_sql.clone()),
                                            view_sql: Some(view_sql.clone()),
                                        }
                                    )));
                                } else {
                                    println!("\nTheoretical and practical results match!");
                                }

                                if !extra_a.is_empty() {
                                    println!("\nAdditional relations found in practical A: {}", extra_a.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
                                }
                                if !extra_u.is_empty() {
                                    println!("\nAdditional relations found in practical U: {}", extra_u.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(", "));
                                }

                                // Store cases with extra relations
                                if !extra_a.is_empty() || !extra_u.is_empty() {
                                    let extra_filename = format!("extra_{}A_{}U.sql", extra_a.len(), extra_u.len());
                                    if !std::path::Path::new(&extra_filename).exists() {
                                        let mut log = std::fs::File::create(&extra_filename)?;
                                        writeln!(log, "-- Case with {} extra A relations and {} extra U relations", extra_a.len(), extra_u.len())?;
                                        writeln!(log, "-- Results:")?;
                                        writeln!(log, "-- Theoretical A: {}", format!("{{{}}}", theoretical_a.iter()
                                            .map(|(k, v)| format!("{}:{{{}}}", k.as_str(), v.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")))
                                            .collect::<Vec<_>>()
                                            .join(",")))?;
                                        writeln!(log, "-- Practical A:   {}", pg_practical.as_ref()
                                            .and_then(|(a, _)| a.as_ref())
                                            .unwrap_or(&String::new()))?;
                                        writeln!(log, "-- Theoretical U: {}", format!("{{{}}}", theoretical_u.iter()
                                            .map(|s| s.as_str())
                                            .collect::<Vec<_>>()
                                            .join(",")))?;
                                        writeln!(log, "-- Practical U:   {}", pg_practical.as_ref()
                                            .and_then(|(_, u)| u.as_ref())
                                            .unwrap_or(&String::new()))?;
                                        writeln!(log)?;

                                        // Write all SQL statements
                                        for sql in &output {
                                            writeln!(log, "{}", sql)?;
                                        }
                                        writeln!(log, "\n\n")?;
                                        writeln!(log, "{}", view_sql)?;
                                        writeln!(log, "\n\n")?;
                                        writeln!(log, "{}", verify_sql)?;
                                        writeln!(log, "\n")?;

                                        println!("Found new case with extra relations! Saved to {}", extra_filename);
                                    }
                                }
                            }
                        },
                        Err(e) => {
                            eprintln!("\nError: Failed to connect to database");
                            eprintln!("Details: {}", e);
                            eprintln!("\nPlease check:");
                            eprintln!("1. Is PostgreSQL running?");
                            eprintln!("2. Can you connect to {}:{} ?", args.db_host, args.db_port);
                            eprintln!("3. Are the database credentials correct?");
                            eprintln!("   - Database: {}", args.db_name);
                            eprintln!("   - User: {}", db_user);
                            eprintln!("   - Host: {}", args.db_host);
                            eprintln!("   - Port: {}", args.db_port);
                            return Err(Box::new(FuzzerError::PostgresError(
                                e,
                                ErrorContext {
                                    output: output.clone(),
                                    query: Some(conn_string.clone()),
                                    view_sql: None,
                                }
                            )));
                        }
                    }
                }

                // Test SQLite if enabled
                if args.test_dbs.contains(&Database::SQLite) {
                    println!("\nExecuting queries against SQLite:");
                    let sqlite_db = format!("{}.db", Uuid::new_v4());
                    let start = Instant::now();
                    let sqlite = Connection::open(&sqlite_db)?;

                    // Execute schema and data insertion in SQLite
                    for sql in &output {
                        if let Err(e) = sqlite.execute(sql, []) {
                            // Ignore SQLITE_OK status
                            if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                if error.extended_code == 21 {
                                    continue;
                                }
                            }
                            return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                output: output.clone(),
                                query: Some(sql.clone()),
                                view_sql: Some(view_sql.clone()),
                            })));
                        }
                    }
                    println!("Table creation and data insertion time: {:?}", start.elapsed());

                    // Execute view creation in SQLite
                    let start = Instant::now();
                    if let Err(e) = sqlite.execute(&view_sql, []) {
                        // Ignore SQLITE_OK status
                        if let rusqlite::Error::SqliteFailure(error, _) = &e {
                            if error.extended_code == 21 {
                                // Continue without error
                            } else {
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(view_sql.clone()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        } else {
                            return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                output: output.clone(),
                                query: Some(view_sql.clone()),
                                view_sql: Some(view_sql.clone()),
                            })));
                        }
                    }
                    println!("View creation time: {:?}", start.elapsed());

                    // Get practical results from SQLite
                    let start = Instant::now();
                    sqlite_practical = {
                        let mut stmt = match sqlite.prepare(&verify_sql) {
                            Ok(stmt) => stmt,
                            Err(e) => {
                                // Ignore SQLITE_OK status
                                if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                    if error.extended_code == 21 {
                                        continue;
                                    }
                                }
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(verify_sql.clone()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        };
                        let mut rows = match stmt.query([]) {
                            Ok(rows) => rows,
                            Err(e) => {
                                // Ignore SQLITE_OK status
                                if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                    if error.extended_code == 21 {
                                        continue;
                                    }
                                }
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(verify_sql.clone()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        };
                        if let Some(row) = rows.next()? {
                            let sqlite_a: Option<String> = row.get(0)?;
                            let sqlite_u: Option<String> = row.get(1)?;
                            Some((sqlite_a, sqlite_u))
                        } else {
                            None
                        }
                    };
                    println!("Verification query time: {:?}", start.elapsed());
                    sqlite_conn = Some((sqlite, sqlite_db));
                }

                // Compare results between databases if both were tested
                if let (Some(pg_results), Some(sqlite_results)) = (&pg_practical, &sqlite_practical) {
                    if pg_results != sqlite_results {
                        println!("PostgreSQL and SQLite practical results differ!");
                        println!("PostgreSQL A: {:?}, U: {:?}", pg_results.0, pg_results.1);
                        println!("SQLite    A: {:?}, U: {:?}", sqlite_results.0, sqlite_results.1);
                        return Err(Box::new(FuzzerError::ComparisonError(
                            "PostgreSQL and SQLite practical A/U sets differ".to_string(),
                            ErrorContext {
                                output: output.clone(),
                                query: Some(verify_sql.clone()),
                                view_sql: Some(view_sql.clone()),
                            }
                        )));
                    } else {
                        println!("PostgreSQL and SQLite practical results are identical");
                    }

                    // Compare actual results
                    let order_by_columns: Vec<_> = table_aliases.iter()
                        .map(|(_, alias)| {
                            format!("CASE WHEN {0}_id IS NULL THEN -1 ELSE {0}_id END", alias)
                        })
                        .collect();
                    let compare_sql = format!("SELECT * FROM v ORDER BY {}", order_by_columns.join(", "));

                    // Get PostgreSQL results
                    let pg_rows = if let Some(client) = &mut pg_client {
                        client.query(&compare_sql, &[])?
                    } else {
                        Vec::new()
                    };
                    let pg_values: Vec<Vec<Option<i32>>> = pg_rows.iter().map(|row| {
                        let mut values = Vec::new();
                        for i in 0..row.len() {
                            values.push(row.get(i));
                        }
                        values
                    }).collect();

                    // Get SQLite results
                    let sqlite_rows = if let Some((ref sqlite, _)) = sqlite_conn {
                        let mut stmt = match sqlite.prepare(&compare_sql) {
                            Ok(stmt) => stmt,
                            Err(e) => {
                                // Ignore SQLITE_OK status
                                if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                    if error.extended_code == 21 {
                                        continue;
                                    }
                                }
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(compare_sql.to_string()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        };
                        let column_count = stmt.column_count();
                        let rows = match stmt.query_map([], |row| {
                            let mut values = Vec::new();
                            for i in 0..column_count {
                                values.push(row.get::<_, Option<i32>>(i).unwrap_or(None));
                            }
                            Ok(values)
                        }) {
                            Ok(rows) => rows,
                            Err(e) => {
                                // Ignore SQLITE_OK status
                                if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                    if error.extended_code == 21 {
                                        continue;
                                    }
                                }
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(compare_sql.to_string()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        };
                        match rows.collect::<Result<Vec<_>, _>>() {
                            Ok(rows) => rows,
                            Err(e) => {
                                // Ignore SQLITE_OK status
                                if let rusqlite::Error::SqliteFailure(error, _) = &e {
                                    if error.extended_code == 21 {
                                        continue;
                                    }
                                }
                                return Err(Box::new(FuzzerError::SQLiteError(e, ErrorContext {
                                    output: output.clone(),
                                    query: Some(compare_sql.to_string()),
                                    view_sql: Some(view_sql.clone()),
                                })));
                            }
                        }
                    } else {
                        Vec::new()
                    };

                    // Write results to CSV files for diff
                    let pg_file = format!("{}_pg.csv", Uuid::new_v4());
                    let sqlite_file = format!("{}_sqlite.csv", Uuid::new_v4());

                    let mut pg_writer = File::create(&pg_file)?;
                    for row in &pg_values {
                        writeln!(pg_writer, "{}", row.iter()
                            .map(|v| match v {
                                Some(n) => n.to_string(),
                                None => "NULL".to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(","))?;
                    }

                    let mut sqlite_writer = File::create(&sqlite_file)?;
                    for row in &sqlite_rows {
                        writeln!(sqlite_writer, "{}", row.iter()
                            .map(|v| match v {
                                Some(n) => n.to_string(),
                                None => "NULL".to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(","))?;
                    }

                    // Compare results with git diff
                    let diff_output = std::process::Command::new("git")
                        .args(["diff", "--no-index", "--color=always", &pg_file, &sqlite_file])
                        .output()?;

                    // Clean up temporary files
                    fs::remove_file(&pg_file)?;
                    fs::remove_file(&sqlite_file)?;
                    if let Some((_, sqlite_db)) = sqlite_conn {
                        fs::remove_file(&sqlite_db)?;
                    }

                    if !diff_output.status.success() {
                        let error_msg = String::from_utf8_lossy(&diff_output.stdout);
                        if error_msg.trim().is_empty() {
                            let stderr_msg = String::from_utf8_lossy(&diff_output.stderr);
                            if !stderr_msg.trim().is_empty() {
                                println!("{}", stderr_msg);
                            }
                        } else {
                            println!("{}", error_msg);
                        }

                        let mut error_output = Vec::new();
                        error_output.push(String::from_utf8_lossy(&diff_output.stdout).into_owned());
                        error_output.push(String::from_utf8_lossy(&diff_output.stderr).into_owned());

                        return Err(Box::new(FuzzerError::ComparisonError(
                            "PostgreSQL and SQLite results differ".to_string(),
                            ErrorContext {
                                output: error_output,
                                query: Some(compare_sql.to_string()),
                                view_sql: None,
                            }
                        )));
                    } else {
                        println!("PostgreSQL and SQLite practical results are identical");
                    }
                }

                // Check if this is a new case
                let filename = format!("joins_{}A_{}U.sql", theoretical_a.len(), theoretical_u.len());
                if !std::path::Path::new(&filename).exists() {
                    // Log successful new case
                    let mut log = std::fs::File::create(&filename)?;
                    writeln!(log, "-- New case with {} elements in A and {} elements in U",
                        theoretical_a.len(), theoretical_u.len())?;
                    writeln!(log, "-- Results:")?;
                    writeln!(log, "-- Theoretical A: {}", format!("{{{}}}", theoretical_a.iter()
                        .map(|(k, v)| format!("{}:{{{}}}", k.as_str(), v.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")))
                        .collect::<Vec<_>>()
                        .join(",")))?;
                    writeln!(log, "-- Practical A:   {}", pg_practical.as_ref()
                        .and_then(|(a, _)| a.as_ref())
                        .unwrap_or(&String::new()))?;
                    writeln!(log, "-- Theoretical U: {}", format!("{{{}}}", theoretical_u.iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<_>>()
                        .join(",")))?;
                    writeln!(log, "-- Practical U:   {}", pg_practical.as_ref()
                        .and_then(|(_, u)| u.as_ref())
                        .unwrap_or(&String::new()))?;
                    writeln!(log)?;

                    // Add schema setup statements
                    writeln!(log, "DROP SCHEMA IF EXISTS {} CASCADE;", args.db_name)?;
                    writeln!(log, "CREATE SCHEMA {};", args.db_name)?;
                    writeln!(log, "SET search_path TO {};\n", args.db_name)?;

                    // Write all SQL statements
                    for sql in &output {
                        writeln!(log, "{}", sql)?;
                    }
                    writeln!(log, "\n\n")?;
                    writeln!(log, "{}", view_sql)?;
                    writeln!(log, "\n-- Foreign Key Join Tests")?;
                    if let Some(test) = &valid_test {
                        writeln!(log, "{}\n", test)?;
                    }
                    if let Some(test) = &invalid_test {
                        writeln!(log, "{}\n", test)?;
                    }
                    writeln!(log, "\n-- Verification query")?;
                    writeln!(log, "{}", verify_sql)?;
                    writeln!(log, "\n")?;

                    println!("Found new case! Saved to {}", filename);
                }

                continue;
            }
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    // Set up Ctrl+C handler
    ctrlc::set_handler(move || {
        println!("\nReceived Ctrl+C, exiting...");
        std::process::exit(0);
    })?;

    loop {
        match run_fuzzer(&args) {
            Ok(_) => continue,  // Continue running after success
            Err(e) => {
                // Only handle real errors
                if let Some(fuzzer_err) = e.downcast_ref::<FuzzerError>() {
                    match fuzzer_err {
                        FuzzerError::SQLiteError(rusqlite::Error::SqliteFailure(error, _), _) if error.extended_code == 21 => {
                            // This is not a real error, continue running
                            continue;
                        },
                        _ => {
                            // For all other errors, print and save them
                            eprintln!("Error: {}", fuzzer_err);
                            match fuzzer_err {
                                FuzzerError::PostgresError(_, ctx) |
                                FuzzerError::SQLiteError(_, ctx) |
                                FuzzerError::ComparisonError(_, ctx) => {
                                    let filename = save_error_sql(&ctx.output, &format!("{}", fuzzer_err), ctx.query.as_deref(), ctx.view_sql.as_deref());
                                    eprintln!("SQL saved to {}", filename);
                                },
                                _ => {}
                            }
                            return Err(e);
                        }
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }
}

fn save_error_sql(output: &[String], error: &impl std::fmt::Display, failed_query: Option<&str>, view_sql: Option<&str>) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let filename = format!("{}.sql", timestamp);
    let mut file = File::create(&filename).expect("Failed to create error file");

    writeln!(file, "-- Error: {}\n", error).expect("Failed to write error");

    // Write all previous SQL statements
    for sql in output {
        writeln!(file, "{}", sql).expect("Failed to write SQL");
    }

    // Write the view definition if provided
    if let Some(view) = view_sql {
        writeln!(file, "\n-- View definition:").expect("Failed to write comment");
        writeln!(file, "{}", view).expect("Failed to write view SQL");
    }

    // Write the failing query if provided
    if let Some(query) = failed_query {
        writeln!(file, "\n-- Query that caused the error:").expect("Failed to write comment");
        writeln!(file, "{}", query).expect("Failed to write failing query");
    }

    filename
}

#[derive(Debug, Clone, PartialEq)]
enum Database {
    PostgreSQL,
    SQLite,
}

impl FromStr for Database {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "pg" | "postgres" | "postgresql" => Ok(Database::PostgreSQL),
            "sqlite" => Ok(Database::SQLite),
            _ => Err(format!("Invalid database name: {}", s))
        }
    }
}

fn create_view_sql(first_table: &str, joins: &[JoinInfo], table_aliases: &[(String, String)], join_syntax: &str, tables: &[Table]) -> String {
    let mut sql = String::from("-- This view represents the derived table created by the join sequence.\n");
    sql.push_str("-- Each table's ID is selected to track which rows from the base tables appear in the result.\n");
    sql.push_str("-- All UNIQUE columns are also included to support foreign key joins against the view.\n");

    if join_syntax.to_uppercase() == "KEY" {
        sql.push_str("-- The join sequence uses the KEY syntax to specify foreign key relationships.\n");
        sql.push_str("-- Arrows (->, <-) indicate the direction of the foreign key constraint.\n");
    } else {
        sql.push_str("-- The join sequence uses traditional ON syntax for joins.\n");
    }

    sql.push_str("CREATE VIEW v AS\nSELECT\n");

    // First add all ID columns
    let id_columns: Vec<_> = table_aliases.iter()
        .map(|(_, alias)| format!("    {}.id AS {}_id", alias, alias))
        .collect();
    sql.push_str(&id_columns.join(",\n"));

    // Then add all UNIQUE columns from each table
    for (table_name, alias) in table_aliases {
        let table = tables.iter().find(|t| t.name == *table_name).unwrap();
        for column in &table.columns {
            if column.is_unique && column.name != "id" {
                sql.push_str(&format!(",\n    {}.{} AS {}_{}", alias, column.name, alias, column.name));
            }
        }
    }
    sql.push_str("\n");

    sql.push_str(&format!("FROM {}\n", first_table));
    for join in joins {
        if join_syntax.to_uppercase() == "KEY" {
            sql.push_str(&format!("{} JOIN {} AS {} KEY ({}) {} {} ({})\n",
                join.join_type.as_str(),
                join.new_table,
                join.new_alias,
                join.new_table_col,
                join.arrow.as_str(),
                join.existing_alias,
                join.existing_table_col));
        } else {
            sql.push_str(&format!("{} JOIN {} AS {} ON {}.{} = {}.{}\n",
                join.join_type.as_str(),
                join.new_table,
                join.new_alias,
                join.new_alias,
                join.new_table_col,
                join.existing_alias,
                join.existing_table_col));
        }
    }

    sql.push_str(";");
    sql
}

fn create_verify_sql(table_aliases: &[(String, String)]) -> String {
    let mut sql = String::new();
    sql.push_str("WITH A_tables AS (\n");

    // Generate A verification using UNION ALL
    let a_checks: Vec<_> = table_aliases.iter().map(|(table, alias)| {
        format!("    SELECT \n        CASE WHEN NOT EXISTS (SELECT 1 FROM {} WHERE NOT EXISTS (SELECT 1 FROM v WHERE v.{}_id = {}.id))\n        THEN '{}' END AS table_name",
            table, alias, table, alias)
    }).collect();
    sql.push_str(&a_checks.join("\n    UNION ALL\n"));

    sql.push_str("\n),\nU_tables AS (\n");

    // Generate U verification using UNION ALL
    let u_checks: Vec<_> = table_aliases.iter().map(|(_, alias)| {
        format!("    SELECT\n        CASE WHEN (SELECT COUNT({0}_id) = COUNT(DISTINCT {0}_id) FROM v)\n        THEN '{0}' END AS table_name",
            alias)
    }).collect();
    sql.push_str(&u_checks.join("\n    UNION ALL\n"));

    sql.push_str("\n)\nSELECT \n");
    sql.push_str("    string_agg(table_name, ',' ORDER BY table_name) AS \"A\",\n");
    sql.push_str("    (SELECT string_agg(table_name, ',' ORDER BY table_name) FROM U_tables WHERE table_name IS NOT NULL) AS \"U\"\n");
    sql.push_str("FROM A_tables \nWHERE table_name IS NOT NULL;");

    sql
}

fn create_fk_join_tests(tables: &[Table], first_table: &str, joins: &[JoinInfo], theoretical_a: &std::collections::HashMap<String, std::collections::HashSet<String>>, theoretical_u: &std::collections::HashSet<String>) -> (Option<String>, Option<String>) {
    let mut valid_test = None;
    let mut invalid_test = None;

    // Build a map of table names to their aliases in the view
    let mut table_aliases = std::collections::HashMap::new();
    table_aliases.insert(first_table.to_string(), first_table.to_string());
    for join in joins {
        table_aliases.insert(join.new_table.clone(), join.new_alias.clone());
    }

    // Find tables that have foreign keys referencing tables in our view
    'outer: for table in tables {
        for column in &table.columns {
            if let Some((ref_table, ref_col)) = &column.reference {
                // Check if the referenced table is in our view (either as first table or through a join)
                if let Some(ref_alias) = table_aliases.get(ref_table) {
                    // Check if this referenced table is both in A and U (positive test)
                    if valid_test.is_none() && theoretical_a.get(ref_alias).map(|s| s.contains(ref_alias)).unwrap_or(false) && theoretical_u.contains(ref_alias) {
                        let mut sql = String::new();
                        sql.push_str("-- Valid Foreign Key Join test\n");
                        sql.push_str("-- This join should succeed since the referenced columns are from a relation\n");
                        sql.push_str("-- that both preserves all rows and preserves uniqueness.\n");
                        sql.push_str(&format!("SELECT COUNT(*)\nFROM v\nJOIN {} KEY ({}) -> v ({}_{});",
                            table.name, column.name, ref_alias, ref_col));
                        valid_test = Some(sql);
                    }
                    // Check if this referenced table is NOT both in A and U (negative test)
                    else if invalid_test.is_none() && (!theoretical_a.get(ref_alias).map(|s| s.contains(ref_alias)).unwrap_or(false) || !theoretical_u.contains(ref_alias)) {
                        let mut sql = String::new();
                        sql.push_str("-- Invalid Foreign Key Join test\n");
                        sql.push_str("-- This join should fail with 'virtual foreign key constraint violation'\n");
                        sql.push_str("-- since the referenced columns are from a relation that does NOT both\n");
                        sql.push_str("-- preserve all rows and preserve uniqueness.\n");
                        sql.push_str(&format!("SELECT COUNT(*)\nFROM v\nJOIN {} KEY ({}) -> v ({}_{});",
                            table.name, column.name, ref_alias, ref_col));
                        invalid_test = Some(sql);
                    }

                    // If we have both a valid and invalid test, we can stop
                    if valid_test.is_some() && invalid_test.is_some() {
                        break 'outer;
                    }
                }
            }
        }
    }

    (valid_test, invalid_test)
}
