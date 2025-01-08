use rand::distributions::{Distribution, Uniform};
use rand::{thread_rng, Rng};
use rand::seq::SliceRandom;
use std::sync::atomic::{AtomicUsize, Ordering};
use postgres::{Client, NoTls};
use std::env;
use std::fs::File;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use clap::Parser;

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

fn verify_derived_table(first_table: &str, joins: &[JoinInfo]) -> (std::collections::HashSet<String>, std::collections::HashSet<String>) {
    if joins.is_empty() {
        return (std::collections::HashSet::new(), std::collections::HashSet::new());
    }

    let mut a = std::collections::HashSet::from([first_table.to_string()]);
    let mut u = std::collections::HashSet::from([first_table.to_string()]);

    for join in joins.iter() {
        let (existing_alias, new_alias) = match join.arrow {
            FKDir::Backward => (join.existing_alias.clone(), join.new_alias.clone()),
            FKDir::Forward => (join.existing_alias.clone(), join.new_alias.clone()),
        };

        let cond = a.contains(&existing_alias) && join.fk_cols_not_null;

        match (join.join_type, join.arrow, cond) {
            (JoinType::Left, FKDir::Backward, _) => (),
            (JoinType::Left, FKDir::Forward, false) => (),
            (JoinType::Left, FKDir::Forward, true) => {
                a.insert(new_alias.clone());
            },
            (JoinType::Full, _, _) => {
                a.insert(new_alias.clone());
            },
            (JoinType::Inner, _, false) => {
                a.clear();
            },
            (JoinType::Inner, FKDir::Backward, true) => {
                a = std::collections::HashSet::from([existing_alias.clone()]);
            },
            (JoinType::Inner, FKDir::Forward, true) => {
                a = std::collections::HashSet::from([new_alias.clone()]);
            },
            (JoinType::Right, FKDir::Backward, false) => {
                a = std::collections::HashSet::from([new_alias.clone()]);
            },
            (JoinType::Right, FKDir::Backward, true) => {
                a = std::collections::HashSet::from([existing_alias.clone(), new_alias.clone()]);
            },
            (JoinType::Right, FKDir::Forward, _) => {
                a = std::collections::HashSet::from([new_alias.clone()]);
            },
        }

        if u.contains(&existing_alias) && join.fk_cols_unique {
            u.insert(new_alias.clone());
        } else if matches!(join.arrow, FKDir::Forward) && !u.contains(&existing_alias) && !join.fk_cols_unique {
            u.clear();
        } else if matches!(join.arrow, FKDir::Forward) && u.contains(&existing_alias) && !join.fk_cols_unique {
            u = std::collections::HashSet::from([new_alias.clone()]);
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
        let ref_table_is_used = used_aliases.iter().any(|a| a.table_name == rel.0);

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

fn main() {
    let args = Args::parse();

    let mut first_output = true;  // Track if this is our first output

    loop {
        let mut output = Vec::new();  // Buffer for SQL statements

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
                let view_sql = create_view_sql(&first_table, &joins, &table_aliases, &args.join_syntax);
                let verify_sql = create_verify_sql(&table_aliases);

                // Get theoretical results
                let (theoretical_a, theoretical_u) = verify_derived_table(&first_table, &joins);

                // Before executing view_sql, print the theoretical sets:
                if !first_output {
                    print!("\x1B[3F\x1B[0J");
                }
                println!("A: {}", format!("{{{}}}", theoretical_a.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")));
                println!("U: {}", format!("{{{}}}", theoretical_u.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(",")));
                println!("");  // Extra line for status messages
                first_output = false;

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
                        // Set statement timeout right after connecting
                        if let Err(e) = client.batch_execute(&format!("SET statement_timeout = '{}';", args.statement_timeout_ms)) {
                            let filename = save_error_sql(&output, &e, Some(&format!("SET statement_timeout = '{}';", args.statement_timeout_ms)));
                            eprintln!("SQL saved to {}", filename);
                            std::process::exit(1);
                        }

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
                                args.db_name, args.db_name, args.db_name)));
                            eprintln!("SQL saved to {}", filename);
                            std::process::exit(1);
                        }

                        // Execute statements and handle errors
                        for sql in &output {
                            if let Err(e) = client.batch_execute(sql) {
                                let filename = save_error_sql(&output, &e, Some(sql));
                                eprintln!("SQL saved to {}", filename);
                                std::process::exit(1);
                            }
                        }

                        // Execute view creation
                        if let Err(e) = client.batch_execute(&view_sql) {
                            let filename = save_error_sql(&output, &e, Some(&view_sql));
                            eprintln!("SQL saved to {}", filename);
                            std::process::exit(1);
                        }

                        // Execute verification query (now without setting timeout again)
                        match client.query_one(&verify_sql, &[]) {
                            Ok(row) => {
                                // Get comma-separated strings from query
                                let practical_a_str: Option<String> = row.get(0);
                                let practical_u_str: Option<String> = row.get(1);

                                // Convert to HashSets using as_ref() to borrow instead of move
                                let practical_a_set: std::collections::HashSet<String> = practical_a_str
                                    .as_ref()
                                    .map(|s| s.split(',').map(String::from).collect())
                                    .unwrap_or_default();
                                let practical_u_set: std::collections::HashSet<String> = practical_u_str
                                    .as_ref()
                                    .map(|s| s.split(',').map(String::from).collect())
                                    .unwrap_or_default();

                                // Create sorted string representations
                                let mut theoretical_a_vec: Vec<_> = theoretical_a.iter().collect();
                                let mut theoretical_u_vec: Vec<_> = theoretical_u.iter().collect();
                                theoretical_a_vec.sort();
                                theoretical_u_vec.sort();

                                let theoretical_a_str = format!("{{{}}}", theoretical_a_vec.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(","));
                                let theoretical_u_str = format!("{{{}}}", theoretical_u_vec.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(","));

                                // For practical sets, we can use the strings directly from the query since they're already sorted
                                let practical_a_str = format!("{{{}}}", practical_a_str.unwrap_or_default());
                                let practical_u_str = format!("{{{}}}", practical_u_str.unwrap_or_default());

                                // Print results
                                print!("\x1B[3F\x1B[0J");  // Move up 3 lines and clear them
                                println!("A: {} {} {}",
                                    theoretical_a_str,
                                    if theoretical_a.is_subset(&practical_a_set) { "⊆" } else { "⊈" },
                                    practical_a_str
                                );
                                println!("U: {} {} {}",
                                    theoretical_u_str,
                                    if theoretical_u.is_subset(&practical_u_set) { "⊆" } else { "⊈" },
                                    practical_u_str
                                );

                                // Check for errors
                                let mut error_msg = String::new();

                                if !theoretical_a.is_subset(&practical_a_set) {
                                    error_msg.push_str(&format!(
                                        "Theoretical A is not a subset of practical A\n\
                                        Extra in theoretical A: {:?}\n",
                                        theoretical_a.difference(&practical_a_set).collect::<Vec<_>>()
                                    ));
                                }
                                if !theoretical_u.is_subset(&practical_u_set) {
                                    error_msg.push_str(&format!(
                                        "Theoretical U is not a subset of practical U\n\
                                        Extra in theoretical U: {:?}\n",
                                        theoretical_u.difference(&practical_u_set).collect::<Vec<_>>()
                                    ));
                                }

                                if !error_msg.is_empty() {
                                    let filename = save_error_sql(&output, &error_msg, Some(&verify_sql));
                                    eprintln!("SQL saved to {}", filename);
                                    std::process::exit(1);
                                }

                                // Generate filename based on A and U set sizes
                                let filename = format!("joins_{}A_{}U.sql", theoretical_a.len(), theoretical_u.len());

                                // Check if this case already exists
                                if std::path::Path::new(&filename).exists() {
                                    println!("");
                                    continue;
                                }

                                // Log successful new case
                                let mut log = std::fs::File::create(&filename)
                                    .expect("Failed to create log file");

                                writeln!(log, "-- New case with {} elements in A and {} elements in U",
                                    theoretical_a.len(), theoretical_u.len()).expect("Failed to write to log");
                                writeln!(log, "-- Results:").expect("Failed to write to log");
                                writeln!(log, "-- Theoretical A: {}", theoretical_a_str).expect("Failed to write to log");
                                writeln!(log, "-- Practical A:   {}", practical_a_str).expect("Failed to write to log");
                                writeln!(log, "-- Theoretical U: {}", theoretical_u_str).expect("Failed to write to log");
                                writeln!(log, "-- Practical U:   {}", practical_u_str).expect("Failed to write to log");
                                writeln!(log).expect("Failed to write to log");

                                // Write all SQL statements
                                for sql in &output {
                                    writeln!(log, "{}", sql).expect("Failed to write to log");
                                }
                                writeln!(log, "\n\n").expect("Failed to write to log");  // Add extra newline before view
                                writeln!(log, "{}", view_sql).expect("Failed to write to log");
                                writeln!(log, "\n\n").expect("Failed to write to log");  // Add extra newline before verification
                                writeln!(log, "{}", verify_sql).expect("Failed to write to log");
                                writeln!(log, "\n").expect("Failed to write to log");

                                println!("Found new case! Saved to {}", filename);
                            }
                            Err(e) => {
                                if e.to_string().contains("statement timeout") {
                                    print!("\x1B[1F\x1B[0J");  // Move up 1 line and clear them
                                    println!("Verification query timed out after {} ms, skipping...", args.statement_timeout_ms);
                                    continue;  // Skip this case and continue with the next one
                                } else {
                                    let filename = save_error_sql(&output, &e, Some(&verify_sql));
                                    eprintln!("SQL saved to {}", filename);
                                    std::process::exit(1);
                                }
                            }
                        }
                    }
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
                        let filename = save_error_sql(&output, &e, Some(&conn_string));
                        eprintln!("SQL saved to {}", filename);
                        std::process::exit(1);
                    }
                }
            }
        }
    }
}

fn save_error_sql(output: &[String], error: &impl std::fmt::Display, failed_query: Option<&str>) -> String {
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

    // Write the failing query if provided
    if let Some(query) = failed_query {
        writeln!(file, "\n-- Query that caused the error:").expect("Failed to write comment");
        writeln!(file, "{}", query).expect("Failed to write failing query");
    }

    filename
}

// Add these helper functions
fn create_view_sql(first_table: &str, joins: &[JoinInfo], table_aliases: &[(String, String)], join_syntax: &str) -> String {
    let mut sql = String::from("-- This view represents the derived table created by the join sequence.\n");
    sql.push_str("-- Each table's ID is selected to track which rows from the base tables appear in the result.\n");

    if join_syntax.to_uppercase() == "KEY" {
        sql.push_str("-- The join sequence uses the KEY syntax to specify foreign key relationships.\n");
        sql.push_str("-- Arrows (->, <-) indicate the direction of the foreign key constraint.\n");
    } else {
        sql.push_str("-- The join sequence uses traditional ON syntax for joins.\n");
    }

    sql.push_str("CREATE VIEW v AS\nSELECT\n");

    let id_columns: Vec<_> = table_aliases.iter()
        .map(|(_, alias)| format!("    {}.id AS {}_id", alias, alias))
        .collect();
    sql.push_str(&id_columns.join(",\n"));
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
