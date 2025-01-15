use std::error::Error;
use std::fs::{self, File};
use std::io::Write;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use crate::FuzzerError;
use crate::ErrorContext;

/// Parses "t9: {t9}, t8: {t9}" into:
/// {
///    "t9": {"t9"},
///    "t8": {"t9"}
/// }
fn parse_a_map(content: &str) -> HashMap<String, HashSet<String>> {
    let mut result = HashMap::new();
    let mut remainder = content.trim();
    let mut segment_index = 0;

    while !remainder.is_empty() {
        let colon_pos = match remainder.find(':') {
            Some(pos) => pos,
            None => break,
        };
        let key = remainder[..colon_pos].trim().to_string();

        remainder = remainder[colon_pos + 1..].trim();

        let brace_pos = match remainder.find('{') {
            Some(pos) => pos,
            None => break,
        };

        // Skip the '{'
        remainder = remainder[brace_pos + 1..].trim();

        let mut brace_count = 1;
        let mut set_content = String::new();
        let mut i = 0;
        let chars: Vec<char> = remainder.chars().collect();

        while i < chars.len() && brace_count > 0 {
            match chars[i] {
                '{' => {
                    brace_count += 1;
                    set_content.push(chars[i]);
                },
                '}' => {
                    brace_count -= 1;
                    if brace_count > 0 {
                        set_content.push(chars[i]);
                    }
                },
                c => {
                    set_content.push(c);
                }
            }
            i += 1;
        }

        remainder = &remainder[i..].trim();

        let val_set = set_content
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .collect::<HashSet<_>>();

        result.insert(key.clone(), val_set);

        if remainder.starts_with(',') {
            remainder = &remainder[1..].trim();
        }

        segment_index += 1;
    }

    result
}

pub fn handle_fk_join_test(
    test_query: &str,
    view_sql: &str,
    table_sql: &str,
    is_valid: bool,
    expected_a: &HashMap<String, HashSet<String>>,
    expected_u: &HashSet<String>
) -> Result<(), Box<dyn Error>> {
    let temp_file = format!("fk_join_test_{}.sql", Uuid::new_v4());
    let mut file = File::create(&temp_file)?;

    // Write table creation + queries
    writeln!(file, "DROP SCHEMA IF EXISTS public CASCADE;")?;
    writeln!(file, "CREATE SCHEMA public;")?;
    writeln!(file, "SET client_min_messages TO notice;")?;
    writeln!(file, "{}", table_sql)?;
    writeln!(file, "{}", view_sql)?;
    writeln!(file, "\n{}", test_query)?;
    drop(file);

    let output = std::process::Command::new("psql")
        .args(["-a", "-f", &temp_file, "joinfuzzer"])
        .output()?;

    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let stderr_str = String::from_utf8_lossy(&output.stderr);

    let mut theoretical_a = HashMap::new();
    let mut theoretical_u = HashSet::new();

    for line in stderr_str.lines() {
        if line.contains("NOTICE:") {
            let msg = line.split("NOTICE:").nth(1).unwrap_or("").trim();

            if msg.contains("A =>") {
                theoretical_a.clear();
                let set_str = msg.split("A =>").nth(1).unwrap_or("").trim();
                let content = set_str.trim_matches(|c| c == '{' || c == '}');
                let table_map = parse_a_map(content);
                theoretical_a = table_map;

            } else if msg.contains("U =>") {
                theoretical_u.clear();
                let set_str = msg.split("U =>").nth(1).unwrap_or("").trim();
                let content = set_str.trim_matches(|c| c == '{' || c == '}');

                if !content.is_empty() {
                    for table in content.split(',').map(|s| s.trim()) {
                        if !table.is_empty() {
                            theoretical_u.insert(table.to_string());
                        }
                    }
                }
            }
        }
    }

    fs::remove_file(&temp_file)?;

    if !is_valid && !stderr_str.contains("virtual foreign key constraint violation") {
        return Err(Box::new(FuzzerError::ComparisonError(
            format!("Invalid foreign key join failed with wrong error: {}", stderr_str),
            ErrorContext {
                output: Vec::new(),
                query: Some(test_query.to_string()),
                view_sql: Some(view_sql.to_string()),
            }
        )));
    }

    if theoretical_a != *expected_a || theoretical_u != *expected_u {
        return Err(Box::new(FuzzerError::ComparisonError(
            format!(
                "-- Theoretical sets from PostgreSQL don't match expected sets.\n\
                 -- Expected A: {:?}\n-- Got A: {:?}\n\
                 -- Expected U: {:?}\n-- Got U: {:?}",
                expected_a, theoretical_a, expected_u, theoretical_u
            ),
            ErrorContext {
                output: Vec::new(),
                query: Some(test_query.to_string()),
                view_sql: Some(view_sql.to_string()),
            }
        )));
    }

    Ok(())
}
