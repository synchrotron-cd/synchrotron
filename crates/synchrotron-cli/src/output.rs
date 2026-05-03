//! Output rendering for the CLI.
//!
//! Every command builds a [`serde_json::Value`] response and hands it
//! to [`print`], which serializes to JSON, YAML, or a column-aligned
//! table according to the user's `--output` flag. The table renderer
//! takes a `columns` spec so each command picks the fields it wants
//! to surface — JSON/YAML always include the full payload.
//!
//! Tables are produced manually rather than via a crate: padding to
//! the longest cell per column and joining with two-space gutters is
//! sufficient and avoids pulling in a transitive terminal-detection
//! dependency for what is essentially a debug-friendly view.

use anyhow::Result;
use clap::ValueEnum;
use serde_json::Value;

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Table,
    Json,
    Yaml,
}

/// One column in a table: human header + the JSON pointer-style key
/// path (slash-separated) used to extract the cell from each row.
pub struct Column {
    pub header: &'static str,
    pub path: &'static str,
}

/// Print `value` according to `format`. For `Table`, expects either a
/// JSON array of objects (one row each) OR an object containing an
/// array under a single key — the array is auto-detected. `columns`
/// is ignored for JSON/YAML output.
pub fn print(format: OutputFormat, value: &Value, columns: &[Column]) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(value)?);
        }
        OutputFormat::Yaml => {
            // serde_yaml_ng can serialize any serde_json::Value.
            print!("{}", serde_yaml_ng::to_string(value)?);
        }
        OutputFormat::Table => {
            print_table(value, columns)?;
        }
    }
    Ok(())
}

fn print_table(value: &Value, columns: &[Column]) -> Result<()> {
    // Allow a wrapper object like `{"apps": [...]}` — pull out the
    // first array we find, otherwise treat the value itself as a
    // single-row table.
    let rows: Vec<&Value> = if let Some(arr) = value.as_array() {
        arr.iter().collect()
    } else if let Some(obj) = value.as_object() {
        if let Some(arr) = obj.values().find_map(|v| v.as_array()) {
            arr.iter().collect()
        } else {
            vec![value]
        }
    } else {
        vec![value]
    };

    if rows.is_empty() {
        println!("(no rows)");
        return Ok(());
    }

    let mut cells: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for row in &rows {
        let mut cs = Vec::with_capacity(columns.len());
        for c in columns {
            cs.push(extract(row, c.path));
        }
        cells.push(cs);
    }

    let mut widths: Vec<usize> = columns.iter().map(|c| c.header.len()).collect();
    for row in &cells {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.len());
        }
    }

    let render = |row: &[String]| {
        row.iter()
            .enumerate()
            .map(|(i, cell)| format!("{:<width$}", cell, width = widths[i]))
            .collect::<Vec<_>>()
            .join("  ")
    };

    let header_row: Vec<String> = columns.iter().map(|c| c.header.to_string()).collect();
    println!("{}", render(&header_row).trim_end());
    for row in &cells {
        println!("{}", render(row).trim_end());
    }
    Ok(())
}

fn extract(value: &Value, path: &str) -> String {
    let mut cur = value;
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        cur = match cur.get(seg) {
            Some(v) => v,
            None => return String::from("-"),
        };
    }
    match cur {
        Value::Null => "-".into(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        other => other.to_string(),
    }
}
