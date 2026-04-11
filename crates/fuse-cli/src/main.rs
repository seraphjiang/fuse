// SPDX-License-Identifier: Apache-2.0

//! Fuse CLI — query any datasource from the command line.
//!
//! Usage:
//!   fuse query "SELECT * FROM cluster_a.logs LIMIT 10"
//!   fuse query --format ppl "source = cluster_a.logs | head 10"
//!   fuse explain "SELECT * FROM cluster_a.logs JOIN dynamodb.users ON ..."
//!   fuse health
//!   fuse datasources
//!   fuse datasources cluster_a          # list tables
//!   fuse datasources cluster_a logs     # list fields

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "fuse", about = "Fuse federated query engine CLI")]
struct Cli {
    /// Fuse server URL
    #[arg(long, env = "FUSE_URL", default_value = "http://localhost:9400")]
    url: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Execute a SQL or PPL query
    Query {
        /// The query string
        query: String,
        /// Query format: sql or ppl
        #[arg(short, long, default_value = "sql")]
        format: String,
    },
    /// Show the execution plan for a query
    Explain {
        /// The query string
        query: String,
        /// Query format: sql or ppl
        #[arg(short, long, default_value = "sql")]
        format: String,
    },
    /// Check server health and connector status
    Health,
    /// List datasources, tables, or fields
    Datasources {
        /// Datasource ID (optional — list tables)
        datasource: Option<String>,
        /// Table name (optional — list fields)
        table: Option<String>,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = reqwest::Client::new();

    let result = match cli.command {
        Command::Query { query, format } => run_query(&client, &cli.url, &query, &format).await,
        Command::Explain { query, format } => run_explain(&client, &cli.url, &query, &format).await,
        Command::Health => run_health(&client, &cli.url).await,
        Command::Datasources { datasource, table } => {
            run_datasources(&client, &cli.url, datasource.as_deref(), table.as_deref()).await
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

async fn run_query(
    client: &reqwest::Client,
    url: &str,
    query: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let res: serde_json::Value = client
        .post(format!("{url}/api/fuse/query"))
        .json(&serde_json::json!({ "query": query, "format": format }))
        .send()
        .await?
        .json()
        .await?;

    if let Some(err) = res.get("error") {
        eprintln!("Query error: {}", err.as_str().unwrap_or("unknown"));
        std::process::exit(1);
    }

    let cols = res["columns"].as_array();
    let rows = res["rows"].as_array();

    if let (Some(cols), Some(rows)) = (cols, rows) {
        // Print header
        let headers: Vec<&str> = cols.iter().filter_map(|c| c.as_str()).collect();
        println!("{}", headers.join("\t"));
        println!("{}", headers.iter().map(|h| "-".repeat(h.len().max(8))).collect::<Vec<_>>().join("\t"));
        // Print rows
        for row in rows {
            if let Some(cells) = row.as_array() {
                let line: Vec<String> = cells.iter().map(|c| match c {
                    serde_json::Value::Null => "NULL".into(),
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                }).collect();
                println!("{}", line.join("\t"));
            }
        }
        // Metadata
        if let Some(meta) = res.get("metadata") {
            let total = meta.get("total_rows").and_then(|v| v.as_u64()).unwrap_or(rows.len() as u64);
            eprint!("{total} rows");
            if let Some(ms) = meta.get("elapsed_ms").and_then(|v| v.as_u64()) {
                eprint!(" in {ms}ms");
            }
            eprintln!();
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&res)?);
    }
    Ok(())
}

async fn run_explain(
    client: &reqwest::Client,
    url: &str,
    query: &str,
    format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let res: serde_json::Value = client
        .post(format!("{url}/api/fuse/query/explain"))
        .json(&serde_json::json!({ "query": query, "format": format }))
        .send()
        .await?
        .json()
        .await?;

    if let Some(plan) = res.get("plan").and_then(|p| p.as_str()) {
        println!("{plan}");
    } else {
        println!("{}", serde_json::to_string_pretty(&res)?);
    }
    Ok(())
}

async fn run_health(
    client: &reqwest::Client,
    url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let res: serde_json::Value = client
        .get(format!("{url}/api/fuse/health"))
        .send()
        .await?
        .json()
        .await?;

    let status = res["status"].as_str().unwrap_or("unknown");
    println!("Status: {status}");

    if let Some(connectors) = res.get("connectors").and_then(|c| c.as_object()) {
        for (name, info) in connectors {
            let st = info["status"].as_str().unwrap_or("?");
            let icon = if st == "connected" { "✓" } else { "✗" };
            println!("  {icon} {name}: {st}");
        }
    }
    Ok(())
}

async fn run_datasources(
    client: &reqwest::Client,
    url: &str,
    datasource: Option<&str>,
    table: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = match (datasource, table) {
        (Some(ds), Some(tbl)) => format!("{url}/api/fuse/datasources/{ds}/schemas/{tbl}/fields"),
        (Some(ds), None) => format!("{url}/api/fuse/datasources/{ds}/schemas"),
        (None, _) => format!("{url}/api/fuse/datasources"),
    };

    let res: serde_json::Value = client.get(&endpoint).send().await?.json().await?;

    if let Some(arr) = res.as_array() {
        for item in arr {
            match item {
                serde_json::Value::String(s) => println!("{s}"),
                serde_json::Value::Object(obj) => {
                    let name = obj.get("name").or(obj.get("id")).and_then(|v| v.as_str()).unwrap_or("?");
                    if let Some(ft) = obj.get("field_type").or(obj.get("type")).and_then(|v| v.as_str()) {
                        println!("{name}: {ft}");
                    } else {
                        println!("{name}");
                    }
                }
                other => println!("{other}"),
            }
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&res)?);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn test_cli_parse_query() {
        let cli = Cli::parse_from(["fuse", "query", "SELECT 1"]);
        assert!(matches!(cli.command, Command::Query { .. }));
    }

    #[test]
    fn test_cli_parse_query_with_format() {
        let cli = Cli::parse_from(["fuse", "query", "-f", "ppl", "source = logs"]);
        if let Command::Query { format, query } = cli.command {
            assert_eq!(format, "ppl");
            assert_eq!(query, "source = logs");
        } else {
            panic!("expected Query");
        }
    }

    #[test]
    fn test_cli_parse_explain() {
        let cli = Cli::parse_from(["fuse", "explain", "SELECT * FROM t"]);
        assert!(matches!(cli.command, Command::Explain { .. }));
    }

    #[test]
    fn test_cli_parse_health() {
        let cli = Cli::parse_from(["fuse", "health"]);
        assert!(matches!(cli.command, Command::Health));
    }

    #[test]
    fn test_cli_parse_datasources() {
        let cli = Cli::parse_from(["fuse", "datasources"]);
        if let Command::Datasources { datasource, table } = cli.command {
            assert!(datasource.is_none());
            assert!(table.is_none());
        } else {
            panic!("expected Datasources");
        }
    }

    #[test]
    fn test_cli_parse_datasources_with_args() {
        let cli = Cli::parse_from(["fuse", "datasources", "cluster_a", "logs"]);
        if let Command::Datasources { datasource, table } = cli.command {
            assert_eq!(datasource.as_deref(), Some("cluster_a"));
            assert_eq!(table.as_deref(), Some("logs"));
        } else {
            panic!("expected Datasources");
        }
    }

    #[test]
    fn test_cli_default_url() {
        let cli = Cli::parse_from(["fuse", "health"]);
        assert_eq!(cli.url, "http://localhost:9400");
    }

    #[test]
    fn test_cli_custom_url() {
        let cli = Cli::parse_from(["fuse", "--url", "http://fuse:9400", "health"]);
        assert_eq!(cli.url, "http://fuse:9400");
    }

    #[test]
    fn test_cli_no_subcommand_fails() {
        let result = Cli::try_parse_from(["fuse"]);
        assert!(result.is_err(), "missing subcommand should fail");
    }

    #[test]
    fn test_cli_query_missing_arg_fails() {
        let result = Cli::try_parse_from(["fuse", "query"]);
        assert!(result.is_err(), "query without string should fail");
    }

    #[test]
    fn test_cli_explain_missing_arg_fails() {
        let result = Cli::try_parse_from(["fuse", "explain"]);
        assert!(result.is_err(), "explain without string should fail");
    }

    #[test]
    fn test_cli_unknown_subcommand_fails() {
        let result = Cli::try_parse_from(["fuse", "bogus"]);
        assert!(result.is_err(), "unknown subcommand should fail");
    }

    #[test]
    fn test_cli_datasources_table_without_ds() {
        // Can't provide table without datasource (positional order)
        let cli = Cli::parse_from(["fuse", "datasources", "myds"]);
        if let Command::Datasources { datasource, table } = cli.command {
            assert_eq!(datasource.as_deref(), Some("myds"));
            assert!(table.is_none());
        } else {
            panic!("expected Datasources");
        }
    }
}
