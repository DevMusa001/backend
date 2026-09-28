//! Operational CLI for the Zenith backend: recovery and maintenance jobs
//! that need direct database access outside the request path.
//!
//! Subcommands:
//!   zenith-admin readmodels rebuild   Rebuild the CQRS read models from source
//!   zenith-admin readmodels check     Report read-model drift without fixing it

use std::env;

async fn open_db() -> sqlx::SqlitePool {
    dotenvy::dotenv().ok();
    let database_url =
        env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://zenith.db".to_string());
    zenith_backend::db::init_pool(&database_url).await
}

#[tokio::main]
async fn main() {
    zenith_backend::init_tracing();

    let args: Vec<String> = env::args().skip(1).collect();
    let usage = "usage: zenith-admin readmodels <rebuild|check>";

    let Some((command, rest)) = args.split_first() else {
        eprintln!("{usage}");
        std::process::exit(2);
    };

    match command.as_str() {
        "readmodels" => match rest.first().map(String::as_str) {
            Some("rebuild") => {
                let db = open_db().await;
                zenith_backend::readmodels::rebuild_all(&db)
                    .await
                    .expect("readmodels rebuild failed");
                println!("read models rebuilt from source");
            }
            Some("check") => {
                let db = open_db().await;
                let problems = zenith_backend::readmodels::check_consistency(&db)
                    .await
                    .expect("readmodels check failed");
                if problems.is_empty() {
                    println!("read models consistent with source");
                } else {
                    for problem in &problems {
                        println!("{problem}");
                    }
                    std::process::exit(1);
                }
            }
            _ => {
                eprintln!("{usage}");
                std::process::exit(2);
            }
        },
        _ => {
            eprintln!("{usage}");
            std::process::exit(2);
        }
    }
}
