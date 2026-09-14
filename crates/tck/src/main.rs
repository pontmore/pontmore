use std::{env, path::Path, process::ExitCode};

use pontmore_tck::run_path;

fn main() -> ExitCode {
    let path = env::args().nth(1).unwrap_or_else(|| "tck/vectors".into());
    match run_path(Path::new(&path)) {
        Ok(summary) if summary.passed() => {
            println!(
                "Pontmore TCK: {} cases in {} files passed",
                summary.cases, summary.files
            );
            ExitCode::SUCCESS
        }
        Ok(summary) => {
            eprintln!(
                "Pontmore TCK: {} of {} cases failed",
                summary.failures.len(),
                summary.cases
            );
            for failure in summary.failures {
                eprintln!("- {failure}");
            }
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("Pontmore TCK error: {error}");
            ExitCode::FAILURE
        }
    }
}
