//! Language-neutral compatibility-vector runner.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use pontmore_core::{
    DescriptorSelection, ErrorCode, Tag, validate_agent_definition, validate_escrow_descriptor,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct VectorFile {
    schema_version: u64,
    cases: Vec<VectorCase>,
}

#[derive(Debug, Deserialize)]
struct VectorCase {
    id: String,
    spec: String,
    input: VectorInput,
    expected: Expected,
}

#[derive(Debug, Deserialize)]
struct VectorInput {
    #[serde(rename = "type")]
    input_type: InputType,
    content: serde_json::Value,
    tags: Vec<Tag>,
    #[serde(default)]
    selection: Option<DescriptorSelection>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InputType {
    AgentDefinition,
    EscrowDescriptor,
}

#[derive(Debug, Deserialize)]
struct Expected {
    valid: bool,
    #[serde(default)]
    error_codes: BTreeSet<ErrorCode>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunSummary {
    pub files: usize,
    pub cases: usize,
    pub failures: Vec<String>,
}

impl RunSummary {
    #[must_use]
    pub fn passed(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Execute every `.json` vector file below `path`.
///
/// # Errors
///
/// Returns an error for unreadable directories/files or malformed vector files.
pub fn run_path(path: &Path) -> Result<RunSummary, String> {
    let mut paths = Vec::new();
    collect_json_files(path, &mut paths)?;
    paths.sort();

    let mut summary = RunSummary {
        files: paths.len(),
        cases: 0,
        failures: Vec::new(),
    };

    for path in paths {
        let bytes = fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?;
        let vectors: VectorFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{}: {error}", path.display()))?;
        if vectors.schema_version != 1 {
            return Err(format!(
                "{}: unsupported vector schema version {}",
                path.display(),
                vectors.schema_version
            ));
        }

        for case in &vectors.cases {
            summary.cases += 1;
            run_case(&path, case, &mut summary.failures)?;
        }
    }

    Ok(summary)
}

fn run_case(path: &Path, case: &VectorCase, failures: &mut Vec<String>) -> Result<(), String> {
    let content = serde_json::to_string(&case.input.content)
        .map_err(|error| format!("{}:{}: {error}", path.display(), case.id))?;
    let errors = match case.input.input_type {
        InputType::AgentDefinition => validate_agent_definition(&content, &case.input.tags),
        InputType::EscrowDescriptor => {
            validate_escrow_descriptor(&content, &case.input.tags, case.input.selection)
        }
    };
    let actual: BTreeSet<ErrorCode> = errors.iter().map(|error| error.code).collect();
    let actual_valid = errors.is_empty();
    if actual_valid != case.expected.valid || actual != case.expected.error_codes {
        failures.push(format!(
            "{}:{} [{}] expected valid={} errors={:?}; got valid={} errors={actual:?}",
            path.display(),
            case.id,
            case.spec,
            case.expected.valid,
            case.expected.error_codes,
            actual_valid
        ));
    }
    Ok(())
}

fn collect_json_files(path: &Path, output: &mut Vec<PathBuf>) -> Result<(), String> {
    if path.is_file() {
        if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            output.push(path.to_path_buf());
        }
        return Ok(());
    }

    let entries = fs::read_dir(path).map_err(|error| format!("{}: {error}", path.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("{}: {error}", path.display()))?;
        let child = entry.path();
        if child.is_dir() {
            collect_json_files(&child, output)?;
        } else if child
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            output.push(child);
        }
    }
    Ok(())
}
