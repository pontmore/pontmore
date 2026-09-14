//! Language-neutral compatibility-vector runner.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use pontmore_core::{
    CoordinationState, DescriptorBinding, DescriptorSelection, ErrorCode, Tag,
    validate_agent_definition, validate_escrow_descriptor, validate_swap_coordination,
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
    #[serde(default)]
    content: Option<serde_json::Value>,
    #[serde(default)]
    tags: Vec<Tag>,
    #[serde(default)]
    selection: Option<DescriptorSelection>,
    #[serde(default)]
    root: Option<nostr::event::Event>,
    #[serde(default)]
    actions: Vec<nostr::event::Event>,
    #[serde(default)]
    descriptor: Option<DescriptorBinding>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InputType {
    AgentDefinition,
    EscrowDescriptor,
    SwapCoordination,
}

#[derive(Debug, Deserialize)]
struct Expected {
    valid: bool,
    #[serde(default)]
    error_codes: BTreeSet<ErrorCode>,
    #[serde(default)]
    state: Option<CoordinationState>,
    #[serde(default)]
    rejection_codes: BTreeSet<ErrorCode>,
    #[serde(default)]
    fork: Option<bool>,
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
    if matches!(case.input.input_type, InputType::SwapCoordination) {
        return run_coordination_case(path, case, failures);
    }
    let content = case
        .input
        .content
        .as_ref()
        .ok_or_else(|| format!("{}:{}: missing content", path.display(), case.id))?;
    let content = serde_json::to_string(content)
        .map_err(|error| format!("{}:{}: {error}", path.display(), case.id))?;
    let errors = match case.input.input_type {
        InputType::AgentDefinition => validate_agent_definition(&content, &case.input.tags),
        InputType::EscrowDescriptor => {
            validate_escrow_descriptor(&content, &case.input.tags, case.input.selection)
        }
        InputType::SwapCoordination => unreachable!("handled above"),
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

fn run_coordination_case(
    path: &Path,
    case: &VectorCase,
    failures: &mut Vec<String>,
) -> Result<(), String> {
    let root = case
        .input
        .root
        .as_ref()
        .ok_or_else(|| format!("{}:{}: missing root", path.display(), case.id))?;
    let descriptor = case
        .input
        .descriptor
        .as_ref()
        .ok_or_else(|| format!("{}:{}: missing descriptor", path.display(), case.id))?;
    match validate_swap_coordination(root, &case.input.actions, descriptor) {
        Ok(result) => {
            let rejections: BTreeSet<ErrorCode> =
                result.rejections.iter().map(|item| item.code).collect();
            let matches = case.expected.valid
                && case.expected.error_codes.is_empty()
                && case.expected.state == Some(result.state)
                && case.expected.rejection_codes == rejections
                && case
                    .expected
                    .fork
                    .is_none_or(|fork| fork == result.fork.is_some());
            if !matches {
                failures.push(format!(
                    "{}:{} [{}] expected state={:?} rejections={:?} fork={:?}; got state={:?} rejections={:?} fork={}",
                    path.display(),
                    case.id,
                    case.spec,
                    case.expected.state,
                    case.expected.rejection_codes,
                    case.expected.fork,
                    result.state,
                    rejections,
                    result.fork.is_some()
                ));
            }
        }
        Err(errors) => {
            let actual: BTreeSet<ErrorCode> = errors.iter().map(|error| error.code).collect();
            if case.expected.valid || actual != case.expected.error_codes {
                failures.push(format!(
                    "{}:{} [{}] expected valid={} errors={:?}; got root errors={actual:?}",
                    path.display(),
                    case.id,
                    case.spec,
                    case.expected.valid,
                    case.expected.error_codes
                ));
            }
        }
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
