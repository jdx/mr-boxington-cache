#[cfg(test)]
pub use mbx_cache_protocol::ActionPrediction as TaskActionPrediction;
pub use mbx_cache_protocol::DigestAlgorithm as Algorithm;
pub use mbx_cache_protocol::{
    ActionResult, CcMetadata, Digest, Directory, RustcMetadata, TaskActionManifest,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskAction {
    pub version: u8,
    pub kind: String,
    pub task: String,
    pub phase: TaskPhase,
    pub run: Vec<TaskRunEntry>,
    pub args: Vec<String>,
    pub shell: Option<String>,
    pub outputs: Vec<String>,
    pub root: String,
    pub source_hash: String,
    #[serde(default)]
    pub dependency_keys: Vec<String>,
    pub environment: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pub command_inputs: Vec<TaskCommandInput>,
    pub vars: BTreeMap<String, String>,
    pub tools: Vec<String>,
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskPhase {
    Normal,
    Post,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TaskRunEntry {
    Script(String),
    Single(TaskRunSingle),
    Group(TaskRunGroup),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunSingle {
    pub task: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRunGroup {
    pub tasks: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskCommandInput {
    pub command: String,
    pub stdout_hash: String,
    pub stderr_hash: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskMetadata {
    pub version: u8,
    pub kind: String,
    pub task_identity: String,
    pub roots: Vec<String>,
    pub output: Vec<TaskOutput>,
    pub restored_bytes: u64,
    pub execution_duration_ns: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskOutput {
    pub stream: TaskOutputStream,
    pub line: String,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskOutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustcAction {
    pub version: u8,
    pub kind: String,
    pub adapter_version: u8,
    pub compiler: RustcCompiler,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, Option<String>>,
    pub inputs: Vec<RustcInput>,
    #[serde(default)]
    pub linker: Option<RustcLinkerIdentity>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustcCompiler {
    pub toolchain: String,
    pub rustc_version: String,
    pub host: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustcInput {
    pub path: String,
    /// Identifies local input content for the action key. This is not a CAS
    /// reference: compiler source inputs are never uploaded to the service.
    pub digest: Digest,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustcLinkerIdentity {
    pub driver: String,
    pub driver_version: String,
    pub linker_version: String,
    #[serde(default)]
    pub crt_objects: BTreeMap<String, Digest>,
    #[serde(default)]
    pub sdk: Option<String>,
    #[serde(default)]
    pub deployment_target: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CcAction {
    pub version: u8,
    pub kind: String,
    pub adapter_version: u8,
    #[serde(default)]
    pub assembly_input_model: Option<u8>,
    pub compiler: CcCompiler,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, Option<String>>,
    pub inputs: Vec<CcInput>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CcCompiler {
    pub assembler: String,
    pub family: String,
    pub target: String,
    pub version_text: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CcInput {
    pub digest: Digest,
    pub path: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildScriptAction {
    pub version: u8,
    pub kind: String,
    pub binary_action: Digest,
    pub cargo_environment: BTreeMap<String, Option<String>>,
    pub environment: BTreeMap<String, Option<String>>,
    pub inputs: BTreeMap<String, BuildScriptInput>,
    pub out_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BuildScriptInput {
    Missing,
    File {
        digest: Digest,
    },
    Directory {
        digest: Digest,
    },
    Symlink {
        target: String,
        referent: Box<BuildScriptInput>,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BuildScriptMetadata {
    pub version: u8,
    pub kind: String,
    pub stdout: Digest,
    pub stderr: Digest,
}

fn valid_string(value: &str) -> bool {
    !value.contains('\0')
}

fn valid_strings(values: &[String]) -> bool {
    values.iter().all(|value| valid_string(value))
}

fn valid_string_map(values: &BTreeMap<String, String>) -> bool {
    values
        .iter()
        .all(|(key, value)| valid_string(key) && valid_string(value))
}

impl TaskAction {
    pub fn validate(&self) -> bool {
        self.version == 1
            && self.kind == "task"
            && valid_string(&self.task)
            && matches!(self.phase, TaskPhase::Normal | TaskPhase::Post)
            && self.run.iter().all(TaskRunEntry::validate)
            && valid_strings(&self.args)
            && self.shell.as_deref().is_none_or(valid_string)
            && valid_strings(&self.outputs)
            && valid_string(&self.root)
            && valid_string(&self.source_hash)
            && valid_strings(&self.dependency_keys)
            && self
                .environment
                .iter()
                .all(|(key, value)| valid_string(key) && value.as_deref().is_none_or(valid_string))
            && self.command_inputs.iter().all(TaskCommandInput::validate)
            && valid_string_map(&self.vars)
            && valid_strings(&self.tools)
            && valid_string(&self.os)
            && valid_string(&self.arch)
    }
}

impl TaskRunEntry {
    fn validate(&self) -> bool {
        match self {
            Self::Script(script) => valid_string(script),
            Self::Single(entry) => {
                valid_string(&entry.task)
                    && valid_strings(&entry.args)
                    && valid_string_map(&entry.env)
            }
            Self::Group(entry) => valid_strings(&entry.tasks),
        }
    }
}

impl TaskCommandInput {
    fn validate(&self) -> bool {
        valid_string(&self.command)
            && valid_string(&self.stdout_hash)
            && valid_string(&self.stderr_hash)
    }
}

impl TaskMetadata {
    pub fn validate(&self) -> bool {
        // Serde's u64 deserialization is the schema validation for these numeric fields.
        let _ = (self.restored_bytes, self.execution_duration_ns);
        self.version == 1
            && self.kind == "task"
            && valid_string(&self.task_identity)
            && valid_strings(&self.roots)
            && self.output.iter().all(TaskOutput::validate)
    }
}

impl TaskOutput {
    fn validate(&self) -> bool {
        matches!(
            self.stream,
            TaskOutputStream::Stdout | TaskOutputStream::Stderr
        ) && valid_string(&self.line)
    }
}

impl RustcAction {
    pub fn validate(&self) -> bool {
        let mut input_paths = HashSet::new();
        self.version == 1
            && self.kind == "rustc"
            && self.adapter_version > 0
            && self.compiler.validate()
            && valid_strings(&self.arguments)
            && self.environment.iter().all(|(key, value)| {
                !key.is_empty() && valid_string(key) && value.as_deref().is_none_or(valid_string)
            })
            && !self.inputs.is_empty()
            && self
                .inputs
                .iter()
                .all(|input| input.validate() && input_paths.insert(&input.path))
            && self
                .linker
                .as_ref()
                .is_none_or(RustcLinkerIdentity::validate)
    }
}

impl RustcCompiler {
    fn validate(&self) -> bool {
        [&self.toolchain, &self.rustc_version, &self.host]
            .into_iter()
            .all(|value| !value.is_empty() && valid_string(value))
    }
}

impl RustcInput {
    fn validate(&self) -> bool {
        valid_normalized_path(&self.path) && self.digest.validate().is_ok()
    }
}

impl RustcLinkerIdentity {
    fn validate(&self) -> bool {
        [&self.driver, &self.driver_version, &self.linker_version]
            .into_iter()
            .all(|value| !value.is_empty() && valid_string(value))
            && self.crt_objects.iter().all(|(name, digest)| {
                !name.is_empty() && valid_string(name) && digest.validate().is_ok()
            })
            && self
                .sdk
                .as_deref()
                .is_none_or(|value| !value.is_empty() && valid_string(value))
            && self
                .deployment_target
                .as_deref()
                .is_none_or(|value| !value.is_empty() && valid_string(value))
    }
}

impl CcAction {
    pub fn validate(&self) -> bool {
        let mut input_paths = HashSet::new();
        self.version == 1
            && self.kind == "cc"
            && self.adapter_version > 0
            && self.assembly_input_model.is_none_or(|version| version == 1)
            && self.compiler.validate()
            && valid_strings(&self.arguments)
            && valid_optional_string_map(&self.environment)
            && !self.inputs.is_empty()
            && self
                .inputs
                .iter()
                .all(|input| input.validate() && input_paths.insert(&input.path))
    }
}

impl CcCompiler {
    fn validate(&self) -> bool {
        [
            &self.assembler,
            &self.family,
            &self.target,
            &self.version_text,
        ]
        .into_iter()
        .all(|value| !value.is_empty() && valid_string(value))
    }
}

impl CcInput {
    fn validate(&self) -> bool {
        // System headers are intentionally left as host paths, and include
        // manifests use an adapter-owned prefix. These are key inputs rather
        // than paths the server reads, so only their serialized integrity is
        // relevant here.
        !self.path.is_empty() && valid_string(&self.path) && self.digest.validate().is_ok()
    }
}

impl BuildScriptAction {
    pub fn validate(&self) -> bool {
        self.version == 2
            && self.kind == "build-script"
            && self.binary_action.validate().is_ok()
            && valid_optional_string_map(&self.cargo_environment)
            && valid_optional_string_map(&self.environment)
            && self
                .inputs
                .iter()
                .all(|(path, input)| valid_string(path) && !path.is_empty() && input.validate(0))
            && self.out_dir.as_deref().is_none_or(valid_string)
    }
}

impl BuildScriptInput {
    fn validate(&self, depth: usize) -> bool {
        if depth > 64 {
            return false;
        }
        match self {
            Self::Missing => true,
            Self::File { digest } | Self::Directory { digest } => digest.validate().is_ok(),
            Self::Symlink { target, referent } => {
                valid_string(target) && referent.validate(depth + 1)
            }
        }
    }
}

impl BuildScriptMetadata {
    pub fn validate(&self) -> bool {
        self.version == 1
            && self.kind == "build-script"
            && self.stdout.validate().is_ok()
            && self.stderr.validate().is_ok()
    }
}

fn valid_optional_string_map(values: &BTreeMap<String, Option<String>>) -> bool {
    values.iter().all(|(key, value)| {
        !key.is_empty() && valid_string(key) && value.as_deref().is_none_or(valid_string)
    })
}

fn valid_normalized_path(path: &str) -> bool {
    let Some((placeholder, suffix)) = path
        .strip_prefix("${")
        .and_then(|path| path.split_once('}'))
    else {
        return false;
    };
    if placeholder.is_empty()
        || !placeholder
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return false;
    }
    suffix.is_empty()
        || suffix.strip_prefix('/').is_some_and(|suffix| {
            !suffix.is_empty()
                && !suffix.contains(['\\', '\0'])
                && suffix
                    .split('/')
                    .all(|component| !component.is_empty() && component != "." && component != "..")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_lowercase_hex_digests() {
        let valid = Digest {
            algorithm: Algorithm::Blake3.into(),
            hash: "a".repeat(64),
            size: 42,
        };
        assert!(valid.validate().is_ok());
        let invalid = Digest {
            hash: "A".repeat(64),
            ..valid
        };
        assert!(invalid.validate().is_err());
        let unknown = Digest {
            algorithm: "..".into(),
            hash: "a".repeat(64),
            size: 42,
        };
        assert!(unknown.validate().is_err());
    }
}
