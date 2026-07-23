use std::{
    env,
    error::Error,
    ffi::{OsStr, OsString},
    fmt, fs,
    path::{Path, PathBuf},
    process::{Command, ExitStatus},
};

mod header_conformance;
mod header_fuzz;

const DEFAULT_FEATURES: &str = "default-release-binaries";
const DEFAULT_UBUNTU_IMAGE: &str = "ubuntu:22.04";
const DEFAULT_RUST_VERSION: &str = "1.91";
const DEFAULT_IMAGE_TAG: &str = "zebra-ubuntu-package:local";
const OUTPUT_BINARY_NAME: &str = "zakurad";

type BoxError = Box<dyn Error>;

fn main() {
    if let Err(error) = try_main() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn try_main() -> Result<(), BoxError> {
    let mut args = env::args().skip(1);

    match args.next().as_deref() {
        Some("package") if args.next().as_deref() == Some("ubuntu") => {
            if args.next().is_some() {
                return Err(Box::new(UsageError(
                    "unexpected extra arguments for `cargo xtask package ubuntu`",
                )));
            }

            package_ubuntu()
        }
        Some("header-conformance") => {
            let rule_id = args.next();

            if args.next().is_some() {
                return Err(Box::new(UsageError(
                    "expected at most one rule ID after `cargo xtask header-conformance`",
                )));
            }

            header_conformance::run(&repo_root()?, rule_id.as_deref())
        }
        Some("minimize-header-fuzz") => {
            let artifact = args.next().ok_or_else(|| {
                Box::new(UsageError(
                    "expected an artifact after `cargo xtask minimize-header-fuzz`",
                )) as BoxError
            })?;
            if args.next().is_some() {
                return Err(Box::new(UsageError(
                    "expected exactly one artifact after `cargo xtask minimize-header-fuzz`",
                )));
            }
            header_fuzz::minimize(&repo_root()?, Path::new(&artifact))
        }
        Some("-h" | "--help") | None => {
            if args.next().is_some() {
                return Err(Box::new(UsageError(
                    "help does not accept additional arguments",
                )));
            }

            print_help();
            Ok(())
        }
        _ => Err(Box::new(UsageError("unknown xtask command"))),
    }
}

fn package_ubuntu() -> Result<(), BoxError> {
    let repo_root = repo_root()?;
    let output_dir = repo_root.join("target").join("ubuntu");
    let output_path = output_dir.join(OUTPUT_BINARY_NAME);
    let dockerfile = repo_root.join("docker").join("ubuntu-package.Dockerfile");

    fs::create_dir_all(&output_dir)?;
    if output_path.is_file() {
        fs::remove_file(&output_path)?;
    }

    run_command(
        Command::new("docker")
            .arg("build")
            .arg("--file")
            .arg(&dockerfile)
            .arg("--tag")
            .arg(DEFAULT_IMAGE_TAG)
            .arg("--build-arg")
            .arg(format!("UBUNTU_IMAGE={DEFAULT_UBUNTU_IMAGE}"))
            .arg("--build-arg")
            .arg(format!("RUST_VERSION={DEFAULT_RUST_VERSION}"))
            .arg("--build-arg")
            .arg(format!("FEATURES={DEFAULT_FEATURES}"))
            .arg(&repo_root),
    )?;

    let container_id = command_output(
        Command::new("docker")
            .arg("create")
            .arg(DEFAULT_IMAGE_TAG)
            .arg("true"),
    )?;

    let container_id = container_id.trim();

    if container_id.is_empty() {
        return Err("docker create did not return a container id".into());
    }

    let copy_result = run_command(
        Command::new("docker")
            .arg("cp")
            .arg(format!("{container_id}:/zakurad"))
            .arg(&output_path),
    );

    let remove_result = run_command(Command::new("docker").arg("rm").arg(container_id));

    copy_result?;
    remove_result?;

    println!("Ubuntu package written to {}", output_path.display());

    Ok(())
}

fn repo_root() -> Result<PathBuf, BoxError> {
    let xtask_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let repo_root = xtask_dir
        .parent()
        .ok_or("xtask crate should live directly under the workspace root")?;

    Ok(repo_root.to_path_buf())
}

fn run_command(command: &mut Command) -> Result<(), BoxError> {
    print_command(command);

    let status = command.status()?;

    if status.success() {
        Ok(())
    } else {
        Err(Box::new(CommandError::new(command, status)))
    }
}

fn command_output(command: &mut Command) -> Result<String, BoxError> {
    print_command(command);

    let output = command.output()?;

    if output.status.success() {
        let stdout = String::from_utf8(output.stdout)?;
        Ok(stdout)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        Err(format!(
            "command failed with status {}: {}",
            output.status,
            stderr.trim()
        )
        .into())
    }
}

fn print_command(command: &Command) {
    let mut rendered = String::from("$");
    rendered.push(' ');
    rendered.push_str(&render_os_str(command.get_program()));

    for arg in command.get_args() {
        rendered.push(' ');
        rendered.push_str(&render_os_str(arg));
    }

    println!("{rendered}");
}

fn render_os_str(value: &OsStr) -> String {
    let text = value.to_string_lossy();

    if text.chars().any(char::is_whitespace) {
        format!("{text:?}")
    } else {
        text.into_owned()
    }
}

#[derive(Debug)]
struct UsageError(&'static str);

impl fmt::Display for UsageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.0)?;
        print_usage(f)
    }
}

impl Error for UsageError {}

#[derive(Debug)]
struct CommandError {
    program: OsString,
    args: Vec<OsString>,
    status: ExitStatus,
}

impl CommandError {
    fn new(command: &Command, status: ExitStatus) -> Self {
        Self {
            program: command.get_program().to_owned(),
            args: command.get_args().map(OsStr::to_owned).collect(),
            status,
        }
    }
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "command failed with status {}: {}",
            self.status,
            render_command_line(&self.program, &self.args),
        )
    }
}

impl Error for CommandError {}

fn render_command_line(program: &OsStr, args: &[OsString]) -> String {
    let mut rendered = render_os_str(program);

    for arg in args {
        rendered.push(' ');
        rendered.push_str(&render_os_str(arg));
    }

    rendered
}

fn print_help() {
    println!("Workspace automation for Zakura.");
    println!();
    let mut help = String::new();
    print_usage(&mut help).expect("writing help to a string should succeed");
    print!("{help}");
}

fn print_usage(output: &mut impl fmt::Write) -> fmt::Result {
    writeln!(output, "Usage:")?;
    writeln!(output, "  cargo xtask package ubuntu")?;
    writeln!(output, "  cargo xtask header-conformance [LC-…]")?;
    writeln!(output, "  cargo xtask minimize-header-fuzz <artifact>")?;
    writeln!(output)?;
    writeln!(
        output,
        "Builds a Zakura release binary on {DEFAULT_UBUNTU_IMAGE} using Docker,"
    )?;
    writeln!(
        output,
        "enables features `{DEFAULT_FEATURES}`, and writes the binary to"
    )?;
    writeln!(output, "target/ubuntu/{OUTPUT_BINARY_NAME}.")?;
    writeln!(output)?;
    writeln!(
        output,
        "Validates the fork-aware header-chain specification and conformance manifest."
    )?;
    writeln!(output)?;
    writeln!(
        output,
        "Minimizes a header fuzz crash with pinned nightly cargo-fuzz, then prints"
    )?;
    writeln!(
        output,
        "its SHA-256, decoded bounded operations, and a deterministic Rust regression."
    )
}
