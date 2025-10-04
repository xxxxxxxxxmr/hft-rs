use std::{env, error::Error, process::Command};

fn main() {
    if let Err(err) = real_main() {
        eprintln!("xtask error: {err}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let task = match args.next() {
        Some(task) => task,
        None => {
            print_help();
            return Ok(());
        }
    };

    match task.as_str() {
        "fmt" => run_cargo(&["fmt", "--all"]),
        "fmt-check" => run_cargo(&["fmt", "--all", "--", "--check"]),
        "clippy" => run_cargo(&[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ]),
        "check" => run_cargo(&["check", "--workspace"]),
        "lint" => {
            run_cargo(&["fmt", "--all", "--", "--check"])?;
            run_cargo(&[
                "clippy",
                "--workspace",
                "--all-targets",
                "--",
                "-D",
                "warnings",
            ])
        }
        "help" | "-h" | "--help" => {
            print_help();
            Ok(())
        }
        other => {
            eprintln!("unknown task: {other}\n");
            print_help();
            Err("unknown task".into())
        }
    }
}

fn run_cargo(args: &[&str]) -> Result<(), Box<dyn Error>> {
    let status = Command::new("cargo").args(args).status()?;
    if status.success() {
        Ok(())
    } else {
        let code = status.code().unwrap_or(1);
        Err(format!("cargo {args:?} exited with status {code}").into())
    }
}

fn print_help() {
    eprintln!(
        "xtask commands:\n  xtask fmt          # cargo fmt --all\n  xtask fmt-check    # cargo fmt --all -- --check\n  xtask clippy       # cargo clippy --workspace --all-targets -D warnings\n  xtask check        # cargo check --workspace\n  xtask lint         # fmt-check + clippy\n  xtask help         # show this message"
    );
}
