use super::docker::{RunContext, SHARE};

/// The failure diagnostics: transcript tail, each container's tree listing,
/// raw `status --json` (printed unparsed, so a schema mismatch is visible in
/// the dump rather than able to break it), captured flocal stderr and watch
/// stdout, and sshd logs. A failing scenario must be diagnosable from this
/// output alone.
pub fn print_dump(context: &RunContext) {
    eprintln!("==== e2e failure dump (run {}) ====", context.run_id);
    eprintln!("---- permanent diagnostics ----");
    for line in context.diagnostics() {
        eprintln!("{line}");
    }
    eprintln!("---- transcript tail ----");
    for line in context.transcript_tail(50) {
        eprintln!("{line}");
    }
    let containers = context.containers.lock().expect("containers lock").clone();
    for name in &containers {
        eprintln!("---- {name}: tree ----");
        print_exec(
            context,
            name,
            &[
                "find",
                SHARE,
                "-mindepth",
                "1",
                "-printf",
                "%y %m %p -> %l\\n",
            ],
        );
        eprintln!("---- {name}: raw status --json ----");
        print_exec(context, name, &["flocal", "status", SHARE, "--json"]);
        eprintln!("---- {name}: captured flocal stderr ----");
        print_exec(
            context,
            name,
            &["cat", "--", "/home/peer/.flocal-stderr.log"],
        );
        eprintln!("---- {name}: captured watch stdout ----");
        print_exec(
            context,
            name,
            &["cat", "--", "/home/peer/.flocal-watch.log"],
        );
        eprintln!("---- {name}: docker logs ----");
        match context.docker_raw(&["logs", "--tail", "50", name]) {
            Ok(output) => {
                eprint!("{}", String::from_utf8_lossy(&output.stdout));
                eprint!("{}", String::from_utf8_lossy(&output.stderr));
            }
            Err(error) => eprintln!("(docker logs failed: {error:#})"),
        }
    }
    if context.keep() {
        eprintln!("FLOCAL_E2E_KEEP=1: containers kept for inspection: {containers:?}");
    }
    eprintln!("==== end of dump ====");
}

fn print_exec(context: &RunContext, container: &str, command: &[&str]) {
    let mut args = vec!["exec", "-u", "peer", container];
    args.extend_from_slice(command);
    match context.docker_raw(&args) {
        Ok(output) if output.status.success() => {
            eprint!("{}", String::from_utf8_lossy(&output.stdout));
        }
        Ok(output) => eprintln!(
            "(command failed: {})",
            String::from_utf8_lossy(&output.stderr).trim_end()
        ),
        Err(error) => eprintln!("(command unavailable: {error:#})"),
    }
}
