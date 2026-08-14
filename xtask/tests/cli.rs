use std::process::Command;

#[test]
fn invalid_command_exits_unsuccessfully() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("unknown")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap(),
        "xtask: usage: cargo xtask <check|check-generated|generate|crashlab ...>\n"
    );
}
