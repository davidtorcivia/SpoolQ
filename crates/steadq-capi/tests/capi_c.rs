// Compiles and runs the C integration test against the built shared
// library, mirroring the CI invocation. Skips when the cdylib has not
// been built (`cargo build -p steadq-capi`); CI builds it before
// `cargo test --all`, so the C test gates there.

use std::path::PathBuf;
use std::process::Command;

#[test]
fn capi_c_integration() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target = match std::env::var("CARGO_TARGET_DIR") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => manifest.join("../../target"),
    };

    let lib_dir = ["debug", "release"]
        .into_iter()
        .map(|profile| target.join(profile))
        .find(|dir| dir.join("libsteadq_capi.so").exists());
    let Some(lib_dir) = lib_dir else {
        eprintln!("skipping: libsteadq_capi.so not built; run `cargo build -p steadq-capi`");
        return;
    };

    let bin = target.join("capi_test");
    let compile = Command::new("cc")
        .arg(manifest.join("tests/test_capi.c"))
        .arg(format!("-I{}", manifest.join("include").display()))
        .arg(format!("-L{}", lib_dir.display()))
        .arg("-lsteadq_capi")
        .arg("-o")
        .arg(&bin)
        .arg(format!("-Wl,-rpath,{}", lib_dir.display()))
        .status()
        .unwrap();
    assert!(compile.success(), "compiling test_capi.c failed");

    let run = Command::new(&bin).status().unwrap();
    assert!(run.success(), "C ABI integration test failed");
}
