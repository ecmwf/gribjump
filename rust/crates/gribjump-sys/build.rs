//! Build script for gribjump-sys
//!
//! Supports two build modes:
//! - `vendored` (default): Clone and build gribjump from source using ecbuild
//! - `system`: Use `CMake` `find_package` to find system-installed gribjump
//!
//! Both modes build the CXX bridge for C++ to Rust bindings.

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cpp/gribjump_bridge.h");
    println!("cargo:rerun-if-changed=cpp/gribjump_bridge.cpp");
    println!("cargo:rerun-if-env-changed=GRIBJUMP_DIR");
    println!("cargo:rerun-if-env-changed=CMAKE_PREFIX_PATH");
    println!("cargo:rerun-if-env-changed=DOCS_RS");

    if bindman_utils::is_docs_rs() {
        return;
    }

    bindman_utils::validate_build_mode(cfg!(feature = "system"), cfg!(feature = "vendored"));

    if cfg!(feature = "system") {
        build_system();
    } else {
        build_vendored();
    }
}

/// Build using system-installed gribjump via `CMake` `find_package`
#[cfg(feature = "system")]
fn build_system() {
    let crate_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    // Get dependency paths from -sys crates
    let eckit_include = env::var("DEP_ECKIT_INCLUDE")
        .expect("DEP_ECKIT_INCLUDE not set - eckit-sys must be a dependency");
    let eckit_root = env::var("DEP_ECKIT_ROOT")
        .expect("DEP_ECKIT_ROOT not set - eckit-sys must be a dependency");
    let metkit_include = env::var("DEP_METKIT_INCLUDE")
        .expect("DEP_METKIT_INCLUDE not set - metkit-sys must be a dependency");
    let eccodes_include = env::var("DEP_ECCODES_INCLUDE")
        .expect("DEP_ECCODES_INCLUDE not set - eccodes-sys must be a dependency");
    let fdb_include = env::var("DEP_FDB_SYS_INCLUDE")
        .expect("DEP_FDB_SYS_INCLUDE not set - fdb-sys must be a dependency");

    let (root, gribjump_include, lib_dir) =
        bindman_utils::cmake_find_package("gribjump", "0.10.3", Some("GRIBJUMP_DIR"));

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=gribjump");

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/gribjump_bridge.cpp"))
        .include(&gribjump_include)
        .include(&eckit_include)
        .include(&metkit_include)
        .include(&eccodes_include)
        .include(&fdb_include)
        .include(crate_dir.join("cpp"))
        .flag_if_supported("-std=c++17")
        .compile("gribjump_sys_bridge");

    // Link to eckit (bridge uses its symbols)
    println!("cargo:rustc-link-search=native={eckit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=eckit");
    bindman_utils::link_cpp_stdlib();

    // Re-publish each dependency's install lib dir so the downstream
    // `gribjump` crate's build script can emit matching absolute rpath
    // entries on the final binary. `rustc-link-arg` emitted by a
    // library crate's build.rs does not reach binaries that link the
    // crate, so the rpath flags have to come from `gribjump/build.rs`.
    // The fdb5 entry is forwarded from `fdb-sys` (which does the same
    // re-publishing trick) so gribjump's binaries pick up every rpath
    // they need, not just the ones gribjump-sys knows about directly.
    let metkit_root = env::var("DEP_METKIT_ROOT")
        .expect("DEP_METKIT_ROOT not set - metkit-sys must be a dependency");
    let eccodes_root = env::var("DEP_ECCODES_ROOT")
        .expect("DEP_ECCODES_ROOT not set - eccodes-sys must be a dependency");
    let fdb5_lib = env::var("DEP_FDB_SYS_SYSTEM_FDB5_LIB").expect(
        "DEP_FDB_SYS_SYSTEM_FDB5_LIB not set - fdb-sys must be built with --features system",
    );
    println!("cargo:system_gribjump_lib={}", lib_dir.display());
    println!("cargo:system_eckit_lib={eckit_root}/lib");
    println!("cargo:system_metkit_lib={metkit_root}/lib");
    println!("cargo:system_eccodes_lib={eccodes_root}/lib");
    println!("cargo:system_fdb5_lib={fdb5_lib}");

    // Export for downstream crates
    println!("cargo:root={}", root.display());
    println!("cargo:include={}", gribjump_include.display());

    // Check C++ API
    bindman_build::check_cpp_api(&gribjump_include, &crate_dir.join("src/lib.rs"));
}

#[cfg(not(feature = "system"))]
fn build_system() {
    unreachable!("build_system called without system feature");
}

/// Build gribjump from source using ecbuild
#[cfg(feature = "vendored")]
fn build_vendored() {
    use std::fs;
    use std::process::Command;

    const ECBUILD_REPO: &str = "https://github.com/ecmwf/ecbuild.git";
    const ECBUILD_TAG: &str = "3.13.1";

    const GRIBJUMP_REPO: &str = "https://github.com/ecmwf/gribjump.git";
    const GRIBJUMP_TAG: &str = "0.10.3";

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    let src_dir = out_dir.join("src");
    let build_dir = out_dir.join("build");
    let install_dir = out_dir.join("install");

    fs::create_dir_all(&src_dir).expect("Failed to create src directory");
    fs::create_dir_all(&build_dir).expect("Failed to create build directory");

    // Get dependency paths from -sys crates
    let eckit_root = env::var("DEP_ECKIT_ROOT")
        .expect("DEP_ECKIT_ROOT not set - eckit-sys must be a dependency");
    let metkit_root = env::var("DEP_METKIT_ROOT")
        .expect("DEP_METKIT_ROOT not set - metkit-sys must be a dependency");
    let eccodes_root = env::var("DEP_ECCODES_ROOT")
        .expect("DEP_ECCODES_ROOT not set - eccodes-sys must be a dependency");
    let aec_root = env::var("DEP_ECCODES_AEC_ROOT")
        .expect("DEP_ECCODES_AEC_ROOT not set - eccodes-sys must be a dependency");
    let fdb_root = env::var("DEP_FDB_SYS_ROOT")
        .expect("DEP_FDB_SYS_ROOT not set - fdb-sys must be a dependency");

    // Clone sources
    let ecbuild_src = bindman_utils::git_clone(ECBUILD_REPO, ECBUILD_TAG, &src_dir.join("ecbuild"));
    let gribjump_src =
        bindman_utils::git_clone(GRIBJUMP_REPO, GRIBJUMP_TAG, &src_dir.join("gribjump"));

    let ecbuild_bin = ecbuild_src.join("bin/ecbuild");
    let num_jobs = bindman_utils::build_parallelism();

    let cmake_prefix_path =
        format!("{eckit_root};{metkit_root};{eccodes_root};{aec_root};{fdb_root}");

    // Build gribjump
    let mut cmd = Command::new(&ecbuild_bin);
    cmd.current_dir(&build_dir)
        .arg(format!("--prefix={}", install_dir.display()))
        .arg("--")
        .arg(&gribjump_src)
        .arg(format!("-DCMAKE_PREFIX_PATH={cmake_prefix_path}"))
        .arg("-DCMAKE_BUILD_TYPE=Release")
        // Always disabled (no features)
        .arg("-DENABLE_TESTS=OFF")
        .arg("-DBUILD_TESTING=OFF")
        .arg("-DENABLE_DOCS=OFF")
        .arg("-DENABLE_PYTHON_API_TESTS=OFF");

    cmd.arg(format!(
        "-DENABLE_GRIBJUMP_LOCAL_EXTRACT={}",
        bindman_utils::on_off(cfg!(feature = "local-extract"))
    ));

    bindman_utils::run_command(&mut cmd, "ecbuild configure gribjump");

    bindman_utils::run_command(
        Command::new("cmake")
            .args(["--build", ".", "--parallel", &num_jobs])
            .current_dir(&build_dir),
        "cmake build gribjump",
    );

    bindman_utils::run_command(
        Command::new("cmake")
            .args(["--install", "."])
            .current_dir(&build_dir),
        "cmake install gribjump",
    );

    // Determine library directory
    let lib_dir = bindman_utils::resolve_lib_dir(&install_dir);

    let include_dir = install_dir.join("include");
    let crate_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set"));

    // gribjump source directory contains private headers like Types.h
    let gribjump_src_include = gribjump_src.join("src");

    // Build the CXX bridge
    cxx_build::bridge("src/lib.rs")
        .file(crate_dir.join("cpp/gribjump_bridge.cpp"))
        .include(&include_dir)
        .include(&gribjump_src_include)
        .include(format!("{eckit_root}/include"))
        .include(format!("{metkit_root}/include"))
        .include(format!("{eccodes_root}/include"))
        .include(format!("{fdb_root}/include"))
        .include(crate_dir.join("cpp"))
        .flag_if_supported("-std=c++17")
        .compile("gribjump_sys_bridge");

    // Link directives
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=gribjump");

    // The bridge code directly uses eckit and metkit symbols
    println!("cargo:rustc-link-search=native={eckit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=eckit");
    println!("cargo:rustc-link-search=native={metkit_root}/lib");
    println!("cargo:rustc-link-lib=dylib=metkit");
    bindman_utils::link_cpp_stdlib();

    // Export for downstream crates
    println!("cargo:root={}", install_dir.display());
    println!("cargo:include={}", include_dir.display());

    // Check C++ API
    bindman_build::check_cpp_api(&gribjump_src_include, &crate_dir.join("src/lib.rs"));
}

#[cfg(not(feature = "vendored"))]
fn build_vendored() {
    unreachable!("build_vendored called without vendored feature");
}
