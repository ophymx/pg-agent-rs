// Compile the .proto files in the workspace's proto/ directory into Rust
// modules under OUT_DIR. The generated code is re-included from lib.rs via
// `tonic::include_proto!("pgagentpb")`.
//
// Field-level validation (buf.validate) is NOT pulled in here on purpose —
// see SPEC §3.3. Validation is performed by hand in the Rust handlers so the
// proto files stay portable and we don't take a heavy dep for one regex and
// one min_len.

use std::error::Error;

const PROTO_ROOT: &str = "../../proto";

const PROTOS: &[&str] = &["common.proto", "pgagent_local.proto", "pgagent_peer.proto"];

fn main() -> Result<(), Box<dyn Error>> {
    for p in PROTOS {
        println!("cargo:rerun-if-changed={PROTO_ROOT}/{p}");
    }

    let proto_paths: Vec<String> = PROTOS.iter().map(|p| format!("{PROTO_ROOT}/{p}")).collect();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&proto_paths, &[PROTO_ROOT.to_string()])?;

    Ok(())
}
