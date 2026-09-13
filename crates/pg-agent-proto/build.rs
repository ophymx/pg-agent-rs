// Compile the .proto files in the workspace's proto/ directory into Rust
// modules under OUT_DIR. The generated code is re-included from lib.rs via
// `tonic::include_proto!("pgagentpb")`.
//
// Field-level validation (buf.validate) is NOT pulled in here on purpose —
// see SPEC §3.4. Validation is performed by hand in the Rust handlers so the
// proto files stay portable and we don't take a heavy dep for one regex and
// one min_len.

use std::error::Error;

const PROTO_ROOT: &str = "../../proto";

const PROTOS: &[&str] = &[
    "common.proto",
    "pgagent_local.proto",
    "pgagent_peer.proto",
    "pgagent_raft.proto",
];

fn main() -> Result<(), Box<dyn Error>> {
    // No system protoc required — use the vendored binary. Set rather than
    // passed to `tonic_build` because prost-build sources protoc from the
    // environment, and this is the variable it reads.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);

    for p in PROTOS {
        println!("cargo:rerun-if-changed={PROTO_ROOT}/{p}");
    }

    let proto_paths: Vec<String> = PROTOS.iter().map(|p| format!("{PROTO_ROOT}/{p}")).collect();

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // `WalChunk.data` carries the FetchWal payload — 16 MiB per WAL
        // segment in 1 MiB chunks. As a `Vec<u8>` prost copies every chunk
        // out of tonic's decode buffer on the receiving side; as a
        // `bytes::Bytes` the decode is a refcount bump on that same buffer,
        // and the sender can hand a `BytesMut` slice straight in. Scoped to
        // this one field on purpose: the `payload` fields on the raft and
        // local services are small and their `Vec<u8>` callers are not worth
        // churning.
        .bytes(["pgagentpb.WalChunk.data"])
        .compile_protos(&proto_paths, &[PROTO_ROOT.to_string()])?;

    Ok(())
}
