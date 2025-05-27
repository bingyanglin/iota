use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root_dir = PathBuf::from("src/proto");
    let proto_package_dir = proto_root_dir.join("iota/gprc/v1");

    // Temporarily reduce to known good/edited files for debugging
    let proto_filenames = [
        "common.proto",
        "checkpoints.proto",
        "objects.proto",
        "transactions.proto",
        // "info.proto",
        // "committee.proto",
        // "system.proto",
        // "coins.proto",
        // "epochs.proto",
        // "accounts.proto",
    ];

    let mut protos_to_compile_fullpath_str: Vec<String> = Vec::new();
    let mut proto_paths_for_rerun: Vec<PathBuf> = Vec::new();

    for filename in &proto_filenames {
        let full_path = proto_package_dir.join(filename);
        if full_path.exists() {
            proto_paths_for_rerun.push(full_path.clone());
            match full_path.to_str() {
                Some(s) => protos_to_compile_fullpath_str.push(s.to_string()),
                None => {
                    return Err(
                        format!("Invalid path for proto file: {}", full_path.display()).into(),
                    );
                }
            }
        } else {
            eprintln!(
                "cargo:warning=Proto file {} not found in {}, skipping.",
                filename,
                proto_package_dir.display()
            );
        }
    }

    for pf_path_buf in &proto_paths_for_rerun {
        println!("cargo:rerun-if-changed={}", pf_path_buf.display());
    }

    if protos_to_compile_fullpath_str.is_empty() {
        eprintln!(
            "cargo:warning=No proto files found or specified for compilation in {}.",
            proto_package_dir.display()
        );
    } else {
        tonic_build::configure().compile(
            &protos_to_compile_fullpath_str
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<&str>>(),
            &[proto_root_dir
                .to_str()
                .expect("proto_root_dir is not valid UTF-8")],
        )?;
    }

    Ok(())
}
