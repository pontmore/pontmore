use std::path::Path;

#[test]
fn repository_vectors_pass() {
    let vectors = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tck/vectors");
    let summary = pontmore_tck::run_path(&vectors).expect("vectors should load");
    assert!(summary.passed(), "{:#?}", summary.failures);
    assert!(summary.cases >= 10, "the starter TCK should not be trivial");
}
