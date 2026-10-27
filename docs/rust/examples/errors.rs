mod common;

use gribjump::{Error, ExtractionRequest, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let dir = tempfile::tempdir()?;
    let _fdb = common::archive_fixture(dir.path())?;
    let gribjump = GribJump::new()?;

    // [errors]
    assert!(matches!(Range::new(5, 5), Err(Error::InvalidArgument(_))));

    let request = ExtractionRequest::new(
        format!("{},step=1", common::FIELDS),
        vec![Range::new(0, 5)?],
        "0000000000000000000000000000dead",
    );
    match gribjump.extract(&[request]) {
        Ok(_) => println!("extracted"),
        Err(Error::Eckit(e)) => println!("{e}"),
        Err(other) => println!("{other}"),
    }
    // [end errors]
    Ok(())
}
