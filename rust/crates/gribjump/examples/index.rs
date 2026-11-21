mod common;

use gribjump::GribJump;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let dir = tempfile::tempdir()?;
    let _fdb = common::archive_fixture(dir.path())?;
    let path = common::write_fixture(dir.path())?;
    let gribjump = GribJump::new()?;

    // [scan]
    let scanned = gribjump.scan_requests(&["retrieve,class=rd,expver=xxxx"], false)?;
    assert_eq!(scanned, 3);

    let scanned = gribjump.scan_paths(&[path.to_string_lossy()])?;
    assert_eq!(scanned, 3);
    assert!(path.with_extension("grib.gribjump").exists());

    let axes = gribjump.axes("class=rd,expver=xxxx", 3)?;
    let mut steps = axes["step"].clone();
    steps.sort();
    assert_eq!(steps, ["1", "2", "3"]);
    // [end axes]
    Ok(())
}
