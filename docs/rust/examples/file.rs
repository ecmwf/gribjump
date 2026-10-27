mod common;

use gribjump::{FileExtraction, GribJump, PathExtractionRequest, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let dir = tempfile::tempdir()?;
    let path = common::write_fixture(dir.path())?;
    let gribjump = GribJump::new()?;

    // [offsets]
    let (mut reading, _) = eckit::DataHandle::from_path(&path)?.open_for_read()?;
    let mut offsets = Vec::new();
    for message in eckit::MessageReader::new(&mut reading)? {
        offsets.push(u64::try_from(message?.offset())?);
    }
    assert_eq!(offsets.len(), 3);
    // [end offsets]

    // [file]
    let ranges = vec![Range::new(0, 5)?, Range::new(20, 30)?];
    let extraction = FileExtraction::new(path.to_string_lossy())
        .with_messages(offsets.iter().map(|&offset| (offset, ranges.clone())));

    for field in gribjump.extract_from_file(&extraction)? {
        let field = field?;
        println!(
            "{} ranges, {} values",
            field.num_ranges(),
            field.total_values()
        );
    }

    let request =
        PathExtractionRequest::new(path.to_string_lossy(), ranges.clone(), common::GRID_HASH)
            .with_offset(usize::try_from(offsets[1])?);
    let fields: Vec<_> = gribjump
        .extract_from_paths(&[request])?
        .collect::<gribjump::Result<_>>()?;
    assert_eq!(fields.len(), 1);
    // [end paths]
    Ok(())
}
