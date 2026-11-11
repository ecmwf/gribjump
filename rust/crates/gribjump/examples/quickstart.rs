use gribjump::{FileExtraction, GribJump, Range};

// [main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let gribjump = GribJump::new()?;

    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extract_ranges.grib"
    );
    let extraction =
        FileExtraction::new(path).with_message(0, vec![Range::new(0, 5)?, Range::new(20, 30)?]);

    for field in gribjump.extract_from_file(&extraction)? {
        let field = field?;
        for range in field.iter() {
            println!("{} values: {:?}", range.len(), range.values());
        }
    }
    Ok(())
}
// [end main]
