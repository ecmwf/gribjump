mod common;

use gribjump::{ExtractionRequest, GribJump, Range, RangeResult};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let dir = tempfile::tempdir()?;
    let _fdb = common::archive_fixture(dir.path())?;
    let gribjump = GribJump::new()?;

    // [extract]
    let requests = vec![
        ExtractionRequest::new(
            format!("{},step=1", common::FIELDS),
            vec![Range::new(0, 5)?, Range::new(20, 30)?],
            common::GRID_HASH,
        ),
        ExtractionRequest::new(
            format!("{},step=3", common::FIELDS),
            vec![Range::from(0..100)],
            common::GRID_HASH,
        ),
    ];

    for field in gribjump.extract(&requests)? {
        let field = field?;
        for (i, range) in field.iter().enumerate() {
            let valid = range.valid_values().count();
            println!("range {i}: {} values, {valid} valid", range.len());
            for (value, is_valid) in range.iter_with_validity().take(3) {
                println!("  {value} {is_valid}");
            }
        }
    }
    // [end extract]

    // [mars]
    let ranges = [Range::new(0, 5)?, Range::new(20, 30)?];
    let fields: Vec<gribjump::ExtractionResult> = gribjump
        .extract_mars(
            &format!("retrieve,{},step=1/2/3", common::FIELDS),
            &ranges,
            common::GRID_HASH,
        )?
        .collect::<gribjump::Result<_>>()?;
    assert_eq!(fields.len(), 3);
    // [end mars]

    // [owned]
    let kept: Vec<RangeResult> = fields[0].to_owned_ranges();
    drop(fields);
    assert_eq!(kept[0].values.len(), 5);
    assert!(kept[0].is_valid(0));
    // [end owned]
    Ok(())
}
