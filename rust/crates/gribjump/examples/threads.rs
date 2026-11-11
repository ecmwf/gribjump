mod common;

use std::thread;

use gribjump::{ExtractionRequest, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let dir = tempfile::tempdir()?;
    let _fdb = common::archive_fixture(dir.path())?;
    let gribjump = GribJump::new()?;

    // [threads]
    let workers: Vec<_> = [1, 2, 3]
        .into_iter()
        .map(|step| {
            let gribjump = gribjump.clone();
            thread::spawn(move || -> gribjump::Result<usize> {
                let request = ExtractionRequest::new(
                    format!("{},step={step}", common::FIELDS),
                    vec![Range::new(0, 10)?],
                    common::GRID_HASH,
                );
                let mut values = 0;
                for field in gribjump.extract(&[request])? {
                    values += field?.total_values();
                }
                Ok(values)
            })
        })
        .collect();

    for worker in workers {
        assert_eq!(worker.join().expect("thread panicked")?, 10);
    }
    // [end threads]
    Ok(())
}
