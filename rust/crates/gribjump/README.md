# gribjump

Safe Rust wrapper for ECMWF's [GribJump](https://github.com/ecmwf/gribjump) library.

GribJump enables efficient random access extraction of data from GRIB files stored in FDB, without needing to decode entire messages.

## Usage

```rust
use gribjump::{GribJump, ExtractionRequest};

// Create GribJump instance
let gj = GribJump::new()?;

// Define extraction ranges (start, end pairs)
let ranges = vec![(0, 100), (500, 600)];

// Extract specific value ranges from GRIB data
let request = ExtractionRequest::new("class=od,stream=oper,type=fc", ranges);
let results = gj.extract(&[request])?;

for result in results {
    for range in result.iter() {
        println!("Values: {:?}", range.values());
    }
}
```

## Features

- `vendored` (default) - Build GribJump and dependencies from source
- `system` - Link against system-installed GribJump

## License

Apache-2.0
