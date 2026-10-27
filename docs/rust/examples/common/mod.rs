#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use fdb::Fdb;

pub const GRIB: &[u8] = include_bytes!("../data/extract_ranges.grib");
pub const GRID_HASH: &str = "33c7d6025995e1b4913811e77d38ec50";
pub const FIELDS: &str = "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,domain=g,\
type=fc,levtype=sfc,param=151130";

const SCHEMA: &str = "\
param:    Param;
step:     Step;
date:     Date;
levelist: Double;
expver:   Expver;
time:     Time;

[ class, expver, stream, date, time, domain?
    [ type, levtype
        [ step, levelist?, param ]]
]
";

// [setup]
pub fn archive_fixture(dir: &Path) -> Result<Fdb, Box<dyn std::error::Error>> {
    fs::write(dir.join("schema"), SCHEMA)?;
    let yaml = format!(
        "type: local\nengine: toc\nschema: {root}/schema\nspaces:\n  - roots:\n      - path: {root}\n",
        root = dir.display()
    );
    unsafe { std::env::set_var("FDB5_CONFIG", &yaml) };

    let fdb = Fdb::open(Some(&yaml.parse()?), None)?;
    fdb.archive_raw(GRIB)?;
    fdb.flush()?;
    Ok(fdb)
}
// [end setup]

pub fn write_fixture(dir: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = dir.join("fields.grib");
    fs::write(&path, GRIB)?;
    Ok(path)
}
