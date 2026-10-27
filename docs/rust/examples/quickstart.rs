use gribjump::{ExtractionRequest, GribJump, Range};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    eckit::init();
    let gribjump = GribJump::new()?;

    let request = ExtractionRequest::new(
        "class=rd,expver=xxxx,stream=oper,date=20230508,time=1200,domain=g,\
         type=fc,levtype=sfc,param=151130,step=2",
        vec![Range::new(0, 5)?, Range::new(20, 30)?],
        "33c7d6025995e1b4913811e77d38ec50",
    );

    for field in gribjump.extract(&[request])? {
        let field = field?;
        for range in field.iter() {
            println!("{} values: {:?}", range.len(), range.values());
        }
    }
    Ok(())
}
