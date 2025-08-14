GribJump Index Files (.gribjump)
================================

The GribJump Index files are binary files containing metadata used by GribJump for efficient data extraction from GRIB files. The contents can be inspected using the `gribjump-dump-info` tool::

  $: gribjump-dump-info mydata.gribjump
  IndexFile[mydata.gribjump (70 entries)]:
    Offset:0 -> CcsdsInfo,version=1,referenceValue=-4.90285e-18,binaryScaleFactor=-80,decimalScaleFactor=0,editionNumber=2,bitsPerValue=24,offsetBeforeData=24780,offsetAfterData=346458,offsetBeforeBitmap=199,numberOfValues=138777,numberOfDataPoints=196608,totalLength=346462,sphericalHarmonics=0,md5GridSection=f3dfeb7a5bbbdd13a20d10fdb3797c71,packingType=grid_ccsds,ccsdsFlags=14,ccsdsBlockSize=32,ccsdsRsi=128,ccsdsOffsets.size=34
    Offset:346462 -> CcsdsInfo,version=1,referenceValue=-0.000280982,binaryScaleFactor=-35,decimalScaleFactor=0,editionNumber=2,bitsPerValue=24,offsetBeforeData=24780,offsetAfterData=373191,offsetBeforeBitmap=199,numberOfValues=138777,numberOfDataPoints=196608,totalLength=373195,sphericalHarmonics=0,md5GridSection=f3dfeb7a5bbbdd13a20d10fdb3797c71,packingType=grid_ccsds,ccsdsFlags=14,ccsdsBlockSize=32,ccsdsRsi=128,ccsdsOffsets.size=34
    Offset:719657 -> CcsdsInfo,version=1,referenceValue=-0.000520969,binaryScaleFactor=-34,decimalScaleFactor=0,editionNumber=2,bitsPerValue=24,offsetBeforeData=24780,offsetAfterData=370912,offsetBeforeBitmap=199,numberOfValues=138777,numberOfDataPoints=196608,totalLength=370916,sphericalHarmonics=0,md5GridSection=f3dfeb7a5bbbdd13a20d10fdb3797c71,packingType=grid_ccsds,ccsdsFlags=14,ccsdsBlockSize=32,ccsdsRsi=128,ccsdsOffsets.size=34
    ...

There is one pair <Offset, GribJumpInfo> for each message in the GRIB file. Here is a breakdown of the dumped information:

- `Offset`: The byte offset in the GRIB file where the corresponding GRIB message begins.
- `CcsdsInfo`: Indicates that the field is packed using CCSDS compression. `SimpleInfo` would indicate simple packing.
- `version`: The serialisation version of the .gribjump index. Currently only version 1 exists.
- `editionNumber`: The edition of the GRIB format (1 or 2).
- `referenceValue`, `binaryScaleFactor`, `decimalScaleFactor`, `bitsPerValue`, are all parameters used to decode the GRIB field values.
- `offsetBeforeData`, `offsetAfterData`, `offsetBeforeBitmap`: byte offsets within the GRIB message (relative to the start of the message) indicating the data and bitmap locations.
- `numberOfValues`: The total number of values encoded in the GRIB message.
- `numberOfDataPoints`: The total number of data points in the GRIB message. This will equal `numberOfValues` if there is no bitmap.
- `totalLength`: The total length of the GRIB message in bytes.
- `sphericalHarmonics`: Indicates if the field is represented using spherical harmonics.
- `md5GridSection`: The MD5 checksum of the grid section of the GRIB message, calculated by eccodes. Can be optionally used to verify that the grid matches expectations.
- `packingType`: The packing type used for the GRIB message (e.g., `grid_ccsds`, `grid_simple`).
- `ccsdsFlags`, `ccsdsBlockSize`, `ccsdsRsi`, `ccsdsOffsets.size`: Various parameters used for CCSDS packing.