#!/usr/bin/env python3
"""Reduce the two original extremes.grib fields using grib_filter only.

Usage: python3 prepare.py /path/to/extremes.grib --grib-filter /path/to/grib_filter
Python generates filter rules; all GRIB reading, value selection and packing
are performed by grib_filter. The original large input is not needed by CTest.
"""
import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('--grib-filter', default='grib_filter')
    parser.add_argument('--output', type=Path, default=Path(__file__).with_name('extremes-100.grib2'))
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        # Capture the source values before changing grid dimensions or scaling.
        # %.17g round-trips doubles; the second filter receives literal values.
        select = directory / 'select.filter'
        rules = ['assert(count <= 2);', 'assert(edition == 2);', 'assert(packingType is "grid_ccsds");']
        rules.extend(f'meta point{i} element(values, {i});' for i in range(100))
        values = ', '.join(f'[point{i}:d%.17g]' for i in range(100))
        # Packing a temporary constant resets the read-only binary scale to zero.
        # This makes CCSDS packing honour the original decimal scale on repack.
        rules.append('print "if (count == [count]) { set Nx = 10; set Ny = 10; '
                     'set numberOfDataPoints = 100; set bitmapPresent = 0; '
                     'set values = {0}; set bitsPerValue = [bitsPerValue]; '
                     'set decimalScaleFactor = [decimalScaleFactor];";')
        rules.append('print "set values = {' + values + '};";')
        rules.append('print "assert(decimalScaleFactor == [decimalScaleFactor]); '
                     'assert(numberOfValues == 100); write; }";')
        select.write_text('\n'.join(rules) + '\n')
        generated = subprocess.check_output([args.grib_filter, str(select), str(args.input)], text=True)
        if generated.count('if (count == ') != 2:
            raise RuntimeError('Expected exactly two source fields')
        repack = directory / 'repack.filter'
        repack.write_text(generated)
        output = directory / 'extremes-100.grib2'
        subprocess.run([args.grib_filter, '-o', str(output), str(repack), str(args.input)], check=True)
        args.output.write_bytes(output.read_bytes())
    print(f'Wrote {args.output}: {args.output.stat().st_size} bytes, two fields of 100 values')


if __name__ == '__main__':
    main()
