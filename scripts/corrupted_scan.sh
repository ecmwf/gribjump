#!/bin/bash

# For scanning corrupted GRIB files and generating gribjump index
# We assume that the tail of a GRIB file is corrupted in the following way:
# - It contains a message that starts with "GRIB" but does not end with "7777"
# - This message is at the end of the file
# - All other messages in the file are valid.
# This script will give spurious results if the file does not follow this pattern.

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <gribfile>"
    exit 1
fi

gribfile=$1
outfile="$gribfile.gribjump"

# Assert that it does not already exist
if [ -f "$outfile" ]; then
    echo "Output file $outfile already exists. Please remove it before running this script."
    exit 1
fi

eccodes_count_messages() {
    local output
    output=$(grib_count $gribfile 2>&1)

    if [[ "$output" =~ ^[0-9]+$ ]]; then
        echo "$output"
        return 0
    fi

    local count
    count=$(echo "$output" | grep -oE 'got as far as [0-9]+' | grep -oE '[0-9]+')

    if [[ -n "$count" ]]; then
        echo "$count"
        return 0
    fi

    echo "Error: Unable to extract count from grib_count output" >&2
    return 1
}

# Get count from eccodes
eccodes_count=$(eccodes_count_messages)
if [ $? -ne 0 ]; then
    echo "Failed to get count from grib_count"
    exit 1
fi

# if eccodes_count is 0, we can exit early
if [ "$eccodes_count" -eq 0 ]; then
    echo "No GRIB messages found in $gribfile"
    exit 0
fi

GRIBJUMP_SCAN_CORRUPTED=1 gribjump-scan-files $gribfile

# sanity check results
if [ ! -f "$outfile" ]; then
    echo "Failed to create gribjump index file: $outfile"
    exit 1
fi

count_gribjump=$(grep -c "Info" "$outfile")

# check gribjump and eccodes agree
if [ "$count_gribjump" -ne "$eccodes_count" ]; then
    echo "Mismatch in counts: gribjump found $count_gribjump messages, eccodes found $eccodes_count messages"
    exit 1
fi

echo "Successfully created gribjump index file: $outfile with $count_gribjump messages"
exit 0