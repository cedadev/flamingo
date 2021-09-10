"""
output_utils.py
===============

A selection of functions and objects to assist in writing
outputs.
"""

import os

from nappy.nc_interface.xarray_to_na import XarrayDatasetToNA


def write_to_csvs(results, output_dir):
    """
    Takes a `results` objects returned by the `clisops.subset()` function.
    It finds all the Xarray Datasets in the results and writes them to CSV 
    files using `nappy`. 
    The number of CSV files depends on the structure of the input
    Datasets. `nappy` will examine the structure and work out which files
    to write.

    Params:
    :results [object]: object returned from `clisops.subset()`
    :output_dir [str]: output directory to write CSV files to

    Returns:
    :output_file_paths [list]: a list of output file paths
    """
    output_file_paths = []
    i = 1

    for result_list in results._results.values():
        
        for ds in result_list:
            xr_to_na = XarrayDatasetToNA(ds)
            xr_to_na.convert()

            output_file = os.path.join(output_dir, f"output_{i:02d}.csv")
            xr_to_na.writeNAFiles(output_file, delimiter=",", float_format="%g")
            output_file_paths.extend(xr_to_na.output_files_written)

            i += 1

    return output_file_paths