from tests.common import _common_wps_process_test

from flamingo.processes.wps_subset_haduk_grid import SubsetHadUKGrid

import numpy as np


PROC_CLASS = SubsetHadUKGrid
VAR_MAP = PROC_CLASS.DSET_INFO["input_variables"]
DATASET_MAP = PROC_CLASS.DSET_INFO["input_datasets"]


def _common_wps_subset_haduk_grid_test(dataset_version, variable, time_range, lons, lats):
    assert dataset_version in DATASET_MAP
    assert variable in VAR_MAP

    var_id = VAR_MAP[variable]

    data_inputs = (f"dataset_version={dataset_version};"
                   f"variable={variable};"
                   f"frequency=Monthly;spatial_average=1km;"
                   f"timeDateRange={time_range};"
                   f"area={lons[0]},{lats[0]},{lons[1]},{lats[1]};output_type=netcdf")

    ds = _common_wps_process_test(PROC_CLASS, data_inputs)

    assert var_id in ds.variables

    if "km" in dataset_version:
        assert ds.transverse_mercator.grid_mapping_name == "transverse_mercator"
    
    assert (lons[0] - 1) < float(ds.longitude.min()) and (lons[1] + 1) > float(ds.longitude.max())
    assert (lats[0] - 1) < float(ds.latitude.min()) and (lats[1] + 1) > float(ds.latitude.max())


def test_wps_subset_haduk_grid_1km_v1_0_3_0(load_ceda_test_data):
    dataset_version = "HadUK-Grid dataset (v1.0.3.0)"
    variable = "Number of days with snow lying at 0900"
    time_range = "1971-01-01/1972-12-30"
    lons = [-1, 1]
    lats = [51, 52]

    _common_wps_subset_haduk_grid_test(dataset_version, variable, time_range, lons, lats)


def test_wps_subset_haduk_grid_1km_v1_0_2_1(load_ceda_test_data):
    dataset_version = "HadUK-Grid dataset (v1.0.2.1)"
    variable = "Number of days with snow lying at 0900"
    time_range = "1971-01-01/2020-12-30"
    lons = [-2, 2]
    lats = [50, 54]

    _common_wps_subset_haduk_grid_test(dataset_version, variable, time_range, lons, lats)


