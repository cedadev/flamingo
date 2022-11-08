from tests.common import MINI_CEDA_CACHE_DIR, MINI_CEDA_CACHE_BRANCH, _common_wps_process_test

import numpy as np
import pytest
import xarray as xr

from flamingo.processes.wps_subset_cru_ts import SubsetCRUTS

PROC_CLASS = SubsetCRUTS
allowed_vars = PROC_CLASS.DSET_INFO["input_variables"]

TEST_SETS = [
    (
        "wet day frequency (days)",
        "1951-01-01",
        "2005-12-15",
        ["-85", "1", "180", "89"],
        "netcdf",
    ),
    (
        "near-surface temperature minimum (degrees Celsius)",
        "1980-02-02",
        "2011-05-20",
        ["1", "20", "50", "80"],
        "netcdf",
    ),
]


def test_wps_subset_cru_ts_4_06(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.06;"
                  "variable=wet day frequency (days);timeDateRange=2001-01-01/2021-06-30;"
                  "area=1,1,300,89;output_type=netcdf")
    ds = _common_wps_process_test(PROC_CLASS, data_inputs)
    assert np.isclose(float(ds.wet.max()), 25.52, atol=.005)


def test_wps_subset_cru_ts_4_05(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.05;"
                  "variable=wet day frequency (days);timeDateRange=2001-01-01/2020-12-30;"
                  "area=1,1,300,89;output_type=netcdf")
    ds = _common_wps_process_test(PROC_CLASS, data_inputs)
    assert np.isclose(float(ds.wet.max()), 23.48, atol=.005)


def test_wps_subset_cru_ts_4_04(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                  "variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;"
                  "area=1,1,300,89;output_type=netcdf")
    _ = _common_wps_process_test(PROC_CLASS, data_inputs)


def test_wps_subset_cru_ts_4_04_csv_tasmin(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                  "variable=near-surface temperature minimum (degrees Celsius);"
                  "timeDateRange=1951-01-01/2005-12-15;area=1,1,300,89;output_type=csv")
    _ = _common_wps_process_test(PROC_CLASS, data_inputs)


def test_wps_subset_cru_ts_4_04_csv_wet(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                   "variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;"
                   "area=1,1,300,89;output_type=csv")
    _ = _common_wps_process_test(PROC_CLASS, data_inputs)


def test_wps_subset_cru_ts_4_04_csv_check_global_attrs(load_ceda_test_data):
    data_inputs = ("dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                   "variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;"
                   "area=1,1,300,89;output_type=csv")
    lines = _common_wps_process_test(PROC_CLASS, data_inputs)
    content = "\n".join(lines)

    assert "Conventions:   CF-1.4" in lines
    assert "CRU TS4.04 Rain Days" in lines

    assert "History:  20" in content
    assert "Converted to NASA Ames format using nappy" in content
    assert "ncks -d lat,,,100 -d lon,,,100 --variable wet /badc/cru/data/cru_ts/cru_ts_4.04/data/wet/cru_ts4.04.1901.2019.wet.dat.nc" in content
    assert "BST : User ianharris : Program makegridsauto.for called by update.for" in content


@pytest.mark.parametrize("variable,start_date,end_date,area,output_type", TEST_SETS)
def test_wps_subset_cru_ts_4_04_check_nc_content(
    load_ceda_test_data, variable, start_date, end_date, area, output_type
):
    data_inputs = (f"dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                   f"variable={variable};timeDateRange={start_date}/{end_date};area={','.join(area)};"
                   f"output_type={output_type}")
    ds = _common_wps_process_test(PROC_CLASS, data_inputs)

    data_vars = ds.data_vars
    expected_vars = set(["time", allowed_vars[variable], "lat", "lon"])
    found_vars = set(ds.variables.keys())

    assert len(data_vars) == 1
    assert expected_vars == found_vars

    first_date = ds.variables["time"].values[0].strftime("%Y-%m-%d")
    last_date = ds.variables["time"].values[-1].strftime("%Y-%m-%d")

    assert first_date >= start_date
    assert last_date <= end_date

    min_lon = ds.variables["lon"].values[0]
    min_lat = ds.variables["lat"].values[0]
    max_lon = ds.variables["lon"].values[-1]
    max_lat = ds.variables["lat"].values[-1]

    assert min_lon >= float(area[0])
    assert min_lat >= float(area[1])
    assert max_lon <= float(area[2])
    assert max_lat <= float(area[3])


@pytest.mark.parametrize("variable,start_date,end_date,area,output_type", TEST_SETS)
def test_wps_subset_cru_ts_4_04_check_min_max(
    load_ceda_test_data, variable, start_date, end_date, area, output_type
):
    data_path = f"{MINI_CEDA_CACHE_DIR}/{MINI_CEDA_CACHE_BRANCH}/archive/badc/cru/data/cru_ts/cru_ts_4.04/data/{allowed_vars[variable]}/*.nc"
    ds = xr.open_mfdataset(data_path, use_cftime=True, decode_timedelta=False)

    min_lon = float(area[0])
    min_lat = float(area[1])
    max_lon = float(area[2])
    max_lat = float(area[3])

    ds_subset = ds.sel(
        time=slice(start_date, end_date),
        lon=slice(min_lon, max_lon),
        lat=slice(min_lat, max_lat),
    )

    max_cld = ds_subset[allowed_vars[variable]].max(skipna=True)
    min_cld = ds_subset[allowed_vars[variable]].min(skipna=True)

    data_inputs = (f"dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;"
                   f"variable={variable};timeDateRange={start_date}/{end_date};area={','.join(area)};"
                   f"output_type={output_type}")
    wps_ds = _common_wps_process_test(PROC_CLASS, data_inputs)

    if allowed_vars[variable] == "wet":
        expected_lons = [-79.75, -29.75, 20.25, 70.25, 120.25, 170.25]
    elif allowed_vars[variable] == "tmn":
        expected_lons = [20.25]
    else:
        raise Exception(f"No expected longitudes rule in test for variable: {variable}")

    assert (
        wps_ds[allowed_vars[variable]].lon.values.tolist() == expected_lons
    )

    assert max_cld == wps_ds[allowed_vars[variable]].max(skipna=True)
    assert min_cld == wps_ds[allowed_vars[variable]].min(skipna=True)
