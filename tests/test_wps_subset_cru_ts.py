from pywps import Service
from pywps.tests import client_for, assert_response_success, assert_process_exception

from tests.common import get_output, PYWPS_CFG, MINI_CEDA_CACHE_DIR, MINI_CEDA_CACHE_BRANCH

from flamingo.processes.wps_subset_cru_ts import SubsetCRUTS, VARIABLE_ALLOWED_VALUES

import numpy as np
import pytest
import xarray as xr
import xml.etree.ElementTree as ET

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


def test_wps_subset_cru_ts(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=Climatic Research Unit (CRU) TS (time-series) datasets 4.04;variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;area=1,1,300,89;output_type=netcdf"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]


def test_wps_subset_cru_ts_csv(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=Climatic Research Unit (CRU) TS (time-series) datasets 4.04;variable=near-surface temperature minimum (degrees Celsius);timeDateRange=1951-01-01/2005-12-15;area=1,1,300,89;output_type=csv"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]


def test_wps_subset_cru_ts_csv(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=Climatic Research Unit (CRU) TS (time-series) datasets 4.04;variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;area=1,1,300,89;output_type=csv"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]


@pytest.mark.parametrize("variable,start_date,end_date,area,output_type", TEST_SETS)
def test_wps_subset_cru_ts_check_nc_content(
    load_ceda_test_data, variable, start_date, end_date, area, output_type
):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = f"dataset_version=Climatic Research Unit (CRU) TS (time-series) datasets 4.04;variable={variable};timeDateRange={start_date}/{end_date};area={','.join(area)};output_type={output_type}"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    output_file = get_output(resp.xml)["output"][7:]  # trim off 'file://'

    tree = ET.parse(output_file)
    root = tree.getroot()

    file_tag = root.find("{urn:ietf:params:xml:ns:metalink}file")
    nc_output_file = file_tag.find("{urn:ietf:params:xml:ns:metalink}metaurl").text[
        7:
    ]  # trim off 'file://'

    ds = xr.open_dataset(nc_output_file)

    data_vars = ds.data_vars
    expected_vars = set(["time", VARIABLE_ALLOWED_VALUES[variable], "lat", "lon"])
    found_vars = set(ds.variables.keys())

    assert len(data_vars) == 1
    assert expected_vars == found_vars

    first_date = ds.variables["time"].values[0]
    last_date = ds.variables["time"].values[-1]

    assert first_date >= np.datetime64(start_date)
    assert last_date <= np.datetime64(end_date)

    min_lon = ds.variables["lon"].values[0]
    min_lat = ds.variables["lat"].values[0]
    max_lon = ds.variables["lon"].values[-1]
    max_lat = ds.variables["lat"].values[-1]

    assert min_lon >= float(area[0])
    assert min_lat >= float(area[1])
    assert max_lon <= float(area[2])
    assert max_lat <= float(area[3])


@pytest.mark.parametrize("variable,start_date,end_date,area,output_type", TEST_SETS)
def test_wps_subset_cru_ts_check_min_max(
    load_ceda_test_data, variable, start_date, end_date, area, output_type
):
    data_path = f"{MINI_CEDA_CACHE_DIR}/{MINI_CEDA_CACHE_BRANCH}/archive/badc/cru/data/cru_ts/cru_ts_4.04/data/{VARIABLE_ALLOWED_VALUES[variable]}/*.nc"
    ds = xr.open_mfdataset(data_path)

    min_lon = float(area[0])
    min_lat = float(area[1])
    max_lon = float(area[2])
    max_lat = float(area[3])

    ds_subset = ds.sel(
        time=slice(start_date, end_date),
        lon=slice(min_lon, max_lon),
        lat=slice(min_lat, max_lat),
    )

    max_cld = ds_subset[VARIABLE_ALLOWED_VALUES[variable]].max(skipna=True)
    min_cld = ds_subset[VARIABLE_ALLOWED_VALUES[variable]].min(skipna=True)

    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = f"dataset_version=Climatic Research Unit (CRU) TS (time-series) datasets 4.04;variable={variable};timeDateRange={start_date}/{end_date};area={','.join(area)};output_type={output_type}"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    output_file = get_output(resp.xml)["output"][7:]  # trim off 'file://'

    tree = ET.parse(output_file)
    root = tree.getroot()

    file_tag = root.find("{urn:ietf:params:xml:ns:metalink}file")
    nc_output_file = file_tag.find("{urn:ietf:params:xml:ns:metalink}metaurl").text[
        7:
    ]  # trim off 'file://'

    wps_ds = xr.open_dataset(nc_output_file)

    if VARIABLE_ALLOWED_VALUES[variable] == "wet":
        expected_lons = [-79.75, -29.75, 20.25, 70.25, 120.25, 170.25]
    elif VARIABLE_ALLOWED_VALUES[variable] == "tmn":
        expected_lons = [20.25]
    else:
        raise Exception(f"No expected longitudes rule in test for variable: {variable}")

    assert (
        wps_ds[VARIABLE_ALLOWED_VALUES[variable]].lon.values.tolist() == expected_lons
    )

    assert max_cld == wps_ds[VARIABLE_ALLOWED_VALUES[variable]].max(skipna=True)
    assert min_cld == wps_ds[VARIABLE_ALLOWED_VALUES[variable]].min(skipna=True)
