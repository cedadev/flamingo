from pywps import Service
from pywps.tests import client_for, assert_response_success, assert_process_exception

from tests.common import get_output, PYWPS_CFG, MINI_CEDA_CACHE_DIR, MINI_CEDA_CACHE_BRANCH

from flamingo.processes.wps_subset_haduk_grid import SubsetHadUKGrid, VARIABLE_ALLOWED_VALUES

import numpy as np
import pytest
import xarray as xr
import xml.etree.ElementTree as ET


PROC_CLASS = SubsetHadUKGrid


def test_wps_subset_haduk_grid_1km_v1_0_3_0(load_ceda_test_data):
    client = client_for(Service(processes=[PROC_CLASS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.05;variable=wet day frequency (days);timeDateRange=2001-01-01/2020-12-30;area=1,1,300,89;output_type=netcdf"

    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )

    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]

    output_file = get_output(resp.xml)["output"][7:]  # trim off 'file://'

    tree = ET.parse(output_file)
    root = tree.getroot()

    file_tag = root.find("{urn:ietf:params:xml:ns:metalink}file")
    nc_file = file_tag.find("{urn:ietf:params:xml:ns:metalink}metaurl").text[7:]

    ds = xr.open_dataset(nc_file, use_cftime=True, decode_timedelta=False)
    assert np.isclose(float(ds.wet.max()), 23.48, atol=.005)



def test_wps_subset_haduk_grid_1km_v1_0_2_1(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=Climatic Research Unit (CRU) TS (time-series) dataset 4.04;variable=wet day frequency (days);timeDateRange=1951-01-01/2005-12-15;area=1,1,300,89;output_type=netcdf"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=SubsetCRUTimeSeries&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]

