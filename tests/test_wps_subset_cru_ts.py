from pywps import Service
from pywps.tests import client_for, assert_response_success, assert_process_exception

from .common import get_output, PYWPS_CFG
from flamingo.processes.wps_subset_cru_ts import SubsetCRUTS

import numpy as np
import xarray as xr
import xml.etree.ElementTree as ET


def test_wps_subset_cru_ts(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=cru_ts.4.04;variable=wet;time=1951-01-01/2005-12-15;area=1,1,300,89"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=subset_cru_ts&datainputs={datainputs}"
    )
    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]

def test_wps_subset_cru_ts_check_nc_content(load_ceda_test_data):
    client = client_for(Service(processes=[SubsetCRUTS()], cfgfiles=[PYWPS_CFG]))
    datainputs = "dataset_version=cru_ts.4.04;variable=wet;time=1951-01-01/2005-12-15;area=1,1,300,89"
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier=subset_cru_ts&datainputs={datainputs}"
    )
    assert_response_success(resp)
    output_file = get_output(resp.xml)["output"][7:] #trim off 'file://'

    tree = ET.parse(output_file)
    root = tree.getroot()

    file_tag = root.find('{urn:ietf:params:xml:ns:metalink}file')
    nc_output_file = file_tag.find('{urn:ietf:params:xml:ns:metalink}metaurl').text[7:] #trim off 'file://'

    ds = xr.open_dataset(nc_output_file)

    data_vars = ds.data_vars
    expected_vars = set(['time', 'wet', 'lat', 'lon'])
    found_vars = set(ds.variables.keys())

    assert len(data_vars) == 1
    assert expected_vars == found_vars

    first_date = ds.variables['time'].values[0]
    last_date = ds.variables['time'].values[-1]

    assert first_date >= np.datetime64('1951-01-01')
    assert last_date <= np.datetime64('2005-12-15')

    min_lat = ds.variables['lat'].values[0]
    max_lat = ds.variables['lat'].values[-1]
    min_lon = ds.variables['lon'].values[0]
    max_lon = ds.variables['lon'].values[-1]

    assert min_lat >= 1
    assert max_lat <= 89
    assert min_lon >= 1
    assert max_lon <= 300
