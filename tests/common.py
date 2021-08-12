import os
from pathlib import Path

from pywps import get_ElementMakerForVersion
from pywps.app.basic import get_xpath_ns
from pywps.tests import WpsClient, WpsTestResponse

from pywps import Service
from pywps.tests import client_for, assert_response_success, assert_process_exception

from jinja2 import Template
import tempfile
import xml.etree.ElementTree as ET

import xarray as xr


TESTS_HOME = os.path.abspath(os.path.dirname(__file__))
PYWPS_CFG = os.path.join(TESTS_HOME, "pywps.cfg")
ROOCS_CFG = os.path.join(tempfile.gettempdir(), "roocs.ini")

VERSION = "1.0.0"
WPS, OWS = get_ElementMakerForVersion(VERSION)
xpath_ns = get_xpath_ns(VERSION)

MINI_CEDA_CACHE_DIR = Path.home() / ".mini-ceda-archive"
MINI_CEDA_CACHE_BRANCH = "main"


def write_roocs_cfg():
    cfg_templ = """[project:cru_ts]
base_dir = {{ ceda_base_dir }}/archive/badc/cru/data/cru_ts
file_name_template = {__derive__var_id}_{frequency}_{__derive__time_range}.{__derive__extension}
fixed_path_modifiers =
    variable:cld dtr frs pet pre tmn tmp tmx vap wet
fixed_path_mappings =
    cru_ts.4.04.{variable}:cru_ts_4.04/data/{variable}/*.nc
    cru_ts.4.05.{variable}:cru_ts_4.05/data/{variable}/cru_ts4.05.1901.2*.{variable}.dat.nc.gz
attr_defaults =
    frequency:mon
facet_rule = project version_major version_minor variable

[project:haduk_grid]
base_dir = {{ ceda_base_dir }}/archive/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid
file_name_template = {__derive__var_id}_hadukgrid_uk_{spatial_average}_{frequency}_{__derive__time_range}.{__derive__extension}
facet_rule = project version_major version_minor version_patch version_extra spatial_average frequency variable version
fixed_path_modifiers = 
    variable:groundfrost pv rainfall sfcWind snowLying sun tas tasmin
    frequency:mon
fixed_path_mappings =
    haduk_grid.v1.0.3.0.1km.{frequency}.{variable}.v20210712:v1.0.3.0/1km/{variable}/{frequency}/v20210712/*.nc
    haduk_grid.v1.0.2.1.1km.{frequency}.{variable}.v20200731:v1.0.2.1/1km/{variable}/{frequency}/v20200731/*.nc
    """

    cfg = Template(cfg_templ).render(ceda_base_dir=(MINI_CEDA_CACHE_DIR / MINI_CEDA_CACHE_BRANCH))

    with open(ROOCS_CFG, "w") as fp:
        fp.write(cfg)

    # point to roocs cfg in environment
    os.environ["ROOCS_CONFIG"] = ROOCS_CFG


def resource_file(filepath):
    return os.path.join(TESTS_HOME, "testdata", filepath)


def get_output(doc):
    """Copied from pywps/tests/test_execute.py.
    TODO: make this helper method public in pywps."""
    output = {}
    for output_el in xpath_ns(
        doc, "/wps:ExecuteResponse" "/wps:ProcessOutputs/wps:Output"
    ):
        [identifier_el] = xpath_ns(output_el, "./ows:Identifier")

        lit_el = xpath_ns(output_el, "./wps:Data/wps:LiteralData")
        if lit_el != []:
            output[identifier_el.text] = lit_el[0].text

        ref_el = xpath_ns(output_el, "./wps:Reference")
        if ref_el != []:
            output[identifier_el.text] = ref_el[0].attrib["href"]

        data_el = xpath_ns(output_el, "./wps:Data/wps:ComplexData")
        if data_el != []:
            output[identifier_el.text] = data_el[0].text

    return output


def _common_wps_process_test(proc_class, data_inputs):
    """
    Calls the Test Client with the process and datainputs.
    Tests a valid file is created and it can be loaded.
    Returns the data object that has been loaded, either:
      xarray.Dataset or lines of a text file
    """
    client = client_for(Service(processes=[proc_class()], cfgfiles=[PYWPS_CFG]))
    resp = client.get(
        f"?service=WPS&request=Execute&version=1.0.0&identifier={proc_class.IDENTIFIER}&datainputs={data_inputs}"
    )

    assert_response_success(resp)
    assert "meta4" in get_output(resp.xml)["output"]
    output_file = get_output(resp.xml)["output"][7:]  # trim off 'file://'

    tree = ET.parse(output_file)
    root = tree.getroot()

    file_tag = root.find("{urn:ietf:params:xml:ns:metalink}file")
    filepath = file_tag.find("{urn:ietf:params:xml:ns:metalink}metaurl").text[7:]

    if "output_type=csv" in data_inputs.lower():
        # Read in as CSV using Nappy
        content = [line.strip() for line in open(filepath).readlines()]
        assert isinstance(content, list)
        assert len(content) > 10
    else:
        content = xr.open_dataset(filepath, use_cftime=True, decode_timedelta=False)
        assert isinstance(content, xr.Dataset)

    return content
