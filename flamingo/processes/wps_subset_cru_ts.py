from flamingo.processes._wps_subset_base import *
# import os

# from daops.ops.subset import subset
# from nappy.nc_interface.xarray_to_na import XarrayDatasetToNA

# from pywps import (
#     BoundingBoxInput,
#     LiteralInput,
#     Process,
#     FORMATS,
#     Format,
#     ComplexOutput,
# )
# from pywps.app.Common import Metadata
# from pywps.app.exceptions import ProcessError

# from flamingo.processes._wps_subset_base import _SubsetBase
# from flamingo.utils.input_utils import parse_wps_input
# from flamingo.utils.metalink_utils import build_metalink
# from flamingo.utils.response_utils import populate_response

# # Load content information about datasets and inputs
# from ceda_wps_assets.flamingo import get_dset_info


# DATASET_ALLOWED_VALUES = dset_info["input_datasets"]
# VARIABLE_ALLOWED_VALUES = dset_info["input_variables"]
# TIME_RANGE = dset_info["time_range"]
# BBOX = dset_info["bbox"]
# CATALOGUE_RECORDS = dset_info["catalogue_records"].items()

# PROCESS_METADATA = [
#     Metadata("CEDA WPS UI", "https://ceda-wps-ui.ceda.ac.uk"),
#     Metadata("CEDA WPS", "https://ceda-wps.ceda.ac.uk"),
#     Metadata("Disclaimer", "https://help.ceda.ac.uk/article/4642-disclaimer"),
# ] + [Metadata(name, url) for name, url in CATALOGUE_RECORDS]


class SubsetCRUTS(SubsetBase):

    IDENTIFIER = "SubsetCRUTimeSeries"
    TITLE = "Subset CRU Time Series"
    ABSTRACT = "Extract a subset from the CRU Time Series data"
    KEYWORDS = ["subset", "climate", "research", "unit", "time", "series", "data"]
    METALINK_ID = "subset-cru-ts-result"

    DSET_INFO = get_dset_info(IDENTIFIER)
    INPUTS_LIST = ["dataset_version", "variable", "timeDateRange", "area", "output_type"]

    PROCESS_METADATA = [
        Metadata("CEDA WPS UI", "https://ceda-wps-ui.ceda.ac.uk"),
        Metadata("CEDA WPS", "https://ceda-wps.ceda.ac.uk"),
        Metadata("Disclaimer", "https://help.ceda.ac.uk/article/4642-disclaimer"),
    ] + [Metadata(name, url) for name, url in DSET_INFO["catalogue_records"].items()]

    def _get_collection(self, inputs):
        dataset_version = self.DSET_INFO["input_datasets"][
            parse_wps_input(inputs, "dataset_version", must_exist=True)
        ]
        variable = self.DSET_INFO["input_variables"][
            parse_wps_input(inputs, "variable", must_exist=True)
        ]

        return f"{dataset_version}.{variable}"

