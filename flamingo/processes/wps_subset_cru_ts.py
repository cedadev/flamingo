import os

from daops.ops.subset import subset
from nappy.nc_interface.xarray_to_na import XarrayDatasetToNA

from pywps import (
    BoundingBoxInput,
    LiteralInput,
    Process,
    FORMATS,
    Format,
    ComplexOutput,
)
from pywps.app.Common import Metadata
from pywps.app.exceptions import ProcessError

from flamingo.utils.input_utils import parse_wps_input
from flamingo.utils.metalink_utils import build_metalink
from flamingo.utils.response_utils import populate_response

# Load content information about datasets and inputs
from ceda_wps_assets.flamingo import get_dset_info

dset_info = get_dset_info(__file__)

DATASET_ALLOWED_VALUES = dset_info["input_datasets"]
VARIABLE_ALLOWED_VALUES = dset_info["input_variables"]
TIME_RANGE = dset_info["time_range"]
BBOX = dset_info["bbox"]
CATALOGUE_RECORDS = dset_info["catalogue_records"].items()

PROCESS_METADATA = [
    Metadata("CEDA WPS UI", "https://ceda-wps-ui.ceda.ac.uk"),
    Metadata("CEDA WPS", "https://ceda-wps.ceda.ac.uk"),
    Metadata("Disclaimer", "https://help.ceda.ac.uk/article/4642-disclaimer"),
] + [Metadata(name, url) for name, url in CATALOGUE_RECORDS]


class SubsetCRUTS(Process):
    def __init__(self):
        inputs = [
            LiteralInput(
                "dataset_version",
                "Dataset Version",
                abstract="The dataset to subset",
                data_type="string",
                allowed_values=list(DATASET_ALLOWED_VALUES.keys()),
                min_occurs=1,
                max_occurs=1,
            ),
            LiteralInput(
                "variable",
                "Variable",
                abstract="The variable to subset",
                data_type="string",
                allowed_values=list(VARIABLE_ALLOWED_VALUES.keys()),
                min_occurs=1,
            ),
            LiteralInput(
                "timeDateRange",
                "Time Period",
                abstract="The time period to subset over.",
                data_type="string",
                default=TIME_RANGE,
                min_occurs=0,
                max_occurs=1,
            ),
            BoundingBoxInput(
                "area",
                "Area",
                abstract="The area to subset over.",
                crss=[BBOX],
                min_occurs=0,
                max_occurs=1,
            ),
            LiteralInput(
                "output_type",
                "Output Format",
                abstract="The file format required for you output data.",
                data_type="string",
                allowed_values=["netcdf", "csv"],
                min_occurs=1,
                max_occurs=1,
            ),
        ]

        outputs = [
            ComplexOutput(
                "output",
                "METALINK v4 output",
                abstract="Metalink v4 document with references to output files.",
                as_reference=True,
                supported_formats=[FORMATS.META4],
            ),
            ComplexOutput(
                "prov",
                "Provenance",
                abstract="Provenance document using W3C standard.",
                as_reference=True,
                supported_formats=[FORMATS.JSON],
            ),
            ComplexOutput(
                "prov_plot",
                "Provenance Diagram",
                abstract="Provenance document as diagram.",
                as_reference=True,
                supported_formats=[
                    Format("image/png", extension=".png", encoding="base64")
                ],
            ),
        ]

        super(SubsetCRUTS, self).__init__(
            
            self._handler,
            identifier="SubsetCRUTimeSeries",
            title="Subset CRU Time Series",
            abstract="Extract a subset from the CRU Time Series data",
            keywords=[
                "subset",
                "climate",
                "research",
                "unit",
                "time",
                "series",
                "data",
            ],
            metadata=PROCESS_METADATA,
            version="1.0.0",
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True,
        )

    def _handler(self, request, response):
        dataset_version = DATASET_ALLOWED_VALUES[
            parse_wps_input(request.inputs, "dataset_version", must_exist=True)
        ]
        variable = VARIABLE_ALLOWED_VALUES[
            parse_wps_input(request.inputs, "variable", must_exist=True)
        ]

        output_format = parse_wps_input(request.inputs, "output_type", must_exist=True)

        output_type = output_format
        if output_format == "csv":
            output_type = "xarray" 

        collection = f"{dataset_version}.{variable}"

        inputs = {
            "collection": collection,
            "time": parse_wps_input(request.inputs, "timeDateRange", default=None),
            "area": parse_wps_input(request.inputs, "area", default=None),
            "output_dir": self.workdir,
            "file_namer": "simple",
            "output_type": output_type
        }

        results = subset(**inputs)

        if output_type == "netcdf":
            output_uris = results.file_uris
        else:
            # If CSV, then write them out using Nappy
            output_uris = []
            i = 1

            for result_list in results._results.values():
              
                for ds in result_list:
                    xr_to_na = XarrayDatasetToNA(ds)
                    xr_to_na.convert()

                    output_file = os.path.join(self.workdir, f"output_{i:02d}.csv")
                    xr_to_na.writeNAFiles(output_file, delimiter=",", float_format="%g")
                    output_uris.extend(xr_to_na.output_files_written)

                    i += 1
                
        ml4 = build_metalink(
            "subset-cru_ts-result",
            "Subsetting result into output file(s).",
            self.workdir,
            output_uris,
            output_format 
        )

        populate_response(response, "subset", self.workdir, inputs, collection, ml4)

        return response
