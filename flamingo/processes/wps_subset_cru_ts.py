from daops.ops.subset import subset

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

from ..utils.input_utils import parse_wps_input
from ..utils.metalink_utils import build_metalink
from ..utils.response_utils import populate_response


DATASET_ALLOWED_VALUES = {
    "Climatic Research Unit (CRU) TS (time-series) datasets 4.04": "cru_ts.4.04"
}

VARIABLE_ALLOWED_VALUES = {
    "precipitation (mm/month)": "pre",
    "near-surface temperature (degrees Celsius)": "tmp",
    "near-surface temperature maximum (degrees Celsius)": "tmx",
    "potential evapotranspiration (mm/day)": "pet",
    "ground frost frequency (days)": "frs",
    "cloud cover (percentage)": "cld",
    "wet day frequency (days)": "wet",
    "diurnal temperature range (degrees Celsius)": "dtr",
    "vapour pressure (hPa)": "vap",
    "near-surface temperature minimum (degrees Celsius)": "tmn",
}


class SubsetCRUTS(Process):
    def __init__(self):
        inputs = [
            LiteralInput(
                "dataset_version",
                "Dataset Version",
                abstract="The variable to data over.",
                data_type="string",
                allowed_values=list(DATASET_ALLOWED_VALUES),
                min_occurs=1,
                max_occurs=1,
            ),
            LiteralInput(
                "variable",
                "Variable",
                abstract="The variable to subset over.",
                data_type="string",
                allowed_values=list(VARIABLE_ALLOWED_VALUES.keys()),
                min_occurs=1,
            ),
            LiteralInput(
                "timeDateRange",
                "Time Period",
                abstract="The time period to subset over.",
                data_type="string",
                default="1901-01-16/2019-12-16",
                min_occurs=0,
                max_occurs=1,
            ),
            BoundingBoxInput(
                "area",
                "Area",
                abstract="The area to subset over.",
                crss=["-180.0, -90.0, 180.0, 90.0,epsg:4326"],
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
                abstract="Metalink v4 document with references to NetCDF files.",
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
            abstract="Run subsetting on CRU Time Series data",
            keywords=[
                "subset",
                "climate",
                "research",
                "unit",
                "time",
                "series",
                "data",
            ],
            metadata=[
                Metadata("CEDA WPS UI", "https://ceda-wps-ui.ceda.ac.uk"),
                Metadata("CEDA WPS", "https://ceda-wps.ceda.ac.uk"),
                Metadata(
                    "Disclaimer", "https://help.ceda.ac.uk/article/4642-disclaimer"
                ),
            ],
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

        collection = f"{dataset_version}.{variable}"

        inputs = {
            "collection": collection,
            "time": parse_wps_input(request.inputs, "timeDateRange", default=None),
            "area": parse_wps_input(request.inputs, "area", default=None),
            #            "apply_fixes": False,
            "output_dir": self.workdir,
            "file_namer": "simple",
            "output_type": parse_wps_input(
                request.inputs, "output_type", must_exist=True
            ),
        }

        output_uris = subset(**inputs).file_uris

        ml4 = build_metalink(
            "subset-cru_ts-result",
            "Subsetting result as NetCDF files.",
            self.workdir,
            output_uris,
        )

        populate_response(response, "subset", self.workdir, inputs, collection, ml4)

        return response
