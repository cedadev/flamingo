from daops.ops.subset import subset

from pywps import LiteralInput, Process, FORMATS, ComplexOutput
from pywps.app.Common import MetaData
from pywps.app.exceptions import ProcessError
from pywps.inout.outputs import MetaFile, MetaLink4

from ..utils.input_utils import parse_wps_input

class SubsetCRUTS(Process):
    def __intit__(self):
        inputs = [
            LiteralInput(
                "dataset_version",
                "Dataset Version",
                abstract="Example: cru_ts.4.04",
                data_type="string",
                min_occurs=1,
                max_occurs=1,
            ),
            LiteralInput(
                "variable",
                "Variable",
                abstract="Example: tmn",
                data_type="string",
                min_occurs=1,
            ),
            LiteralInput(
                "time",
                "Time Period",
                abstract="The time period to subset over separated by /"
                "Example: 1960-01-01/2000-12-30",
                data_type="string",
                min_occurs=0,
                max_occurs=1,
            ),
            LiteralInput(
                "area",
                "Area",
                abstract="The area to subset over as 4 comma separated values."
                "Example: 0.,49.,10.,65",
                data_type="string",
                min_occurs=0,
                max_occurs=1,
            ),
        ]

        outputs = [
            ComplexOutput(
                "output",
                "METALINK v4 output",
                abstract="Metalink v4 document with references to NetCDF files.",
                as_reference=True,
                supported_formats=[FORMATS.META4]
            )
        ]

        super(SubsetCRUTS, self).__init__(
            self._handler,
            identifier="subset_cru_ts",
            title="Subset CRU_TS",
            abstract="Run subsetting on cru ts data",
            version="1.0",
            inputs=inputs,
            outputs=outputs
        )


    def _handler(self, request, response):
        dataset_version = parse_wps_input(request.inputs, 'dataset_version', must_exist=True)
        variable = parse_wps_input(request.inputs, 'variable', must_exist=True)

        collection = f'{dataset_version}.{variable}'

        inputs = {
            "collection": collection,
            "output_dir": self.workdir,
            "time": parse_wps_input(request.inputs, 'time', default=None),
            "area": parse_wps_input(request.inputs, 'area', default=None)
        }

        output_uris = subset(**inputs).file_uris

        ml4 = build_metalink("subset-result", "Subsetting result as NetCDF files.",
                             self.workdir, output_uris,
                             as_urls=False)

        populate_response(response, 'subset', self.workdir, inputs, collection, ml4)
        return response
