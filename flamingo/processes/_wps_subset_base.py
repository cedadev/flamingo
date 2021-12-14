import os

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

from flamingo.utils.input_utils import parse_wps_input
from flamingo.utils.metalink_utils import build_metalink
from flamingo.utils.output_utils import write_to_csvs
from flamingo.utils.response_utils import populate_response

# Load content information about datasets and inputs
from ceda_wps_assets.flamingo import get_dset_info


class SubsetInputFactory:
    """
    Factory class for returning a list of WPS Input objects.
    These are wrapped in methods so that they only get evaluated
    if they are required. This avoids error checking for inputs
    that are not used by some sub-classes.
    """

    def __init__(self, dset_info):
        self.DSET_INFO = dset_info

    def get_inputs(self, input_keys):
        inputs = [self._lookup(key) for key in input_keys]
        return inputs

    def _lookup(self, key):
        method = getattr(self, f"_get_input_{key}")
        return method()

    def _get_input_dataset_version(self):
        return LiteralInput(
            "dataset_version",
            "Dataset Version",
            abstract="The dataset to subset",
            data_type="string",
            allowed_values=list(self.DSET_INFO["input_datasets"].keys()),
            min_occurs=1,
            max_occurs=1,
        )

    def _get_input_variable(self):
        return LiteralInput(
            "variable",
            "Variable",
            abstract="The variable to subset",
            data_type="string",
            allowed_values=list(self.DSET_INFO["input_variables"].keys()),
            min_occurs=1,
        )

    def _get_input_frequency(self):
        return LiteralInput(
            "frequency",
            "Frequency",
            abstract="The temporal frequency to subset",
            data_type="string",
            allowed_values=list(self.DSET_INFO["input_frequencies"].keys()),
            min_occurs=1,
        )

    def _get_input_spatial_average(self):
        return LiteralInput(
            "spatial_average",
            "Spatial Average",
            abstract="The spatial average (resolution) to subset",
            data_type="string",
            allowed_values=list(self.DSET_INFO["input_spatial_averages"].keys()),
            min_occurs=1,
        )

    def _get_input_timeDateRange(self):
        return LiteralInput(
            "timeDateRange",
            "Time Period",
            abstract="The time period to subset over.",
            data_type="string",
            default=self.DSET_INFO["time_range"],
            min_occurs=0,
            max_occurs=1,
        )

    def _get_input_area(self):
        return BoundingBoxInput(
            "area",
            "Area",
            abstract="The area to subset over.",
            crss=[self.DSET_INFO["bbox"]],
            min_occurs=0,
            max_occurs=1,
        )

    def _get_input_output_type(self):
        return LiteralInput(
            "output_type",
            "Output Format",
            abstract="The file format required for you output data.",
            data_type="string",
            allowed_values=["netcdf", "csv"],
            min_occurs=1,
            max_occurs=1,
        )
    

class SubsetBase(Process):

    # REQUIRED class properties:
    #  IDENTIFIER = str
    #  TITLE = str
    #  ABSTRACT = str
    #  KEYWORDS = [list]
    #  METALINK_ID = str
    #  DSET_INFO = str
    #  INPUTS_LIST = [list]
    #  PROCESS_METADATA = [list]

    def __init__(self):

        inputs = self._define_inputs()
        outputs = self._define_outputs()

        super(SubsetBase, self).__init__(
            self._handler,
            identifier=self.IDENTIFIER,
            title=self.TITLE,
            abstract=self.ABSTRACT,
            keywords=self.KEYWORDS,
            metadata=self.PROCESS_METADATA,
            version="1.0.0",
            inputs=inputs,
            outputs=outputs,
            store_supported=True,
            status_supported=True,
        )

    def _define_inputs(self):
        inputs = SubsetInputFactory(self.DSET_INFO).get_inputs(self.INPUTS_LIST)
        return inputs

    def _define_outputs(self):
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

        return outputs

    def _get_collection(self, inputs):
        raise NotImplementedError()

    def _handler(self, request, response):

        output_format = parse_wps_input(request.inputs, "output_type", must_exist=True)

        output_type = output_format
        if output_format == "csv":
            output_type = "xarray" 

        collection = self._get_collection(request.inputs)

        inputs = {
            "collection": collection,
            "time": parse_wps_input(request.inputs, "timeDateRange", as_interval=True, 
                                    default=None),
            "area": parse_wps_input(request.inputs, "area", default=None),
            "output_dir": self.workdir,
            "file_namer": "simple",
            "output_type": output_type,
            "apply_fixes": False
        }

        try:
            results = subset(**inputs)
        except Exception as exc:
            raise ProcessError(f"An error was reported with this job as follows: {str(exc)}")

        if output_type == "netcdf":
            output_uris = results.file_uris
        else:
            try:
                # Output type must be: "csv"
                output_uris = write_to_csvs(results, self.workdir)
            except Exception as exc:
                raise ProcessError(f"An error occurred when converting to CSV: {str(exc)}")
                
        ml4 = build_metalink(
            self.METALINK_ID,
            "Subsetting result into output file(s).",
            self.workdir,
            output_uris,
            output_format 
        )

        populate_response(response, "subset", self.workdir, inputs, collection, ml4)

        return response
