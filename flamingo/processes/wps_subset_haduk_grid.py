from flamingo.processes._wps_subset_base import *


class SubsetHadUKGrid(SubsetBase):

    IDENTIFIER = "SubsetHadUKGrid"
    TITLE = "Subset HadUK-Grid"
    ABSTRACT = "Extract a subset from the HadUK-Grid data"
    KEYWORDS = ["subset", "climate", "observations", "HadUK", "grid", "gridded", "data"]
    METALINK_ID = "subset-haduk-grid-result"

    DSET_INFO = get_dset_info(IDENTIFIER)
    INPUTS_LIST = ["dataset_version", "variable", "frequency", "spatial_average", 
                   "timeDateRange", "area", "output_type"]

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

        id_parts = dataset_version.split(".")
        return f"{'.'.join(id_parts[:-1])}.{variable}.{id_parts[-1]}"