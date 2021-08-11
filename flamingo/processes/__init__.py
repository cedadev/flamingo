from .wps_subset_cru_ts import SubsetCRUTS
from .wps_subset_haduk_grid import SubsetHadUKGrid

processes = [
    SubsetCRUTS(),
    SubsetHadUKGrid(),
]
