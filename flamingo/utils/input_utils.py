import os
from pywps.app.exceptions import ProcessError

from clisops.parameter._utils import interval

def parse_wps_input(inputs, key, as_interval=False, as_sequence=False, 
                    must_exist=False, default=None):

    if not inputs.get(key):
        if must_exist:
            raise ProcessError(f'Required input "{key}" must be provided.')
        else:
            return default
    else:
        value = inputs[key]

    # Special issue for ranges
    if as_interval:
        value = interval(value[0].data)
    elif as_sequence:
        value = [dset.data for dset in value]
    else:
        value = value[0].data

    return value

def clean_inputs(inputs):
    "Remove common arguments not required in processing calls."
    to_remove = ('pre_checked', 'original_files')

    for key in to_remove:
        if key in inputs:
            del inputs[key]


def resolve_collection_if_files(coll):
    # If multiple inputs are files with a common directory name, then
    # return that as a single output

    if len(coll) > 1:
        # Interpret as a sequence of files
        first_dir = os.path.dirname(coll[0])

        # If all are valid file paths and they are all in one directory then return it
        if all([os.path.isfile(item) for item in coll]):
            if os.path.dirname(os.path.commonprefix(coll)) == first_dir:
                return first_dir

    return coll[0]
