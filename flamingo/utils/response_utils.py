from ..provenance import Provenance

import logging
LOGGER = logging.getLogger("PYWPS")


def populate_response(response, label, workdir, inputs, collection, ml4):
    response.outputs["output"].data = ml4.xml

    # Create a tidied copy of the inputs
    inputs_copy = {}

    for key, value in inputs.items():
        inputs_copy[key] = getattr(value, "value", value)

    # Collect provenance
    provenance = Provenance(workdir)
    provenance.start()
    urls = []

    LOGGER.warning("Adding file URLs (except for PNGs (due to provenance.write_png() problem.")
    for f in ml4.files:
        furls = f.urls
        furls_no_png = [furl for furl in furls if not furl.endswith(".png")]
        urls.extend(furls_no_png)

    provenance.add_operator(label, inputs_copy, collection, urls)
    response.outputs["prov"].file = provenance.write_json()
    LOGGER.warning("Commented out: response.outputs[\"prov_plot\"].file = provenance.write_png()")
#    response.outputs["prov_plot"].file = provenance.write_png()
