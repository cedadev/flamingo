from ..provenance import Provenance


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

    for f in ml4.files:
        urls.extend(f.urls)

    provenance.add_operator(label, inputs_copy, collection, urls)
    response.outputs["prov"].file = provenance.write_json()
    response.outputs["prov_plot"].file = provenance.write_png()
