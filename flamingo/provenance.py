import os

from prov.model import ProvDocument
from prov.dot import prov_to_dot


class Provenance(object):
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.doc = None
        self.workflow = None

    def start(self, workflow=False):
        from daops import __version__ as daops_version
        from flamingo import __version__ as flamingo_version

        self.doc = ProvDocument()
        # Declaring namespaces for various prefixes
        self.doc.set_default_namespace(uri="http://purl.org/roocs/prov#")
        self.doc.add_namespace("prov", uri="http://www.w3.org/ns/prov#")
        self.doc.add_namespace(
            "provone", uri="http://purl.dataone.org/provone/2015/01/15/ontology#"
        )
        self.doc.add_namespace("dcterms", uri="http://purl.org/dc/terms/")
        # Define entities
        project_cds = self.doc.agent(
            ":copernicus_CDS",
            {
                "prov:type": "prov:Organization",
                "dcterms:title": "Copernicus Climate Data Store",
            },
        )
        self.sw_flamingo = self.doc.agent(
            ":flamingo",
            {
                "prov:type": "prov:SoftwareAgent",
                "dcterms:source": f"https://github.com/cedadev/flamingo/releases/tag/v{flamingo_version}",
            },
        )
        self.doc.wasAttributedTo(self.sw_flamingo, project_cds)
        self.sw_daops = self.doc.agent(
            ":daops",
            {
                "prov:type": "prov:SoftwareAgent",
                "dcterms:source": f"https://github.com/roocs/daops/releases/tag/v{daops_version}",
            },
        )
        # workflow
        if workflow is True:
            self.workflow = self.doc.entity(
                ":workflow", {"prov:type": "provone:Workflow"}
            )
            orchestrate = self.doc.activity(
                ":orchestrate",
                other_attributes={
                    "prov:startedAtTime": "2020-11-26T09:15:00",
                    "prov:endedAtTime": "2020-11-26T09:30:00",
                },
            )
            self.doc.wasAssociatedWith(
                orchestrate, agent=self.sw_flamingo, plan=self.workflow
            )

    def add_operator(self, operator, parameters, collection, output):
        op = self.doc.activity(
            f":{operator}",
            other_attributes={
                ":time": parameters.get("time"),
                ":apply_fixes": parameters.get("apply_fixes"),
            },
        )
        # input data
        ds_in = os.path.basename(collection[0])
        # ds_in_attrs = {
        #     'prov:type': 'provone:Data',
        #     'prov:value': f'{ds_in}',
        # }
        op_in = self.doc.entity(f":{ds_in}")
        # operator started by daops
        if self.workflow:
            self.doc.wasAssociatedWith(op, agent=self.sw_daops, plan=self.workflow)
        else:
            self.doc.start(op, starter=self.sw_daops, trigger=self.sw_flamingo)
        # Generated output file

        ds_out = os.path.basename(output[0])
        # ds_out_attrs = {
        #     'prov:type': 'provone:Data',
        #     'prov:value': f'{ds_out}',
        # }
        op_out = self.doc.entity(f":{ds_out}")
        self.doc.wasDerivedFrom(op_out, op_in, activity=op)

    def write_json(self):
        outfile = os.path.join(self.output_dir, "provenance.json")
        self.doc.serialize(outfile, format="json")
        return outfile

    def write_png(self):
        outfile = os.path.join(self.output_dir, "provenance.png")
        figure = prov_to_dot(self.doc)
        figure.write_png(outfile)
        return outfile
