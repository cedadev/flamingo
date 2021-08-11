from pywps import Service
from pywps.tests import WpsClient, WpsTestResponse

from flamingo.processes import processes


class WpsTestClient(WpsClient):
    def get(self, *args, **kwargs):
        query = "?"
        for key, value in kwargs.items():
            query += "{0}={1}&".format(key, value)
        return super(WpsTestClient, self).get(query)


def local_client_for(service):
    return WpsTestClient(service, WpsTestResponse)



def test_wps_caps():
    client = local_client_for(Service(processes=processes))
    resp = client.get(service="wps", request="getcapabilities", version="1.0.0")
    names = resp.xpath_text(
        "/wps:Capabilities" "/wps:ProcessOfferings" "/wps:Process" "/ows:Identifier"
    )
    assert sorted(names.split()) == [
        "SubsetCRUTimeSeries",
        "SubsetHadUKGrid"
    ]
