from ducktape.tests.test import Test
from estest.services.pd import PD
from ducktape.mark.resource import cluster
from estest.services.metadata import Metadata
from ducktape.mark import matrix
from ducktape.utils.util import wait_until

from estest.services.range_server import RangeServer
class MetadataTest(Test):
    def __init__(self, test_context):
        super(MetadataTest, self).__init__(test_context=test_context)
    # @cluster(num_nodes=4)
    @matrix(pd_count=[3], count=[100, 1000])
    def test_metadata(self, pd_count, count):
        pd = PD(self.test_context, num_nodes=pd_count)
        pd.start()
        rs = RangeServer(self.test_context, num_nodes=pd_count, pd=pd)
        rs.start()
        metadata = Metadata(self.test_context, num_nodes=1, pd=pd, count=count)
        metadata.start()

        pd.clean()
        rs.clean()
