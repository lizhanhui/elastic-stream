from ducktape.tests.test import Test
from estest.services.pd import PD
from ducktape.mark.resource import cluster
from ducktape.mark import matrix

from estest.services.range_server import RangeServer
from estest.services.verifiable_producer import VerifiableProducer
from estest.services.verifiable_consumer import VerifiableConsumer
from estest.services.trim_and_delete_test import TrimAndDelete
class TrimAndDeleteTest(Test):
    def __init__(self, test_context):
        super(TrimAndDeleteTest, self).__init__(test_context=test_context)
    @matrix(rs_count=[1, 3], count=[1024])
    def test_trim_and_delete(self, rs_count, count):
        pd = PD(self.test_context, num_nodes=rs_count)
        pd.start()
        rs = RangeServer(self.test_context, num_nodes=rs_count, pd=pd)
        rs.start()
        trim_and_delete = TrimAndDelete(self.test_context, num_nodes=1, pd=pd, replica=rs_count, count=count)
        trim_and_delete.start()

        pd.clean()
        rs.clean()
