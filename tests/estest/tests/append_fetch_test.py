from ducktape.tests.test import Test
from estest.services.pd import PD
from ducktape.mark.resource import cluster
from ducktape.mark import matrix
from estest.services.range_server import RangeServer
from estest.services.verifiable_producer import VerifiableProducer
from estest.services.verifiable_consumer import VerifiableConsumer
class AppendFetchTest(Test):
    def __init__(self, test_context):
        super(AppendFetchTest, self).__init__(test_context=test_context)
    # @cluster(num_nodes=4)
    @matrix(rs_count=[1, 3], count=[100, 1000])
    def test_append_fetch(self, rs_count, count):
        pd = PD(self.test_context, num_nodes=rs_count)
        pd.start()
        rs = RangeServer(self.test_context, num_nodes=rs_count, pd=pd)
        rs.start()
        producer = VerifiableProducer(self.test_context, num_nodes=1, pd=pd, start_seq=0, count=count, stream_id=-1, replica=rs_count)
        producer.start()
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, pd=pd, start_seq=0, count=count, stream_id=0)
        consumer.start()

        pd.clean()
        rs.clean()
