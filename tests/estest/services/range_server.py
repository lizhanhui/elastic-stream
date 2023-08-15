import os
import signal
from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until
class RangeServer(Service):
    ROOT = "/home/ducker/range-server"
    def __init__(self, context, num_nodes, pd):
        self.pd = pd
        super(RangeServer, self).__init__(context, num_nodes)

    def restart_cluster(self):
        for node in self.nodes:
            self.restart_node(node)

    def restart_node(self, node):
        """Restart the given node."""
        self.stop_node(node)
        self.start_node(node)

    def start_node(self, node):
        idx = self.idx(node)
        node.account.ssh("mkdir -p %s" % RangeServer.ROOT)
        cmd = self.start_cmd(node)
        node.account.ssh(cmd)
        wait_until(lambda: self.listening(node), timeout_sec=30, err_msg="RangeServer node failed to start")

    def listening(self, node):
        try:
            port = 10911
            cmd = "nc -z %s %s" % (node.account.hostname, port)
            node.account.ssh_output(cmd, allow_fail=False)
            self.logger.debug("RangeServer started accepting connections at: '%s:%s')", node.account.hostname, port)
            return True
        except (RemoteCommandError, ValueError) as e:
            return False

    def start_cmd(self, node):
        cmd = "cd " + RangeServer.ROOT + ";"
        cmd += "export ES_STORE_PATH=" + RangeServer.ROOT + "/store;"
        cmd += "export ES_CONFIG=" + RangeServer.ROOT + "/range-server.yaml;"
        cmd += "export ES_ADDR=0.0.0.0:10911;"
        cmd += "export ES_ADVERTISE_ADDR=" + node.account.hostname + ":10911;"
        cmd += "export ES_PD=" + self.pd.get_hostname() + ":12378;"
        cmd += "range-server start &>> range-server.log &"
        return cmd

    def pids(self, node):
        try:
            cmd = "ps -a | grep range-server | awk '{print $1}'"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []


    def alive(self, node):
        return len(self.pids(node)) > 0

    def kill_node(self, node):
        idx = self.idx(node)
        self.logger.info("Killing %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        self.signal_node(node, signal.SIGKILL)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for RangeServer to be killed.")

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        self.signal_node(node)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for RangeServer to stop.")

    def signal_node(self, node, sig=signal.SIGTERM):
        pids = self.pids(node)
        for pid in pids:
            node.account.signal(pid, sig)


    def clean_node(self, node):
        self.stop_node(node)
        node.account.ssh("sudo rm -rf -- %s" % RangeServer.ROOT, allow_fail=False)
